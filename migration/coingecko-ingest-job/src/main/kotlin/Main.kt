package com.cryptotracker.coingecko

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FormatOptions
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardSQLTypeName
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.WriteChannelConfiguration
import com.google.gson.JsonParser
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.Locale
import java.util.UUID
import kotlin.math.min
import kotlin.math.pow

private const val COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
private const val STAGING_TABLE_PREFIX = "_market_cap_history_staging"
private const val CHUNK_DAYS = 90L
private const val DEFAULT_TOTAL_DAYS = 365L
private const val DEFAULT_REQUESTS_PER_MINUTE = 40
private const val DEFAULT_MAX_CONCURRENT_COINS = 10

private val httpClient: HttpClient = HttpClient.newBuilder().build()

data class Window(val start: LocalDate, val end: LocalDate)

data class Row(
    val coinId: String,
    val timestampEpochSeconds: Double,
    val priceUsd: Double?,
    val marketCapUsd: Double?,
    val totalVolumeUsd: Double?,
)

class RateLimiter(requestsPerMinute: Int) {
    private val mutex = Mutex()
    private val minIntervalNanos = (60_000_000_000L / requestsPerMinute)
    private var lastRequestAtNanos = 0L

    suspend fun beforeRequest() {
        mutex.withLock {
            val now = System.nanoTime()
            val elapsed = now - lastRequestAtNanos
            if (elapsed < minIntervalNanos) {
                delay((minIntervalNanos - elapsed) / 1_000_000)
            }
            lastRequestAtNanos = System.nanoTime()
        }
    }
}

fun buildWindows(totalDays: Long, chunkDays: Long, endDate: LocalDate): List<Window> {
    val windows = mutableListOf<Window>()
    var cursorEnd = endDate
    var remaining = totalDays
    while (remaining > 0) {
        val span = min(chunkDays, remaining)
        val cursorStart = cursorEnd.minusDays(span)
        windows.add(Window(cursorStart, cursorEnd))
        cursorEnd = cursorStart
        remaining -= span
    }
    return windows
}

fun fmtNumeric(value: Double?): String? {
    if (value == null) return null
    return String.format(Locale.ROOT, "%.9f", value)
}

suspend fun fetchMarketChartRange(
    coinId: String,
    fromTs: Long,
    toTs: Long,
    apiKey: String?,
    limiter: RateLimiter,
): String {
    val url = "$COINGECKO_BASE_URL/coins/$coinId/market_chart/range?vs_currency=usd&from=$fromTs&to=$toTs"
    val maxAttempts = 6
    for (attempt in 1..maxAttempts) {
        limiter.beforeRequest()
        val requestBuilder = HttpRequest.newBuilder().uri(URI.create(url)).GET().timeout(java.time.Duration.ofSeconds(30))
        if (apiKey != null) requestBuilder.header("x-cg-demo-api-key", apiKey)
        val response = withContext(Dispatchers.IO) {
            httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString())
        }
        if (response.statusCode() == 429 || response.statusCode() >= 500) {
            val wait = min(2.0.pow(attempt).toLong(), 60L)
            println("    $coinId: got HTTP ${response.statusCode()}, backing off ${wait}s (attempt $attempt/$maxAttempts)")
            delay(wait * 1000)
            continue
        }
        if (response.statusCode() !in 200..299) {
            throw RuntimeException("$coinId: HTTP ${response.statusCode()}: ${response.body().take(300)}")
        }
        return response.body()
    }
    throw RuntimeException("$coinId: exhausted $maxAttempts attempts")
}

fun responseToRows(coinId: String, body: String): List<Row> {
    val json = JsonParser.parseString(body).asJsonObject
    fun seriesMap(key: String): LinkedHashMap<Long, Double> {
        val map = LinkedHashMap<Long, Double>()
        val arr = json.getAsJsonArray(key) ?: return map
        for (pair in arr) {
            val p = pair.asJsonArray
            map[p[0].asLong] = p[1].asDouble
        }
        return map
    }
    val prices = seriesMap("prices")
    val marketCaps = seriesMap("market_caps")
    val volumes = seriesMap("total_volumes")
    return prices.keys.map { tsMs ->
        Row(
            coinId = coinId,
            timestampEpochSeconds = tsMs / 1000.0,
            priceUsd = prices[tsMs],
            marketCapUsd = marketCaps[tsMs],
            totalVolumeUsd = volumes[tsMs],
        )
    }
}

fun marketCapSchema(): Schema = Schema.of(
    Field.newBuilder("coin_id", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
    Field.newBuilder("timestamp", StandardSQLTypeName.TIMESTAMP).setMode(Field.Mode.REQUIRED).build(),
    Field.newBuilder("price_usd", StandardSQLTypeName.NUMERIC).build(),
    Field.newBuilder("market_cap_usd", StandardSQLTypeName.NUMERIC).build(),
    Field.newBuilder("total_volume_usd", StandardSQLTypeName.NUMERIC).build(),
)

fun rowToJsonLine(row: Row): String {
    val price = fmtNumeric(row.priceUsd)?.let { "\"$it\"" } ?: "null"
    val marketCap = fmtNumeric(row.marketCapUsd)?.let { "\"$it\"" } ?: "null"
    val volume = fmtNumeric(row.totalVolumeUsd)?.let { "\"$it\"" } ?: "null"
    return "{\"coin_id\":\"${row.coinId}\",\"timestamp\":${row.timestampEpochSeconds}," +
        "\"price_usd\":$price,\"market_cap_usd\":$marketCap,\"total_volume_usd\":$volume}"
}

suspend fun mergeRowsIntoTarget(
    bigquery: BigQuery,
    datasetId: String,
    targetTable: String,
    rows: List<Row>,
    mergeMutex: Mutex,
) {
    if (rows.isEmpty()) return
    val project = bigquery.options.projectId
    val stagingTableId = TableId.of(project, datasetId, "${STAGING_TABLE_PREFIX}_${UUID.randomUUID().toString().replace("-", "")}")
    try {
        withContext(Dispatchers.IO) {
            val writeChannelConfig = WriteChannelConfiguration.newBuilder(stagingTableId)
                .setFormatOptions(FormatOptions.json())
                .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
                .setSchema(marketCapSchema())
                .build()
            val writer = bigquery.writer(writeChannelConfig)
            Channels.newOutputStream(writer).use { out ->
                for (row in rows) {
                    out.write((rowToJsonLine(row) + "\n").toByteArray(StandardCharsets.UTF_8))
                }
            }
            writer.job.waitFor()
        }

        val mergeSql = """
            MERGE `$project.$datasetId.$targetTable` T
            USING `$project.$datasetId.${stagingTableId.table}` S
            ON T.coin_id = S.coin_id AND T.timestamp = S.timestamp
            WHEN MATCHED THEN UPDATE SET
                price_usd = S.price_usd,
                market_cap_usd = S.market_cap_usd,
                total_volume_usd = S.total_volume_usd
            WHEN NOT MATCHED THEN
                INSERT (coin_id, timestamp, price_usd, market_cap_usd, total_volume_usd)
                VALUES (S.coin_id, S.timestamp, S.price_usd, S.market_cap_usd, S.total_volume_usd)
        """.trimIndent()

        mergeMutex.withLock {
            withContext(Dispatchers.IO) {
                bigquery.query(QueryJobConfiguration.newBuilder(mergeSql).build())
            }
        }
    } finally {
        withContext(Dispatchers.IO) {
            bigquery.delete(stagingTableId)
        }
    }
}

suspend fun backfillCoin(
    coinId: String,
    windows: List<Window>,
    apiKey: String?,
    limiter: RateLimiter,
    bigquery: BigQuery,
    datasetId: String,
    targetTable: String,
    mergeMutex: Mutex,
) {
    var windowsProcessed = 0
    for (window in windows) {
        val fromTs = window.start.atStartOfDay(ZoneOffset.UTC).toEpochSecond()
        val toTs = window.end.atStartOfDay(ZoneOffset.UTC).toEpochSecond()
        try {
            val body = fetchMarketChartRange(coinId, fromTs, toTs, apiKey, limiter)
            val rows = responseToRows(coinId, body)
            mergeRowsIntoTarget(bigquery, datasetId, targetTable, rows, mergeMutex)
            windowsProcessed++
        } catch (e: Exception) {
            System.err.println("  $coinId: FAILED on window ${window.start}..${window.end}: ${e.message}")
            break
        }
    }
    println("  $coinId: $windowsProcessed/${windows.size} windows processed")
}

fun main() = runBlocking {
    val projectId = System.getenv("GCP_PROJECT_ID") ?: "gcp-crypto-tracker"
    val datasetId = System.getenv("BQ_DATASET") ?: "crypto_tracker"
    val targetTable = System.getenv("TARGET_TABLE") ?: "market_cap_history_temp"
    val apiKey = System.getenv("COINGECKO_API_KEY")
    val requestsPerMinute = System.getenv("REQUESTS_PER_MINUTE")?.toIntOrNull() ?: DEFAULT_REQUESTS_PER_MINUTE
    val maxConcurrentCoins = System.getenv("MAX_CONCURRENT_COINS")?.toIntOrNull() ?: DEFAULT_MAX_CONCURRENT_COINS
    val totalDays = System.getenv("TOTAL_DAYS")?.toLongOrNull() ?: DEFAULT_TOTAL_DAYS
    val limitCoins = System.getenv("LIMIT_COINS")?.toIntOrNull()

    val bigquery: BigQuery = BigQueryOptions.newBuilder().setProjectId(projectId).build().service

    var coinIds = withContext(Dispatchers.IO) {
        val result = bigquery.query(
            QueryJobConfiguration.newBuilder(
                "SELECT coin_id FROM `$projectId.$datasetId.coins_registry` ORDER BY coin_id"
            ).build()
        )
        result.iterateAll().map { it.get("coin_id").stringValue }
    }
    if (limitCoins != null) coinIds = coinIds.take(limitCoins)

    println(
        "backfilling market cap history for ${coinIds.size} coins into $targetTable, $totalDays days back, " +
            "max_concurrent_coins=$maxConcurrentCoins, requests_per_minute=$requestsPerMinute"
    )

    val windows = buildWindows(totalDays, CHUNK_DAYS, LocalDate.now(ZoneOffset.UTC))
    val limiter = RateLimiter(requestsPerMinute)
    val semaphore = Semaphore(maxConcurrentCoins)
    val mergeMutex = Mutex()

    val jobs = coinIds.map { coinId ->
        async {
            semaphore.withPermit {
                backfillCoin(coinId, windows, apiKey, limiter, bigquery, datasetId, targetTable, mergeMutex)
            }
        }
    }
    jobs.awaitAll()

    println("backfill complete")
}
