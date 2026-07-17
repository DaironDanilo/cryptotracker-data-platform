package com.cryptotracker.ingest

import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.StorageOptions
import com.google.gson.Gson
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpResponse.BodyHandlers
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.Executors
import java.util.zip.ZipInputStream
import javax.xml.parsers.DocumentBuilderFactory
import kotlin.random.Random
import java.net.http.HttpRequest as JavaHttpRequest

// Plain embedded HTTP server, not the Cloud Functions Framework: Google's
// Cloud Functions buildpack only detects a literal Groovy `build.gradle` file
// (it appends a classpath-extraction task to it via raw text append), so it
// can't build a pure Kotlin-DSL (`build.gradle.kts`) project. Cloud Run's
// generic Java/Gradle buildpack does support `.kts`, so this ships as a Cloud
// Run service instead -- functionally identical to a Cloud Function for
// Workflows' purposes (one HTTP call per unit of work), just without the
// Functions Framework wrapper.

private const val ARCHIVE_HOST = "https://data.binance.vision"
private const val LISTING_HOST = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
private const val GCS_BUCKET = "gcp-crypto-tracker-raw-binance"
private const val INTERVAL = "1h"

// Binance switched kline archive timestamps from milliseconds to microseconds
// around Jan 2025 (13-digit vs 16-digit values). Anything above this is µs.
private const val MICROSECOND_THRESHOLD = 100_000_000_000_000L

private val httpClient: HttpClient = HttpClient.newBuilder().build()
private val gson = Gson()

fun main() {
    val port = System.getenv("PORT")?.toIntOrNull() ?: 8080
    val server = HttpServer.create(InetSocketAddress(port), 0)
    server.executor = Executors.newCachedThreadPool()
    server.createContext("/coins", CoinsHandler())
    server.createContext("/download", DownloadHandler())
    server.createContext("/", HttpHandler { exchange ->
        exchange.sendResponseHeaders(200, 0)
        exchange.responseBody.use { it.write("ok".toByteArray()) }
    })
    server.start()
    println("listening on port $port")
}

private fun writeJson(exchange: HttpExchange, status: Int, body: Any) {
    val bytes = gson.toJson(body).toByteArray(Charsets.UTF_8)
    exchange.responseHeaders.add("Content-Type", "application/json")
    exchange.sendResponseHeaders(status, bytes.size.toLong())
    exchange.responseBody.use { it.write(bytes) }
}

private class CoinsHandler : HttpHandler {
    override fun handle(exchange: HttpExchange) {
        try {
            if (exchange.requestMethod != "GET") {
                writeJson(exchange, 405, mapOf("error" to "method not allowed"))
                return
            }
            val bq = BigQueryOptions.getDefaultInstance().service
            val query = QueryJobConfiguration.newBuilder(
                "SELECT coin_id, binance_symbol FROM `gcp-crypto-tracker.bronze.coins_registry` " +
                    "WHERE binance_symbol IS NOT NULL ORDER BY coin_id",
            ).build()
            val result = bq.query(query)
            val coins = result.iterateAll().map { row ->
                mapOf(
                    "coin_id" to row.get("coin_id").stringValue,
                    "binance_symbol" to row.get("binance_symbol").stringValue,
                )
            }
            writeJson(exchange, 200, coins)
        } catch (e: Exception) {
            writeJson(exchange, 500, mapOf("error" to (e.message ?: e.toString())))
        }
    }
}

private class DownloadHandler : HttpHandler {
    override fun handle(exchange: HttpExchange) {
        try {
            if (exchange.requestMethod != "POST") {
                writeJson(exchange, 405, mapOf("error" to "method not allowed"))
                return
            }
            val body = exchange.requestBody.bufferedReader().readText()
            val json = JsonParser.parseString(body).asJsonObject
            val coinId = json.get("coin_id").asString
            val binanceSymbol = json.get("binance_symbol").asString

            val publishTime = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
            val monthlyPrefix = "data/spot/monthly/klines/$binanceSymbol/$INTERVAL/"
            val dailyPrefix = "data/spot/daily/klines/$binanceSymbol/$INTERVAL/"

            val monthlyKeys = listArchiveFiles(monthlyPrefix).filter { !it.endsWith(".CHECKSUM") }.sorted()
            val lastMonthlyYm = monthlyKeys.lastOrNull()?.let { yearMonthFromKey(it) }
            val dailyKeysAll = listArchiveFiles(dailyPrefix).filter { !it.endsWith(".CHECKSUM") }.sorted()
            val dailyKeys = if (lastMonthlyYm != null) {
                dailyKeysAll.filter { yearMonthFromKey(it) > lastMonthlyYm }
            } else {
                dailyKeysAll
            }

            val allKeys = monthlyKeys + dailyKeys
            val storage = StorageOptions.getDefaultInstance().service

            var filesWritten = 0
            var rowsWritten = 0
            for ((idx, key) in allKeys.withIndex()) {
                val rows = downloadAndParseCsv(key)
                val ndjson = buildString {
                    for (r in rows) {
                        append(rowToJson(r, coinId, publishTime))
                        append('\n')
                    }
                }
                val filename = key.substringAfterLast('/').removeSuffix(".zip")
                val blobId = BlobId.of(GCS_BUCKET, "$coinId/$filename.ndjson")
                val blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/x-ndjson").build()
                storage.create(blobInfo, ndjson.toByteArray(Charsets.UTF_8))
                filesWritten++
                rowsWritten += rows.size

                if (idx < allKeys.size - 1) {
                    Thread.sleep(Random.nextLong(100, 5000))
                }
            }

            writeJson(
                exchange,
                200,
                mapOf("coin_id" to coinId, "files_written" to filesWritten, "rows_written" to rowsWritten),
            )
        } catch (e: Exception) {
            writeJson(exchange, 500, mapOf("error" to (e.message ?: e.toString())))
        }
    }
}

private fun listArchiveFiles(prefix: String): List<String> {
    val keys = mutableListOf<String>()
    var marker: String? = null
    while (true) {
        val params = mutableListOf(
            "delimiter=/",
            "prefix=${URLEncoder.encode(prefix, "UTF-8")}",
            "max-keys=1000",
        )
        if (marker != null) params.add("marker=${URLEncoder.encode(marker, "UTF-8")}")
        val uri = URI.create("$LISTING_HOST?${params.joinToString("&")}")
        val req = JavaHttpRequest.newBuilder(uri).GET().timeout(Duration.ofSeconds(30)).build()
        val resp = httpClient.send(req, BodyHandlers.ofInputStream())
        val doc = DocumentBuilderFactory.newInstance().apply { isNamespaceAware = false }
            .newDocumentBuilder().parse(resp.body())
        val contentsNodes = doc.getElementsByTagName("Contents")
        val pageKeys = mutableListOf<String>()
        for (i in 0 until contentsNodes.length) {
            val children = contentsNodes.item(i).childNodes
            for (j in 0 until children.length) {
                val child = children.item(j)
                if (child.nodeName == "Key") pageKeys.add(child.textContent)
            }
        }
        keys.addAll(pageKeys)
        val isTruncated = doc.getElementsByTagName("IsTruncated").item(0)?.textContent == "true"
        if (!isTruncated || pageKeys.isEmpty()) break
        marker = pageKeys.last()
    }
    return keys.filter { it.endsWith(".zip") }
}

private fun yearMonthFromKey(key: String): String {
    val stem = key.substringAfterLast('/').removeSuffix(".zip")
    val parts = stem.split("-")
    return "${parts[2]}-${parts[3]}"
}

private data class CsvRow(
    val openTime: String,
    val open: String,
    val high: String,
    val low: String,
    val close: String,
    val volume: String,
    val closeTime: String,
    val quoteVolume: String,
    val tradeCount: String,
)

private fun downloadAndParseCsv(key: String): List<CsvRow> {
    val uri = URI.create("$ARCHIVE_HOST/$key")
    val req = JavaHttpRequest.newBuilder(uri).GET().timeout(Duration.ofSeconds(60)).build()
    val resp = httpClient.send(req, BodyHandlers.ofInputStream())
    val rows = mutableListOf<CsvRow>()
    ZipInputStream(resp.body()).use { zis ->
        var entry = zis.nextEntry
        while (entry != null) {
            if (entry.name.endsWith(".csv")) {
                val bos = ByteArrayOutputStream()
                zis.copyTo(bos)
                val text = bos.toString(Charsets.UTF_8)
                for (line in text.lineSequence()) {
                    if (line.isBlank()) continue
                    val f = line.split(",")
                    rows.add(CsvRow(f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8]))
                }
            }
            entry = zis.nextEntry
        }
    }
    return rows
}

private fun toEpochInstant(raw: String): Instant {
    val value = raw.toLong()
    return if (value > MICROSECOND_THRESHOLD) {
        Instant.ofEpochMilli(value / 1000)
    } else {
        Instant.ofEpochMilli(value)
    }
}

private fun rowToJson(r: CsvRow, coinId: String, publishTime: String): String {
    val obj = JsonObject()
    obj.addProperty("coin_id", coinId)
    obj.addProperty("interval", INTERVAL)
    obj.addProperty("open_time", DateTimeFormatter.ISO_INSTANT.format(toEpochInstant(r.openTime)))
    obj.addProperty("close_time", DateTimeFormatter.ISO_INSTANT.format(toEpochInstant(r.closeTime)))
    obj.addProperty("open", r.open)
    obj.addProperty("high", r.high)
    obj.addProperty("low", r.low)
    obj.addProperty("close", r.close)
    obj.addProperty("volume", r.volume)
    obj.addProperty("quote_volume", r.quoteVolume)
    obj.addProperty("trade_count", r.tradeCount.toLong())
    obj.addProperty("is_closed", true)
    obj.addProperty("publish_time", publishTime)
    return gson.toJson(obj)
}
