plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.shadow)
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.google.cloud.bigquery)
    implementation(libs.gson)
}

kotlin {
    jvmToolchain(17)
}

tasks.shadowJar {
    archiveBaseName.set("app")
    archiveClassifier.set("")
    archiveVersion.set("")
    mergeServiceFiles()
    manifest {
        attributes["Main-Class"] = "com.cryptotracker.coingecko.MainKt"
    }
}

tasks.named("assemble") {
    dependsOn(tasks.shadowJar)
}
