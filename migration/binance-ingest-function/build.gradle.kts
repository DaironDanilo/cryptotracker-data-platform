plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.shadow)
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.gson)
    implementation(platform(libs.gcp.libraries.bom))
    implementation(libs.google.cloud.bigquery)
    implementation(libs.google.cloud.storage)
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17)
    }
}

application {
    mainClass.set("com.cryptotracker.ingest.MainKt")
}

// Cloud Run's generic Java/Gradle buildpack runs `gradle clean assemble` and
// launches whatever the `application` plugin's start script points at --
// shadowJar bundles all runtime deps into one self-contained jar.
tasks.named("assemble") {
    dependsOn(tasks.named("shadowJar"))
}
