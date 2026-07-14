plugins {
    kotlin("jvm") version "2.0.21" apply false
    kotlin("plugin.serialization") version "2.0.21" apply false
}

allprojects {
    group = "com.cryptotracker"
    version = "0.1.0"

    repositories {
        mavenCentral()
    }
}
