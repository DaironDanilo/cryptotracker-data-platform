rootProject.name = "cryptotracker-data-platform"

// Modules are added as each phase lands. Phase 2 (coin-registry) is implemented
// in Python, not Kotlin, so it is not a Gradle module (see coin-registry/README.md).
//
// Uncomment as each phase is implemented:
// include(":common")
// include(":ws-worker")
// include(":reconciliation-job")
// include(":postgres-subscriber")
// include(":redis-subscriber")
