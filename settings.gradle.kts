rootProject.name = "grpc-enrich"

include("enrich-api")
include("enrich-service")

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}
