plugins {
    application
}

dependencies {
    implementation(project(":enrich-api"))
    implementation(libs.protobuf.java.util)
    implementation(libs.gson)
    implementation(libs.grpc.netty.shaded)
    implementation(libs.grpc.services)
    runtimeOnly(libs.log4j.core)

    testImplementation(libs.grpc.inprocess)
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
}

application {
    mainClass = "ai.pipestream.enrich.server.GrpcEnrichServer"
}
