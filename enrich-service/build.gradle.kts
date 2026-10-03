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
    testImplementation(libs.assertj.core)
    testImplementation(libs.awaitility)
    testRuntimeOnly(libs.junit.launcher)
}

application {
    mainClass = "ai.pipestream.enrich.server.GrpcEnrichServer"
}

// Pinned VLM clients send a Host header of their own, which the JDK HTTP
// client allows only with this property set before its first use. The
// server sets it at startup; the test JVM sets it here, since any test may
// be the first to touch the JDK HTTP client.
tasks.test {
    systemProperty("jdk.httpclient.allowRestrictedHeaders", "host")
}
