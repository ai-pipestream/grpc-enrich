# Build stage: compile, run the full test suite, and assemble the
# distribution. An image never ships from a tree whose tests did not pass.
# (the -dev variant: the plain dhi.io image has no shell, so RUN cannot work)
FROM dhi.io/eclipse-temurin:25-jdk-dev AS build
WORKDIR /src
COPY . .
RUN ./gradlew --no-daemon build :enrich-service:installDist

# Runtime: Docker Hardened Images temurin. No shell, no package manager, and
# the entrypoint already runs as a non-root uid, so there is nothing to
# harden here ourselves. No shell also means the installDist start script
# cannot run, so invoke java on the distribution jars directly. The server
# is diskless by doctrine; run with --read-only and it works unchanged.
FROM dhi.io/eclipse-temurin:25
COPY --from=build /src/enrich-service/build/install/enrich-service /opt/grpc-enrich
EXPOSE 50056 50068
ENTRYPOINT ["java", "-cp", "/opt/grpc-enrich/lib/*", "ai.pipestream.enrich.server.GrpcEnrichServer"]
