# Build stage: compile, run the full test suite, and assemble the
# distribution. An image never ships from a tree whose tests did not pass.
FROM eclipse-temurin:25-jdk AS build
WORKDIR /src
COPY . .
RUN ./gradlew --no-daemon build :enrich-service:installDist

# Runtime: JRE only, non-root, nothing writable needed. The server is
# diskless by doctrine; run with --read-only and it works unchanged.
FROM eclipse-temurin:25-jre
COPY --from=build /src/enrich-service/build/install/enrich-service /opt/grpc-enrich
RUN useradd --system --no-create-home grpcenrich
USER grpcenrich
EXPOSE 50056 50057
ENTRYPOINT ["/opt/grpc-enrich/bin/enrich-service"]
