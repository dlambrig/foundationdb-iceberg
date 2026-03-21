# Stage 1: Build the JAR
# Stage 1: Build the JAR
FROM gradle:9.4.0-jdk17 AS build
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
# Change bootJar to build to be safe
RUN ./gradlew build --no-daemon -x test

# Stage 2: Run the Application
FROM eclipse-temurin:17-jre-jammy
RUN apt-get update && apt-get install -y wget && \
    wget https://github.com/apple/foundationdb/releases/download/7.3.38/foundationdb-clients_7.3.38-1_amd64.deb && \
    dpkg -i foundationdb-clients_7.3.38-1_amd64.deb

WORKDIR /app
# This picks up the generated JAR from the build/libs folder
COPY --from=build /home/gradle/src/build/libs/*.jar app.jar

ENV FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster
ENTRYPOINT ["java", "-cp", "app.jar", "IcebergRestServer"]

