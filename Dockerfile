# Stage 1: Build the JAR
FROM gradle:9.4.0-jdk17 AS build
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN ./gradlew build --no-daemon -x test

# Stage 2: Run the Application
FROM eclipse-temurin:17-jre-jammy
RUN apt-get update && apt-get install -y wget && \
    wget https://github.com/apple/foundationdb/releases/download/7.3.38/foundationdb-clients_7.3.38-1_amd64.deb && \
    dpkg -i foundationdb-clients_7.3.38-1_amd64.deb

WORKDIR /app
COPY --from=build /home/gradle/src/build/libs/*.jar app.jar

ENV FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster
ENTRYPOINT ["java", "-jar", "app.jar"]
