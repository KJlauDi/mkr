FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /build

COPY producer/pom.xml ./pom.xml
COPY producer/src ./src
RUN mvn -q package

FROM eclipse-temurin:17-jre

WORKDIR /app

COPY --from=build /build/target/kafka-demo-producer-1.0.0-jar-with-dependencies.jar ./app.jar

CMD ["java", "-jar", "app.jar"]
