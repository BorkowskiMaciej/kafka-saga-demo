FROM eclipse-temurin:26-jdk AS build

WORKDIR /workspace
COPY . .
RUN ./mvnw -pl inventory-service -am -DskipUnitTests=true package

FROM eclipse-temurin:26-jre

WORKDIR /app
COPY --from=build /workspace/inventory-service/target/inventory-service-*.jar app.jar

EXPOSE 8082
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
