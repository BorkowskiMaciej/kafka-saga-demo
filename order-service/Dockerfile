FROM eclipse-temurin:26-jdk AS build

WORKDIR /workspace
COPY . .
RUN ./mvnw -pl order-service -am -DskipUnitTests=true package

FROM eclipse-temurin:26-jre

WORKDIR /app
COPY --from=build /workspace/order-service/target/order-service-*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
