FROM eclipse-temurin:26-jdk AS build

WORKDIR /workspace
COPY . .
RUN ./mvnw -pl payment-service -am -DskipUnitTests=true package

FROM eclipse-temurin:26-jre

WORKDIR /app
COPY --from=build /workspace/payment-service/target/payment-service-*.jar app.jar

EXPOSE 8081
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
