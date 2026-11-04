FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN mvn -B -DskipTests package

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /src/target/continuing-gitstore.jar /app/continuing-gitstore.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/continuing-gitstore.jar"]
CMD ["node"]
