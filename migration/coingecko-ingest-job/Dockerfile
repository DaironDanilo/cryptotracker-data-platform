FROM eclipse-temurin:17-jdk AS build
WORKDIR /app
COPY . .
RUN ./gradlew shadowJar --no-daemon

# Distroless: no shell, no package manager, nothing beyond the JRE + this
# jar. A fat-jar `java -jar` needs nothing else at runtime. :nonroot runs
# as an unprivileged user (uid 65532) instead of root. Debug locally with
# the `:debug-nonroot` tag (adds a busybox shell) if you ever need to
# `docker exec` in -- swap back to `:nonroot` before deploying.
FROM gcr.io/distroless/java17-debian12:nonroot
WORKDIR /app
COPY --from=build --chown=nonroot:nonroot /app/build/libs/*.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
