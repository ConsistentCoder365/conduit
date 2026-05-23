FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle gradle
RUN ./gradlew --no-daemon -q dependencies > /dev/null
COPY src src
RUN ./gradlew --no-daemon -q installDist -x test

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/build/install/conduit /app
COPY config /app/config
ENV CONDUIT_OUT=/app/out
EXPOSE 9400 5140 8088
# exec form: the JVM is PID 1 and receives SIGTERM directly, so the drain runs on `docker stop`.
ENTRYPOINT ["/app/bin/conduit"]
CMD ["/app/config/conduit.yaml"]
