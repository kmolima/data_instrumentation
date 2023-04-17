# Docker multi-stage build: https://docs.docker.com/build/building/multi-stage/
#
# Build stage
#
FROM maven:3.9.1-eclipse-temurin-11-alpine AS build
MAINTAINER keila

COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean package # use wrapper as alternative


#
# Package stage
#
FROM openjdk:11-jre-slim
COPY --from=build /home/app/target/data_instrumentation-1.0-SNAPSHOT-jar-with-dependencies.jar /usr/local/lib/app.jar
COPY config/config.yaml /etc/config.yaml

# Use non-root user and group
RUN addgroup appgroup
RUN adduser --disabled-password appuser --ingroup appgroup --no-create-home
RUN chown -R appuser:appgroup /etc/config.yaml /usr/local/lib/app.jar
USER appuser

# Target port for Prometheus to scrape metrics
EXPOSE 9091
ENTRYPOINT ["java","-jar","/usr/local/lib/app.jar"]
CMD ["/etc/config.yaml"]
