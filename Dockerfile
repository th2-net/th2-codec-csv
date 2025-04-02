FROM gradle:8.11.1-jdk21 AS build
ARG release_version
COPY ./ .
RUN gradle build dockerPrepare \
    -Prelease_version=${release_version}

FROM adoptopenjdk/openjdk11:alpine
WORKDIR /home
COPY --from=build /home/gradle/build/docker ./
ENTRYPOINT ["/home/service/bin/service"]