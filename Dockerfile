FROM amazoncorretto:11-alpine-jdk
WORKDIR /home
COPY ./build/docker .
ENTRYPOINT ["/home/service/bin/service"]