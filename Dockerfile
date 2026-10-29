# Build stage: keep the toolchain out of the runtime image
FROM ubuntu:22.04 AS build

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
        cmake gcc make libssl-dev libcrypt-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY CMakeLists.txt ./
COPY src ./src
COPY tests ./tests
COPY etc ./etc

RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build -j"$(nproc)" \
    && cmake --install build --prefix /stage

# Runtime stage
FROM ubuntu:22.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
        libssl3 libcrypt1 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /var/lib/sol --shell /usr/sbin/nologin sol \
    && mkdir -p /var/lib/sol /var/log/sol \
    && chown sol:sol /var/lib/sol /var/log/sol

COPY --from=build /stage/bin/sol /usr/local/bin/sol
COPY --from=build /stage/etc/sol/sol.conf /etc/sol/sol.conf

# The sample configuration listens on 127.0.0.1:8883; the container has to
# listen on every interface on the standard port. Override the whole command
# (or mount a configuration) to change it
EXPOSE 1883

USER sol
VOLUME ["/var/lib/sol"]

ENTRYPOINT ["/usr/local/bin/sol"]
CMD ["-a", "0.0.0.0", "-p", "1883", "-c", "/etc/sol/sol.conf"]
