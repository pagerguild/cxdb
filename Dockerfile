# Runtime-only image for cxdb.
# The binary and frontend are built by CI and copied in.
# Build: docker build --build-arg BINARY=cxdb-server --build-arg FRONTEND=frontend/out -t tylerguilde/cxdb .
FROM debian:bookworm-slim

ARG BINARY=cxdb-server
ARG FRONTEND=frontend/out

RUN apt-get update && apt-get install -y --no-install-recommends \
    nginx supervisor ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /app /data /var/log/supervisor

COPY ${BINARY} /app/cxdb
COPY ${FRONTEND} /usr/share/nginx/html
COPY deploy/nginx.conf /etc/nginx/nginx.conf
COPY deploy/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

ENV CXDB_DATA_DIR=/data
ENV CXDB_BIND=0.0.0.0:9009
ENV CXDB_HTTP_BIND=127.0.0.1:9010
EXPOSE 80 9009

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost/v1/contexts?limit=1 || exit 1

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
