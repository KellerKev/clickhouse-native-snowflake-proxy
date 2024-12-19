FROM debian:bookworm

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY target/release/clickhouse_snowflake_proxy /usr/local/bin/clickhouse_snowflake_proxy
RUN chmod +x /usr/local/bin/clickhouse_snowflake_proxy

ENTRYPOINT ["/usr/local/bin/clickhouse_snowflake_proxy"]
