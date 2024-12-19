# clickhouse-native-snowflake-proxy
Clickhouse Drop In server in RUST that proxies requests to Snowflake (experimental atm)

Complete with some samples using Golang (Go) and the Clickhouse Golang driver. 

TLS support can be added by putting a TCP TLS reverse proxy in of this driver such as stunnel.

For now if you want to run the samples and test out the proxy server just update the docker-compose file with your own credentials for Snowflake, 
then run docker compose up and test the Go samples again the proxies. 

You can also build the proxy yourself by following these steps:

1) Install RUST
2) Install Go(lang)

3) git clone https://github.com/KellerKev/clickhouse-native-snowflake-proxy.git
4) cd clickhouse-native-snowflake-proxy
5) Change the Snowflake credentials in main.rs to your own. 
6) cargo build --release
7) ./target/release/clickhouse_native_snowflake_proxy
8) cd go-client-samples
9) go run (pick one of the samples)

The Go samples program will execute queries using the Clickhouse Go driver (SELECTS, INSERTS) which will be actually executed in your Snowflake account and results send back to the Clickhouse proxy.