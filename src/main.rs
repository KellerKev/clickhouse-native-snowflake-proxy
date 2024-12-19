use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use clickhouse_srv::{
    ClickHouseServer, ClickHouseSession, types::{Block, Progress}, errors::Result,
    connection::Connection,
    CHContext,
};
use async_trait::async_trait;
use snowflake_connector_rs::{SnowflakeClient, SnowflakeAuthMethod, SnowflakeClientConfig};
use regex::Regex;
use std::env;

struct MySession {
    account: String,
    role: Option<String>,
    warehouse: Option<String>,
    database: Option<String>,
    schema: Option<String>,
    timeout: Option<Duration>,
}

#[async_trait]
impl ClickHouseSession for MySession {
    async fn execute_query(
        &self,
        context: &mut CHContext,
        connection: &mut Connection,
    ) -> Result<()> {
        let query = &context.state.query;
        println!("Received query: {}", query);

        // Unwrap hello request to get user and password
        let hello = context.hello.as_ref().ok_or_else(|| {
            clickhouse_srv::errors::Error::Other("No hello request received from client".into())
        })?;
        let user = &hello.user;
        let pass = &hello.password;

        let translated_query = translate_query(query);

        let snowflake_client = self
            .create_snowflake_client(user, pass)
            .map_err(|e| clickhouse_srv::errors::Error::Other(format!("{:?}", e).into()))?;

        self.execute_snowflake_query(&snowflake_client, &translated_query, connection)
            .await
            .map_err(|e| clickhouse_srv::errors::Error::Other(format!("{:?}", e).into()))
    }

    fn dbms_name(&self) -> &str {
        "clickhouse-server"
    }

    fn timezone(&self) -> &str {
        "UTC"
    }

    fn get_progress(&self) -> Progress {
        Progress::default()
    }
}

impl MySession {
    fn create_snowflake_client(&self, username: &str, password: &str) -> anyhow::Result<SnowflakeClient> {
        let auth_method = determine_auth_method(password)?;

        let client = SnowflakeClient::new(
            username,
            auth_method,
            SnowflakeClientConfig {
                account: self.account.clone(),
                role: self.role.clone(),
                warehouse: self.warehouse.clone(),
                database: self.database.clone(),
                schema: self.schema.clone(),
                timeout: self.timeout,
            },
        )?;

        Ok(client)
    }

    async fn execute_snowflake_query(
        &self,
        snowflake_client: &SnowflakeClient,
        translated_query: &str,
        connection: &mut Connection,
    ) -> anyhow::Result<()> {
        let session = snowflake_client.create_session().await?;
        let rows = session.query(translated_query).await?;

        if rows.is_empty() {
            let block = Block::new();
            connection.write_block(&block).await?;
            return Ok(());
        }

        let mut block = Block::new();
        let mut column_types = Vec::new();

        let first_row = &rows[0];
        for column_name in first_row.column_names() {
            let value = first_row.get::<String>(column_name).unwrap_or_default();
            let inferred_type = infer_type(&value);
            column_types.push((column_name.clone(), inferred_type));
        }

        for (column_name, inferred_type) in &column_types {
            match inferred_type.as_str() {
                "Int64" => {
                    let values: Vec<i64> = rows
                        .iter()
                        .map(|row| {
                            row.get::<String>(column_name).unwrap_or_default().parse::<i64>().unwrap_or(0)
                        })
                        .collect();
                    block = block.add_column(column_name, values);
                }
                "Float64" => {
                    let values: Vec<f64> = rows
                        .iter()
                        .map(|row| {
                            row.get::<String>(column_name).unwrap_or_default().parse::<f64>().unwrap_or(0.0)
                        })
                        .collect();
                    block = block.add_column(column_name, values);
                }
                "Date" | "String" => {
                    let values: Vec<String> = rows
                        .iter()
                        .map(|row| row.get::<String>(column_name).unwrap_or_default())
                        .collect();
                    block = block.add_column(column_name, values);
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unsupported inferred type: {} for column {}",
                        inferred_type,
                        column_name
                    ));
                }
            }
        }

        connection.write_block(&block).await?;
        Ok(())
    }
}

fn infer_type(value: &str) -> String {
    let int_regex = Regex::new(r"^\d+$").unwrap();
    let float_regex = Regex::new(r"^\d+\.\d+$").unwrap();
    let date_regex = Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap();
    let datetime_regex = Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$").unwrap();

    if int_regex.is_match(value) {
        "Int64".to_string()
    } else if float_regex.is_match(value) {
        "Float64".to_string()
    } else if date_regex.is_match(value) {
        "Date".to_string()
    } else if datetime_regex.is_match(value) {
        "DateTime".to_string()
    } else {
        "String".to_string()
    }
}

fn translate_query(query: &str) -> String {
    query
        .replace("LIMIT", "FETCH FIRST")
        .replace("now()", "CURRENT_TIMESTAMP")
}

fn determine_auth_method(password: &str) -> anyhow::Result<SnowflakeAuthMethod> {
    if password.contains("-----BEGIN PRIVATE KEY-----") {
        let key_pass = env::var("SNOWFLAKE_KEY_PASS").unwrap_or_default();
        Ok(SnowflakeAuthMethod::KeyPair {
            encrypted_pem: password.to_string(),
            password: key_pass.into_bytes(),
        })
    } else {
        Ok(SnowflakeAuthMethod::Password(password.to_string()))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let account = env::var("SNOWFLAKE_ACCOUNT").map_err(|_| {
        anyhow::anyhow!("Missing environment variable: SNOWFLAKE_ACCOUNT")
    })?;
    let role = env::var("SNOWFLAKE_ROLE").ok();
    let warehouse = env::var("SNOWFLAKE_WAREHOUSE").ok();
    let database = env::var("SNOWFLAKE_DATABASE").ok();
    let schema = env::var("SNOWFLAKE_SCHEMA").ok();
    let timeout_secs = env::var("SNOWFLAKE_TIMEOUT").ok().and_then(|v| v.parse::<u64>().ok());
    let timeout = timeout_secs.map(Duration::from_secs);

    // Determine the address to bind to
    let listen_addr = env::var("LISTEN_ADDRESS").unwrap_or_else(|_| "127.0.0.1:9000".to_string());

    println!("Starting server on {}", listen_addr);
    let listener = TcpListener::bind(&listen_addr).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        let session = Arc::new(MySession {
            account: account.clone(),
            role: role.clone(),
            warehouse: warehouse.clone(),
            database: database.clone(),
            schema: schema.clone(),
            timeout,
        });

        tokio::spawn(async move {
            if let Err(e) = ClickHouseServer::run_on_stream(session, stream).await {
                eprintln!("Error handling client: {:?}", e);
            }
        });
    }
}
