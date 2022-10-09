use std::process;
use std::{collections::HashSet, env};

use clap::{Parser, Subcommand};
use serde::Deserialize;
use sqlx::{
    mysql::{MySqlPool, MySqlRow},
    Row,
};
use uuid::Uuid;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long)]
    verbose: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(
        about = "execute a snapshot for the given tables",
        arg_required_else_help = true
    )]
    Snapshot { tables: Vec<String> },
}

#[derive(Deserialize, Debug)]
struct DatabaseConfig {
    hostname: String,
    username: String,
    password: String,
    database: String,
    table: String,
}

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let args = Cli::parse();
    let home = env::var("HOME").expect("Home dir is not set");
    let config: DatabaseConfig = config::Config::builder()
        .add_source(config::File::with_name(&format!(
            "{home}/.config/dbz/config"
        )))
        .add_source(config::Environment::with_prefix("DBZ"))
        .build()
        .unwrap()
        .try_deserialize()
        .unwrap();

    let url = format!(
        "mysql://{}:{}@{}/{}",
        config.username, config.password, config.hostname, config.database
    );
    let conn = MySqlPool::connect(&url).await?;

    match args.command {
        Command::Snapshot { tables } => {
            println!("Executing snapshot for {:?}", tables);
            let data = format!("{{ \"data-collections\": {:?} }}", tables);

            let rows: HashSet<String> = sqlx::query(
                "select concat(table_schema, '.', table_name) from information_schema.tables",
            )
            .try_map(|row: MySqlRow| row.try_get(0))
            .fetch_all(&conn)
            .await?
            .into_iter()
            .collect();

            let invalid_tables: Vec<_> = tables
                .iter()
                .filter(|&table| !rows.contains(table))
                .collect();

            if !invalid_tables.is_empty() {
                eprintln!("Error executing snapshot!");
                eprintln!("  unknown tables: {:?}", invalid_tables);
                process::exit(1);
            }

            let sql = format!("insert into {} values (?, ?, ?)", config.table);
            sqlx::query(&sql)
                .bind(Uuid::new_v4().to_string())
                .bind("execute-snapshot")
                .bind(data)
                .execute(&conn)
                .await?;
        }
    }

    Ok(())
}
