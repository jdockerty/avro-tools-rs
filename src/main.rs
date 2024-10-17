use std::{io::Write, path::PathBuf};

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
struct Cli {
    #[clap(subcommand)]
    commands: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Display the encoded data as JSON.
    ToJSON { input: PathBuf },
    /// Display the schema as JSON.
    Schema { input: PathBuf },
    /// Read the Avro key-value metadata header.
    GetMetadata { input: PathBuf },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let mut out = std::io::stdout().lock();

    match cli.commands {
        Commands::ToJSON { input } => {
            let file = std::fs::File::open(input)?;
            let avro_reader = apache_avro::Reader::new(file)?;
            for r in avro_reader {
                serde_json::to_writer(
                    &mut out,
                    &apache_avro::from_value::<serde_json::Value>(&r?)?,
                )?;
            }
        }
        Commands::GetMetadata { input } => {
            let file = std::fs::File::open(input)?;
            let avro_reader = apache_avro::Reader::new(file)?;
            for (k, v) in avro_reader.user_metadata() {
                writeln!(out, "{k}={}", String::from_utf8_lossy(v))?;
            }
        }
        Commands::Schema { input } => {
            let file = std::fs::File::open(input)?;
            let avro_reader = apache_avro::Reader::new(file)?;
            serde_json::to_writer_pretty(out, avro_reader.writer_schema())?;
        }
    }
    Ok(())
}
