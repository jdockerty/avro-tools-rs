use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
};

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

fn avro_reader<'a>(path: impl AsRef<Path>) -> anyhow::Result<apache_avro::Reader<'a, File>> {
    let file = File::open(path)?;
    Ok(apache_avro::Reader::new(file)?)
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let mut out = std::io::stdout().lock();

    match cli.commands {
        Commands::ToJSON { input } => {
            let reader = avro_reader(input)?;
            for r in reader {
                serde_json::to_writer(
                    &mut out,
                    &apache_avro::from_value::<serde_json::Value>(&r?)?,
                )?;
            }
        }
        Commands::GetMetadata { input } => {
            let reader = avro_reader(input)?;
            for (k, v) in reader.user_metadata() {
                writeln!(out, "{k}={}", String::from_utf8_lossy(v))?;
            }
        }
        Commands::Schema { input } => {
            let reader = avro_reader(input)?;
            serde_json::to_writer_pretty(out, reader.writer_schema())?;
        }
    }
    Ok(())
}
