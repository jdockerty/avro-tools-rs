use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
struct Cli {
    #[clap(subcommand)]
    commands: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    ToJSON { input: PathBuf },
    Schema { input: PathBuf },
    GetMetadata { input: PathBuf },
}

fn main() {
    let cli = Cli::parse();

    match cli.commands {
        Commands::ToJSON { input } => {
            // read file
            // output values to json
        }
        Commands::GetMetadata { input } => {
            let file = std::fs::File::open(input).unwrap();
            let avro_reader = apache_avro::Reader::new(file).unwrap();
            for (k, v) in avro_reader.user_metadata() {
                println!("{k}={}", String::from_utf8_lossy(v));
            }
        }
        Commands::Schema { input } => {
            let file = std::fs::File::open(input).unwrap();
            let avro_reader = apache_avro::Reader::new(file).unwrap();

            serde_json::to_writer_pretty(std::io::stdout(), avro_reader.writer_schema()).unwrap();
        }
    }
}
