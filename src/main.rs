use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
struct App {
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
    println!("Hello, world!");

    let cli = App::parse();

    match cli.commands {
        Commands::ToJSON { input } => {
            // read file
            // output values to json
        }
        Commands::GetMetadata { input } => {
            let file = std::fs::File::open(input).unwrap();

            let avro_reader = apache_avro::Reader::new(file).unwrap();

            for (k, v) in avro_reader.user_metadata() {}
        }
        Commands::Schema { input } => {}
    }
}
