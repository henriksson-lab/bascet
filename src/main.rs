use clap::{Parser, Subcommand};
use clio::Input;

#[derive(Parser)]
#[command(version, about)]
struct CLI {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    // Generate markers from reference CRAM file
    Markers {
        #[arg(value_parser)]
        path_in: clio::Input,
        #[arg(value_parser, default_value = "markers.list")]
        path_out: clio::Output,
    },
    // Split reference CRAM file
    Split {
        #[arg(value_parser)]
        path_in: clio::Input,
        #[arg(value_parser, default_value = "")]
        path_out: clio::Output,
    },
    // Generate query embeddings using markers, index and splits
    Query {
        #[arg(value_parser, default_value = "markers.list")]
        path_markers: clio::Input,
        #[arg(value_parser, default_value = ".rindex")]
        path_rindex: Input,
        #[arg(value_parser, default_value = ".rsplit")]
        path_rsplit: Input,
        #[arg(value_parser, default_value = "out.mm")]
        path_out: clio::Output,
    },
}

fn main() {
    let cli = CLI::parse();

    match cli.command {
        Commands::Markers { path_in, path_out } => {
            println!(
                "Processing markers from reference CRAM: {}",
                path_in.path().display()
            );
            // TODO: Implement markers generation
        }
        Commands::Split { path_in, path_out } => {
            println!("Splitting reference CRAM: {}", path_in.path().display());
            // TODO: Implement split functionality
        }
        Commands::Query {
            path_markers,
            path_rindex,
            path_rsplit,
            path_out,
        } => {
            println!("Querying with:");
            println!("  rindex: {}", path_rindex.path().display());
            println!("  rsplit: {}", path_rsplit.path().display());
            // TODO: Implement query functionality
        }
    }
}
