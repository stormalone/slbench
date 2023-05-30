use clap::Parser;
use std::fs::File;
use std::io::{self, BufRead, LineWriter, Write};
use std::path::Path;
use std::path::PathBuf;

pub const TPCH_TABLES: &[&str] = &[
    "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region",
];

#[derive(clap::Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct Rtconfig {
    /// The number of rows in a table
    #[arg(long, default_value_t = 30)]
    pub row_capacity: usize,

    /// Start reading the table at this line
    #[arg(long, default_value_t = 0)]
    pub start_row: usize,

    /// Stop reading the table at this line
    #[arg(long, default_value_t = 100)]
    pub end_row: usize,

    /// Input path
    #[arg(value_parser, long = "input", default_value = "./data/")]
    input_path: PathBuf,

    /// Output path
    #[arg(value_parser, long = "output", default_value = "./dataout")]
    output_path: PathBuf,
}

fn main() {
    // File hosts must exist in current path before this produces output

    let config = Rtconfig::parse();
    println!("{}", config.row_capacity);

    let x = convert_tbl(
        config.input_path.to_str().unwrap(),
        config.output_path.to_str().unwrap(),
        config.row_capacity,
    );

    println!("{:?}", x);
}

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

pub fn convert_tbl(
    input_path: &str,
    output_path: &str,
    row_capacity: usize,
) -> std::io::Result<()> {
    for table in TPCH_TABLES {
        let input_path = format!("{input_path}/{table}.tbl");
        println!("{:?}", input_path);

        let output_path = format!("{output_path}/{table}.tbl");
        println!("{:?}", output_path);

        let output_file = File::create(output_path)?;
        let mut output_file = LineWriter::new(output_file);

        let mut counter = 0;

        if let Ok(lines) = read_lines(input_path) {
            // Consumes the iterator, returns an (Optional) String
            for line in lines {
                if counter >= row_capacity {
                    break;
                };
                counter += 1;
                if let Ok(ip) = line {
                    println!("{}", ip);
                    output_file.write_all(ip.as_bytes())?;
                    output_file.write_all(b"\n")?;
                }
            }
        }
    }
    Ok(())
}
