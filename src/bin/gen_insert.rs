use clap::Parser;
use std::fs::File;
use std::io::{self, BufRead};
//use std::io::{self, BufRead, LineWriter};
use std::path::Path;
use std::path::PathBuf;

pub const TPCH_TABLES: &[&str] = &[
    "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region",
];

#[derive(clap::Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct Giconfig {
    /// Input path
    #[arg(value_parser, long = "input", default_value = "./dataout/")]
    input_path: PathBuf,

    /// Output path
    #[arg(value_parser, long = "output", default_value = "./valueout")]
    output_path: PathBuf,
}

fn main() {
    let config = Giconfig::parse();

    let _ = check_dirs(config.output_path.clone());

    let x = process_values(
        config.input_path.to_str().unwrap(),
        config.output_path.to_str().unwrap(),
    );

    println!("{:?}", x);
}

fn check_dirs(outdir: PathBuf) -> std::io::Result<()> {
    std::fs::create_dir(outdir)?;
    Ok(())
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

pub fn process_values(input_path: &str, output_path: &str) -> std::io::Result<()> {
    for table in TPCH_TABLES {
        println!("\n{}", table);
        let input_path = format!("{input_path}/{table}.tbl");
        //println!("{:?}", input_path);

        let output_path = format!("{output_path}/{table}.tbl");
        println!("{:?}", output_path);

        //let output_file = File::create(output_path)?;
        //let mut output_file = LineWriter::new(output_file);

        if let Ok(lines) = read_lines(input_path) {
            // Consumes the iterator, returns an (Optional) String
            for line in lines {
                if let Ok(ip) = line {
                    println!("{}", ip);
                    //output_file.write_all(ip.as_bytes())?;
                    //output_file.write_all(b"\n")?;
                }
            }
        }
    }
    Ok(())
}
