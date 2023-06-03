use clap::Parser;
use csv;
use std::error::Error;
use std::fs::File;
use std::io::{LineWriter, Write};
use std::path::PathBuf;

use arrow_schema::DataType;
use datafusion_benchmarks::tpch::{get_tpch_table_schema, TPCH_TABLES};

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

pub fn process_values(input_path: &str, output_path: &str) -> std::io::Result<()> {
    for table in TPCH_TABLES {
        println!("\n{}", table);
        let input_path = format!("{input_path}/{table}.tbl");
        //println!("{:?}", input_path);

        let output_path = format!("{output_path}");
        //println!("{:?}", output_path);

        let _ = get_vec_from_file(table, input_path.as_str(), output_path.as_str());
    }
    Ok(())
}

fn get_vec_from_file(
    table_name: &str,
    input_path: &str,
    output_path: &str,
) -> Result<(), Box<dyn Error>> {
    let schema = get_tpch_table_schema(table_name);
    println!("{:?}", schema);
    //println!("{:?}", schema.fields[0].data_type());

    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'|')
        .has_headers(false)
        .from_path(input_path)?;

    let output_path = format!("{output_path}/{table_name}.tbl");
    println!("{:?}", output_path);

    let output_file = File::create(output_path.clone())?;
    let mut output_file = LineWriter::new(output_file);

    // `.records` return an iterator of the internal
    // record structure
    for result in reader.records() {
        let record: csv::StringRecord = result?;
        //println!("{:?}", record);
        let record_iter = record.iter();
        //let vec1: Vec<_> = record_iter.clone().collect();
        let mut vec = Vec::new();

        for (i, val) in record_iter.enumerate() {
            if val != "" {
                let datatype = schema.fields[i].data_type();
                println!("{:?}", datatype);
                let answer = should_val_be_a_string(datatype.clone());
                // if yes then add the tickmarks around the string, date
                if answer {
                    vec.push("'");
                    vec.push(val);
                    vec.push("'");
                // if no then we do not need tick marks around the value
                } else {
                    vec.push(val);
                }
                vec.push(" ");
            }
        }

        output_file.write_all(vec.concat().as_bytes())?;
        output_file.write_all(b"\n")?;

        // this prints each item of the vector on a separate line
        /*
        for i in vec.iter() {
            println!("{}", i);
        }
        */
        // this prints double quotes which we do not want
        // println!("\n{:?}", vec);
    }
    println!("\n{:?}", output_path);
    Ok(())
}

fn should_val_be_a_string(datatype: DataType) -> bool {
    if datatype.equals_datatype(&DataType::Utf8) {
        return true;
    }
    if datatype.equals_datatype(&DataType::Date32) {
        return true;
    }
    return false;
}
