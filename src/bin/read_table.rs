use std::error::Error;

use csv;

/// Reads data from a file into a reader and prints all records.
///
/// # Error
///
/// If an error occurs, the error is returned to `main`.

fn read_from_file(path: &str) -> Result<(), Box<dyn Error>> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'|')
        .has_headers(false)
        .from_path(path)?;

    // `.records` return an iterator of the internal
    // record structure
    for result in reader.records() {
        let record: csv::StringRecord = result?;
        //println!("{:?}", record);
        let record_iter = record.iter();

        let vec: Vec<_> = record_iter.clone().collect();
        println!("{:?}", vec.len());

        for val in record_iter {
            if val != "" {
                println!("{}", val);
            }
        }
    }

    Ok(())
}

fn main() {
    // If an error occurs print error
    if let Err(e) = read_from_file("./data/region.tbl") {
        eprintln!("{}", e);
    }
}
