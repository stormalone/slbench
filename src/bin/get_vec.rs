use csv;
use std::error::Error;

fn get_vec_from_file(path: &str) -> Result<(), Box<dyn Error>> {
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
        //let vec1: Vec<_> = record_iter.clone().collect();
        let mut vec = Vec::new();

        for val in record_iter {
            if val != "" {
                vec.push(val);
            }
        }
        println!("\n{:?}", vec);
    }

    Ok(())
}

fn main() {
    // If an error occurs print error
    if let Err(e) = get_vec_from_file("./data/region.tbl") {
        eprintln!("{}", e);
    }
}
