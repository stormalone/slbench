use clap::Parser;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::path::PathBuf;

pub const TPCH_TABLES: &[&str] = &[
    "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region",
];

#[derive(clap::Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct Rtconfig {
    /// The number of rows in a table
    #[arg(long, default_value_t = 20)]
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

/*
#[derive(Deserialize, clap::Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct Rtconfig {
    /// IP and port to listen on
    #[arg(long, default_value = "")]
    #[serde(default = "listen_endpoint")]
    pub listen_endpoint: String,

    /// Hostname and port at which to find the leader
    #[arg(long, default_value = "")]
    #[serde(default = "advertise_endpoint")]
    pub leader_endpoint: String,

    /// Hostname and port at which other nodes can find this node
    #[arg(long, default_value = "")]
    #[serde(default = "advertise_endpoint")]
    pub advertise_endpoint: String,

    /// The path at which to store the Raft / write-ahead log
    #[arg(long, default_value = "")]
    #[serde(default = "wal_path")]
    pub wal_path: String,

    /// The URL at which to store deltalake snapshots
    #[arg(long, default_value = "")]
    #[serde(default = "snapshot_url")]
    pub snapshot_url: String,

    /// The minimum number of nodes in a Raft group
    #[arg(long, default_value_t = 3)]
    #[serde(default = "replication_factor")]
    pub replication_factor: usize,

    /// The number of bytes to allocate for a block. It will split if its data exceeds this threshold
    #[arg(long, default_value_t = 4_000_000)]
    #[serde(default = "block_size")]
    pub block_size: usize,

    /// The maximum number of rows in a block before it splits
    #[arg(long, default_value_t = 4000)]
    #[serde(default = "row_capacity")]
    pub row_capacity: usize,

    /// If set to true, an fsync syscall will be performed for every new Raft message
    #[arg(long, default_value_t = true)]
    #[serde(default = "fsync")]
    pub fsync: bool,

    /// If set to true, an fsync syscall will be performed for every new Raft message
    #[arg(long, default_value_t = true)]
    #[serde(default = "persist")]
    pub persist: bool,

    /// If set to true, use SxtDb Tablets instead of ArrowTables
    #[arg(long, default_value_t = false)]
    #[serde(default = "sxt_db")]
    pub sxt_db: bool,
}
*/

fn main() {
    // File hosts must exist in current path before this produces output

    let config = Rtconfig::parse();
    println!("{}", config.row_capacity);

    let mut counter = 0;

    convert_tbl(
        config.input_path.to_str().unwrap(),
        config.output_path.to_str().unwrap(),
        config.row_capacity,
    );

    if let Ok(lines) = read_lines("./data/lineitem.tbl") {
        // Consumes the iterator, returns an (Optional) String
        for line in lines {
            if counter >= config.row_capacity {
                break;
            };
            counter += 1;
            if let Ok(ip) = line {
                println!("{}", ip);
            }
        }
    }
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

/// Conver tbl (csv) file to parquet
pub fn convert_tbl(_input_path: &str, output_path: &str, _row_capacity: usize) {
    let output_root_path = Path::new(output_path);
    println!("{:?}", output_root_path);

    for table in TPCH_TABLES {
        println!("{:?}", table);
    }
}

/*
/// Conver tbl (csv) file to parquet
pub async fn convert_tbl(
    input_path: &str,
    output_path: &str,
    file_format: &str,
    partitions: usize,
    batch_size: usize,
    compression: Compression,
) -> Result<()> {
    let output_root_path = Path::new(output_path);
    for table in TPCH_TABLES {
        let start = Instant::now();
        let schema = get_tbl_tpch_table_schema(table);

        let input_path = format!("{input_path}/{table}.tbl");
        let options = CsvReadOptions::new()
            .schema(&schema)
            .has_header(false)
            .delimiter(b'|')
            .file_extension(".tbl");

        let config = SessionConfig::new().with_batch_size(batch_size);
        let ctx = SessionContext::with_config(config);

        // build plan to read the TBL file
        let mut csv = ctx.read_csv(&input_path, options).await?;

        // Select all apart from the padding column
        let selection = csv
            .schema()
            .fields()
            .iter()
            .take(schema.fields.len() - 1)
            .map(|d| Expr::Column(d.qualified_column()))
            .collect();

        csv = csv.select(selection)?;
        // optionally, repartition the file
        if partitions > 1 {
            csv = csv.repartition(Partitioning::RoundRobinBatch(partitions))?
        }

        // create the physical plan
        let csv = csv.create_physical_plan().await?;

        let output_path = output_root_path.join(table);
        let output_path = output_path.to_str().unwrap().to_owned();

        println!(
            "Converting '{}' to {} files in directory '{}'",
            &input_path, &file_format, &output_path
        );
        match file_format {
            "csv" => ctx.write_csv(csv, output_path).await?,
            "parquet" => {
                let props = WriterProperties::builder()
                    .set_compression(compression)
                    .build();
                ctx.write_parquet(csv, output_path, Some(props)).await?
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Invalid output format: {other}"
                )));
            }
        }
        println!("Conversion completed in {} ms", start.elapsed().as_millis());
    }

    Ok(())
}
*/

// This takes in a set of big files and reduces them down to a much smaller number of lines.
// Input parameters include the maximum number of lines a file can have
// Eventually this function will also take in an optional start line and end line
// So you can save off the smaller subset of lines from the bigger file
// All of these parameters are optional and one can go with the defaults

/*
pub fn reduce_tables(path: impl AsRef<str>) -> Result<()> {
    let path = path.as_ref();
    // create directory to contain the Parquet files (one per partition)
    let fs_path = std::path::Path::new(path);
    if let Err(e) = fs::create_dir(fs_path) {
        return Err(DataFusionError::Execution(format!(
            "Could not create directory {path}: {e:?}"
        )));
    }

    let filename = format!("part-{i}.parquet");
    let path = fs_path.join(filename);
    let file = fs::File::create(path)?;

    Ok(())
}
*/
