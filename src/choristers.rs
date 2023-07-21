use chrono::prelude::*;
use polars::prelude::*;
use std::env;
use std::error::Error;
use std::fs;
use std::io;

struct Chorister {
    fname: String,
    pname: Option<String>,
    lname: String,
    email: String,
    usu_id: String,
    join_date: NaiveDate,
}

pub fn load_choristers() -> Result<DataFrame, Box<dyn Error>> {
    // Read ./data/choristers.csv. If NotFound, create empty dataframe.
    let mut fpath = env::current_exe()?;
    fpath.pop();
    fpath.push("data");
    fpath.push("choristers.csv");

    let choristers = match CsvReader::from_path(fpath) {
        Ok(chr) => chr.finish()?,
        Err(err) => match err {
            PolarsError::Io(ioerr) => match ioerr.kind() {
                io::ErrorKind::NotFound => DataFrame::empty(),
                _ => return Err(Box::new(ioerr)),
            },
            _ => return Err(Box::new(err)),
        },
    };

    Ok(choristers)
}

pub fn write_choristers(choristers: &mut DataFrame) -> Result<(), Box<dyn Error>> {
    let mut fpath = env::current_exe()?;
    fpath.pop();
    fpath.push("data");
    fpath.push("choristers.csv");

    fs::create_dir_all(&fpath.parent().unwrap())?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(fpath)?;

    Ok(CsvWriter::new(&mut file).finish(choristers)?)
}
