use chrono::prelude::*;
use polars::prelude::*;
use std::env;
use std::error::Error;
use std::fs;
use std::io;

pub fn load_choristers() -> Result<DataFrame, Box<dyn Error>> {
    // Read ./data/choristers.csv. If NotFound, create empty dataframe.
    let mut fpath = env::current_exe()?;
    fpath.pop();
    fpath.push("data");
    fpath.push("choristers.csv");

    let schema_ref = Arc::new(Schema::from_iter(
        vec![
            Field::new("id", DataType::UInt32),
            Field::new("fname", DataType::Utf8),
            Field::new("pname", DataType::Utf8),
            Field::new("lname", DataType::Utf8),
            Field::new("email", DataType::Utf8),
            Field::new("usu_id", DataType::UInt32),
            Field::new("join_date", DataType::Date),
        ]
        .into_iter(),
    ));

    let choristers = match CsvReader::from_path(fpath) {
        Ok(chr) => chr.with_dtypes(Some(schema_ref)).finish()?,
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

/// A documentation comment.
/// Signature (fname, pname, lname, email, usu_id).
pub fn add_chorister(
    fname: String,
    pname: Option<String>,
    lname: String,
    email: String,
    usu_id: u32,
) -> Result<(), Box<dyn Error>> {
    let mut choristers: DataFrame = load_choristers()?;

    // Find the id of the tail chorister, and increment by 1.
    // choristers["id"] panics when empty dataframe.
    let flag_id = choristers // Conduct index check.
        .get_column_names_owned()
        .iter()
        .any(|i| i == "id");
    let new_id: u32 = match flag_id {
        true => match /* panics if index missing */ choristers["id"]
            .tail(Some(1))
            .get(0) {
            Ok(val) => match val {
                AnyValue::UInt32(x) => x + 1,
                _ => {
                    return Err(Box::new(polars_err!(
                        ComputeError:
                        "id failed be of type AnyValue::UInt32"
                    )))
                }
            },
            Err(err) => {
                println!("{}", err);
                1
            }
        },
        false => 1,
    };

    let new = df!(
        "id" => &[new_id],
        "fname" => &[fname],
        "pname" => &[pname],
        "lname" => &[lname],
        "email" => &[email],
        "usu_id" => &[usu_id],
        "join_date" => &[Utc::now().date_naive()],
    )?;
    println!("{}", new);

    choristers.vstack_mut(&new)?;
    write_choristers(&mut choristers)?;
    Ok(())
}
