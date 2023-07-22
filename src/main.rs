#![allow(dead_code, unused_variables)]

mod choristers;
use crate::choristers::add_chorister;
use crate::choristers::load_choristers;

fn main() {
    add_chorister(
        "Damian".to_owned(),
        None,
        "Lin".to_owned(),
        "d@lin.com".to_owned(),
        1910481u32,
    )
    .expect("error");
    println!("{}", load_choristers().expect("inside err"))
}
