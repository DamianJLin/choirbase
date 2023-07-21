mod choristers;
use crate::choristers::load_choristers;
use crate::choristers::write_choristers;

fn main() {
    let mut c = load_choristers().unwrap();
    write_choristers(&mut c).unwrap();
}
