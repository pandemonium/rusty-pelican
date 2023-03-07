mod resp;
use resp::*;

fn main() {
    let v = Value::read("-ERR World has come to an end.");

    println!("Value read: {:?}", v)
}

// Write tests.