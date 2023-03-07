mod resp;
use resp::*;

fn main() {
    let source = 
        "$5\r\nhello\r\n";
    let v = source.parse::<Value>();

    println!("Value read: {:?}", v)
}

// Write tests.