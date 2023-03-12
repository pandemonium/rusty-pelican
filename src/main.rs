mod resp;
mod command;
mod core;
use resp::*;

fn main() {
    let source = "*3\r\n$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n";
    let v = source.parse::<Value>();

    println!("Value read: {:?}", v)
}