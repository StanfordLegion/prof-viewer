use std::env;
use std::process::Command;
use std::path::Path;

fn main() {
    let out_dir = env::var_os("OUT_DIR").unwrap();
    let dist_dir = Path::new(&out_dir).join("dist");

    Command::new("trunk").args(["build", "--release", "--dist"]).arg(dist_dir).spawn().expect("Trunk failed");
}
