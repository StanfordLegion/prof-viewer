use std::env;

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    println!("cargo::metadata=SOURCE={}", manifest_dir);

    // Don't rerun, we don't actually depend on anything
    println!("cargo::rerun-if-changed=build.rs");

    println!("cargo::warning=Saving path: {}", manifest_dir)
}
