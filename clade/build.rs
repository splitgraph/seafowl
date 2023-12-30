use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("clade_descriptor.bin"))
        .build_server(true)
        .build_client(true)
        .compile(&["proto/catalog.proto", "proto/schema.proto"], &["proto"])?;

    Ok(())
}
