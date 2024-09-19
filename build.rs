use anyhow::Result;
use vergen_gitcl::{BuildBuilder, CargoBuilder, Emitter, GitclBuilder, RustcBuilder};

fn main() -> Result<()> {
    let build = BuildBuilder::all_build()?;
    let cargo = CargoBuilder::all_cargo()?;

    let gitcl = GitclBuilder::default()
        .all()
        // Change the SHA output to the short variant
        .sha(true)
        // Add a `-dirty` flag to the SEMVER output
        .dirty(true)
        .build()?;
    let rustc = RustcBuilder::all_rustc()?;

    Emitter::default()
        .add_instructions(&build)?
        .add_instructions(&cargo)?
        .add_instructions(&gitcl)?
        .add_instructions(&rustc)?
        .emit()
}
