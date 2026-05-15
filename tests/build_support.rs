// Runs the unit tests defined inside the build-only modules under
// build_support/. Those files are also `include!`d by build.rs to drive
// the const msgid-table generation; this binary exists so `cargo test`
// still exercises the same code paths the build script depends on.
//
// Each include is wrapped in its own `mod` so the two `#[cfg(test)] mod
// tests { ... }` blocks inside the included files don't collide.

mod xml_loader {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/build_support/xml_loader.rs"
    ));
}

mod dialect_parse {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/build_support/dialect_parse.rs"
    ));
}
