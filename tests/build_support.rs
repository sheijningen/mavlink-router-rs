// Re-runs the unit tests inside `build_support/` modules, which are otherwise
// only reached via `include!` from `build.rs`. Each include lives in its own
// `mod` so their `#[cfg(test)] mod tests` blocks don't collide.

#[allow(dead_code)]
mod crc_extra {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/build_support/crc_extra.rs"
    ));
}

#[allow(dead_code)]
mod xml_loader {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/build_support/xml_loader.rs"
    ));
}

#[allow(dead_code)]
mod dialect_parse {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/build_support/dialect_parse.rs"
    ));
}
