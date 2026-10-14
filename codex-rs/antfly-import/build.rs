fn main() {
    // Let the importer find libantfly where it was linked from, so it runs
    // without DYLD_LIBRARY_PATH/LD_LIBRARY_PATH (same as the codex CLI).
    println!("cargo:rerun-if-env-changed=ANTFLY_LIB_DIR");
    if let Ok(dir) = std::env::var("ANTFLY_LIB_DIR")
        && matches!(
            std::env::var("CARGO_CFG_TARGET_OS").as_deref(),
            Ok("macos" | "linux")
        )
    {
        println!("cargo:rustc-link-arg-bins=-Wl,-rpath,{dir}");
    }
}
