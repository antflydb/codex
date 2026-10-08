fn main() {
    // Tests re-exec their own binary (for example the fs sandbox helper) with
    // DYLD_LIBRARY_PATH/LD_LIBRARY_PATH stripped, so they need an rpath to
    // find libantfly, as the CLI binaries have.
    println!("cargo:rerun-if-env-changed=ANTFLY_LIB_DIR");
    if let Ok(dir) = std::env::var("ANTFLY_LIB_DIR")
        && matches!(
            std::env::var("CARGO_CFG_TARGET_OS").as_deref(),
            Ok("macos" | "linux")
        )
    {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}");
    }
}
