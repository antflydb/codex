fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-link-arg=-ObjC");
    }
    // Let the binaries find libantfly where it was linked from, so they run
    // without DYLD_LIBRARY_PATH/LD_LIBRARY_PATH. Packaged builds install the
    // library on the loader path instead.
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
