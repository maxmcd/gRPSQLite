fn main() {
    // Link against the grpsqlite VFS library
    println!("cargo:rustc-link-search=native=/code/target/debug");
    println!("cargo:rustc-link-lib=static=sqlite_vfs");
    println!("cargo:rustc-link-lib=dylib=pthread");
}