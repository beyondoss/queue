use anyhow::{bail, Result};

fn main() -> Result<()> {
    let task = std::env::args().nth(1);
    match task.as_deref() {
        Some("sqlx-prepare") => sqlx_prepare(),
        Some(t) => bail!("unknown xtask: {t}"),
        None => {
            eprintln!("Usage: cargo xtask <task>");
            eprintln!("Tasks: sqlx-prepare");
            Ok(())
        }
    }
}

fn sqlx_prepare() -> Result<()> {
    let status = std::process::Command::new("cargo")
        .args(["sqlx", "prepare", "--workspace"])
        .status()?;
    if !status.success() {
        bail!("sqlx prepare failed");
    }
    Ok(())
}
