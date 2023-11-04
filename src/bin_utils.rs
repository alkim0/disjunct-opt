use csv::Writer;
use itertools::Itertools;
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

pub fn write_records(output: &Path, records: Vec<impl Serialize>) -> csv::Result<()> {
    if let Some(parent) = output.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)
                .expect(format!("Could not create dir {}", parent.display()).as_str());
        }
    }

    let mut writer = Writer::from_path(output)?;
    for record in records {
        writer.serialize(record)?;
    }
    writer.flush()?;
    Ok(())
}

pub fn drop_caches() {
    let drop_caches_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("drop_caches")
        .join("drop_caches");
    let drop_caches_cmd = if drop_caches_path.exists() {
        drop_caches_path
            .canonicalize()
            .unwrap()
            .as_os_str()
            .to_str()
            .unwrap()
            .to_string()
    } else {
        "drop_caches".to_string()
    };
    let status = Command::new("sh").arg("-c").arg(drop_caches_cmd).status();
    if status.map(|status| !status.success()).unwrap_or(true) {
        eprintln!("Failed to drop caches. Resulting times might be compromised. Install a drop_caches command to fix. See the README for more.");
    }
}

pub fn default_stats_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("stats")
}

pub fn default_output_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("output")
}

pub fn parse_comma_range_num_list(
    s: &str,
) -> Result<Vec<usize>, <usize as std::str::FromStr>::Err> {
    use either::Either;
    Ok(s.split(",")
        .map(|val| {
            if val.contains("-") {
                let (beg, end) = val.split_once("-").unwrap();
                let beg: usize = beg.parse()?;
                let end: usize = end.parse()?;
                Ok(Either::Left(beg..(end + 1)))
            } else {
                let num: usize = val.parse()?;
                Ok(Either::Right(std::iter::once(num)))
            }
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .sorted_unstable()
        .collect())
}

pub fn parse_comma_string_list(s: &str) -> Result<Vec<String>, &'static str> {
    Ok(s.split(",").map(|s| s.to_string()).collect())
}
