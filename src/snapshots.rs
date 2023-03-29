use std::fs;
use std::path;
use std::io;

use serde::Serialize;

#[derive(Clone)]
pub struct SnapshotFile {
    path: path::PathBuf,
    index: usize,
}

impl SnapshotFile {
    fn new(path: path::PathBuf, index: usize) -> Self {
        Self { path, index }
    }

    pub fn put<S: Serialize>(&self, data: &S) -> Result<(), io::Error> {
        let open = fs::File::options().write(true).create_new(true).open(self.path.as_path());
        let writer = io::BufWriter::new(open?);
        bincode::serialize_into(writer, data).map_err(|e|
            io::Error::new(io::ErrorKind::Other, e.to_string())
        )
    }
}

pub trait Snapshots {
    fn save_snapshot(&self) -> Result<(), io::Error>;
    fn load_snapshot(&mut self) -> Result<(), io::Error>;
}

fn mk_snapshot_file(index: usize) -> SnapshotFile {
    let path_name = format!("./data/snapshot-{index}.data");
    let path = path::Path::new(&path_name);
    SnapshotFile::new(path.to_path_buf(), index)
}

pub fn allocate_snapshot_file() -> Result<SnapshotFile, io::Error> {
    let mut files = vec![];
    find_all(&path::Path::new("./data"), &mut files)?;
    Ok(files.iter().max_by_key(|f| f.index)
            .map(|f| f.clone())
            .unwrap_or_else(|| mk_snapshot_file(0))
    )
}

fn find_all(in_path: &path::Path, snapshots: &mut Vec<SnapshotFile>) -> Result<(), io::Error> {
    fn mk_snapshot_file(pattern: &regex::Regex, path: &path::PathBuf) -> Option<SnapshotFile> {
        let name  = path.file_name()?.to_str()?;
        let index = pattern.captures(name)?.get(1)?.as_str().parse().ok()?;
        Some(SnapshotFile::new(path.clone(), index))
    }

    let pattern = regex::Regex::new("snapshot-(\\d+)").map_err(|e|
        io::Error::new(io::ErrorKind::Other, e.to_string())
    )?;

    for dir in fs::read_dir(in_path)? {
        match mk_snapshot_file(&pattern, &dir?.path()) {
            Some(snapshot) => snapshots.push(snapshot),
            None           => continue,
        }
    }

    Ok(())
}