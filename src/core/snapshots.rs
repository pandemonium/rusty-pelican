use std::fs;
use std::path;
use std::io;

use serde::{Serialize, de::DeserializeOwned};

#[derive(Clone)]
pub struct SnapshotFile {
    path: path::PathBuf,
    index: usize,
}

impl SnapshotFile {
    fn new(path: &path::Path, index: usize) -> Self {
        Self { path: path.to_path_buf(), index }
    }

    pub fn put<S: Serialize>(&self, data: &S) -> Result<(), io::Error> {
        let file = fs::File::options().write(true).create_new(true).open(self.path.as_path());
        let writer = io::BufWriter::new(file?);
        bincode::serialize_into(writer, data).map_err(|e|
            io::Error::new(io::ErrorKind::Other, e.to_string())
        )
    }

    pub fn get<D>(&self) -> Result<D, io::Error>
    where D: DeserializeOwned {   /* Wtf. */
        let file = fs::File::options().read(true).open(self.path.as_path());
        let reader = io::BufReader::new(file?);
        bincode::deserialize_from(reader).map_err(|e|
            io::Error::new(io::ErrorKind::Other, e.to_string())
        )
    }
}

pub trait Snapshots {
    fn save_snapshot(&self) -> Result<(), io::Error>;
    fn restore_most_recent_snapshot(&mut self) -> Result<(), io::Error>;
}

fn mk_snapshot_file(index: usize) -> SnapshotFile {
    let path_name = format!("./data/snapshot-{index}.data");
    let path = path::Path::new(&path_name);
    SnapshotFile::new(path, index)
}

pub fn most_recent() -> Result<Option<SnapshotFile>, io::Error> {
    let mut files = vec![];
    find_all(path::Path::new("./data"), &mut files)?;
    Ok(files.iter().max_by_key(|f| f.index).cloned())
}

pub fn allocate_new() -> Result<SnapshotFile, io::Error> {
    Ok(most_recent()?.map_or_else(
        ||  mk_snapshot_file(0), 
        |f| mk_snapshot_file(f.index + 1))
    )
}

fn find_all(in_path: &path::Path, snapshots: &mut Vec<SnapshotFile>) -> Result<(), io::Error> {
    fn mk_snapshot_file(pattern: &regex::Regex, path: &path::Path) -> Option<SnapshotFile> {
        let name  = path.file_name()?.to_str()?;
        let index = pattern.captures(name)?.get(1)?.as_str().parse().ok()?;
        Some(SnapshotFile::new(path, index))
    }

    let pattern = regex::Regex::new("snapshot-(\\d+)").map_err(|e|
        io::Error::new(io::ErrorKind::Other, e.to_string())
    )?;

    for dir in fs::read_dir(in_path)? {
        if let Some(snapshot) = mk_snapshot_file(&pattern, &dir?.path()) {
            snapshots.push(snapshot);
        }
    }

    Ok(())
}