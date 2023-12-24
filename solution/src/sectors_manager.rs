use crate::domain::*;
use crate::sectors_manager_public::*;
use core::panic;
use core::str;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

const TEMP_PATH_SUFFIX: &str = "tmp";
const CORRECT_FILE_SIZE: u64 = 4096;

pub struct Manager {
    // TODO: LRU cache for file descriptors
    metadata: RwLock<HashMap<SectorIdx, (u64, u8)>>,
    path: PathBuf,
    dir_fd: File,
}

impl Manager {
    pub async fn new_arc(path: PathBuf) -> Arc<dyn SectorsManager> {
        log::debug!("new manager in creation for dir {:?}", path);

        // Create the directory if not exists.
        create_dir_all(path.clone()).await.unwrap();

        let manager = Arc::new(Manager {
            metadata: RwLock::new(HashMap::new()),
            path: path.clone(),
            dir_fd: File::open(path.clone()).await.unwrap(),
        });
        let mut metadata = manager.metadata.write().await;

        // Read all metadata from path.
        // TODO: is this unwrap legit?
        let mut dir = read_dir(path.clone()).await.unwrap();
        while let Some(file) = dir.next_entry().await.unwrap() {
            log::debug!("handling file {:?}", file.file_name());

            if let Ok(file_name) = file.file_name().into_string() {
                // If .tmp file, make sure that file size is correct.
                // This protects us in case writing to file failed, but file exists.
                if file_name.ends_with(TEMP_PATH_SUFFIX)
                    && file.metadata().await.unwrap().len() != CORRECT_FILE_SIZE
                {
                    continue;
                }

                // Algorithm for writing:
                // 1. Write to .tmp file
                // 2. Remove old file
                // 3. Rename new file
                // Because of that, if .tmp file has priority when receiving the data
                let (idx, ts, wr) = Self::parse_file_name(&file_name);
                if let Some(&(other_ts, other_wr)) = metadata.get(&idx) {
                    log::debug!("two files found for sector {}", idx);
                    log::debug!("in map: {}_{}_{}", idx, other_ts, other_wr);
                    log::debug!("want to add: {}_{}_{}", idx, ts, wr);

                    // If we already have metadata for this sector,
                    // then either we want to add tmp (then override and delete old file)
                    // or we've already added it, then ignore and continue.
                    if file_name.ends_with(TEMP_PATH_SUFFIX) {
                        // Overwrite old data.
                        metadata.insert(idx, (ts, wr));

                        // Remove the file with old data.
                        remove_file(manager.metadata_to_filename(idx, other_ts, other_wr, false))
                            .await
                            .unwrap();
                        manager.sync_dir().await;

                        // Rename .tmp file.
                        let old_path = manager.metadata_to_filename(idx, ts, wr, true);
                        let new_path = manager.metadata_to_filename(idx, ts, wr, false);
                        rename(old_path, new_path).await.unwrap();
                        manager.sync_dir().await;

                        log::debug!("tmp file has overwriten the old file");
                    }
                } else {
                    metadata.insert(idx, (ts, wr));
                }
            }
        }

        // Sync all changes AFTER the loop, in case of errors crash and retry.
        manager.sync_dir().await;

        log::debug!("new manager created - happy managing!");

        std::mem::drop(metadata);
        manager
    }

    // Panics if unexpected file name found.
    fn parse_file_name(name: &str) -> (SectorIdx, u64, u8) {
        let mut iter = name.split('_');
        (
            iter.next().unwrap().parse().unwrap(),
            iter.next().unwrap().parse().unwrap(),
            iter.next().unwrap().parse().unwrap(),
        )
    }

    fn metadata_to_filename(
        &self,
        idx: SectorIdx,
        timestamp: u64,
        rank: u8,
        is_temp: bool,
    ) -> PathBuf {
        let mut p = self.path.clone();
        if is_temp {
            p.push(format!(
                "{}_{}_{}.{}",
                idx, timestamp, rank, TEMP_PATH_SUFFIX
            ));
        } else {
            p.push(format!("{}_{}_{}", idx, timestamp, rank))
        }

        p
    }

    async fn get_filename(&self, idx: SectorIdx) -> PathBuf {
        let metadata = self.read_metadata(idx).await;
        self.metadata_to_filename(idx, metadata.0, metadata.1, false)
    }

    fn get_empty_sector(&self) -> SectorVec {
        SectorVec(vec![0; SECTOR_SIZE])
    }

    async fn sync_dir(&self) {
        self.dir_fd.sync_all().await.unwrap();
    }
}

#[async_trait::async_trait]
impl SectorsManager for Manager {
    /// Returns 4096 bytes of sector data by index.
    async fn read_data(&self, idx: SectorIdx) -> SectorVec {
        log::debug!("reading data for sector {}", idx);

        let filename = self.get_filename(idx).await;
        let file = File::open(self.path.join(filename.clone())).await;

        match file {
            Ok(mut f) => {
                let mut buf = vec![0; SECTOR_SIZE];
                // TODO: is this unwrap legit?
                f.read_exact(&mut buf).await.unwrap();

                log::debug!("content of file {:?}: {:?}", filename, buf);

                SectorVec(buf)
            }
            Err(e) => match e.kind() {
                ErrorKind::NotFound => {
                    log::debug!(
                        "file {:?} for sector {} not found, returning zeros",
                        filename,
                        idx
                    );
                    self.get_empty_sector()
                }
                _ => {
                    log::warn!(
                        "error while opening a file {:?} for sector {}: {}, panic",
                        filename,
                        idx,
                        e.to_string()
                    );
                    panic!()
                }
            },
        }
    }

    /// Returns timestamp and write rank of the process which has saved this data.
    /// Timestamps and ranks are relevant for atomic register algorithm, and are described
    /// there.
    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
        let lock = self.metadata.read().await;

        match lock.get(&idx) {
            Some(&y) => y,
            None => {
                log::warn!("metadata for sector {} not found", idx);
                (0, 0)
            }
        }
    }

    /// Writes a new data, along with timestamp and write rank to some sector.
    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
        let old_filename = self.get_filename(idx).await;
        let temp_filename = self.metadata_to_filename(idx, sector.1, sector.2, true);

        // TODO: is this unwrap legit?
        let mut file = File::create(temp_filename.clone()).await.unwrap();

        // No need to check sector size, deserializing should have already verified that.
        // TODO: are these unwraps legit?
        let _ = file.write_all(&sector.0 .0).await.unwrap();
        file.sync_data().await.unwrap();

        // Remove old file.
        match remove_file(&old_filename).await {
            // If file didn't exist, ignore the error.
            // Else panic.
            Ok(_) => self.sync_dir().await,
            Err(e) => match e.kind() {
                ErrorKind::NotFound => (),
                _ => panic!("removing old file failed"),
            },
        }

        // Rename new file from .tmp to normal name.
        rename(
            temp_filename,
            self.metadata_to_filename(idx, sector.1, sector.2, false),
        )
        .await
        .unwrap();
        self.sync_dir().await;

        self.metadata
            .write()
            .await
            .insert(idx, (sector.1, sector.2));
    }
}
