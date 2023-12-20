use std::collections::HashMap;
use assignment_2_solution::{build_sectors_manager, SectorVec};
use ntest::timeout;
use rand::Rng;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
#[timeout(300)]
async fn drive_can_store_data() {
    // given
    let root_drive_dir = tempdir().unwrap();
    let sectors_manager = build_sectors_manager(root_drive_dir.into_path()).await;

    // when
    sectors_manager
        .write(0, &(SectorVec(vec![2; 4096]), 1, 1))
        .await;
    let data = sectors_manager.read_data(0).await;

    // then
    assert_eq!(sectors_manager.read_metadata(0).await, (1, 1));
    assert_eq!(data.0.len(), 4096);
    assert_eq!(data.0, vec![2; 4096])
}

#[tokio::test]
#[timeout(200)]
async fn data_survives_crash() {
    // given
    let root_drive_dir = tempdir().unwrap();
    {
        let sectors_manager = build_sectors_manager(root_drive_dir.path().to_path_buf()).await;
        sectors_manager
            .write(1, &(SectorVec(vec![7; 4096]), 1, 2))
            .await;
    }

    let sectors_manager = build_sectors_manager(root_drive_dir.path().to_path_buf()).await;

    // when
    let (timestamp, write_rank) = sectors_manager.read_metadata(1).await;
    let data = sectors_manager.read_data(1).await;

    // then
    assert_eq!(timestamp, 1);
    assert_eq!(write_rank, 2);
    assert_eq!(data.0, vec![7; 4096]);
}

#[tokio::test]
#[timeout(5000)]
async fn concurrent_operation_on_different_sectors() {
    // given
    let root_drive_dir = tempdir().unwrap();
    let sectors_manager =
        Arc::new(build_sectors_manager(root_drive_dir.path().to_path_buf()).await);
    let tasks: usize = 10;
    let sectors_batch = 16;
    let mut task_handles = vec![];

    // when
    for i in 0..tasks {
        let sectors_manager = sectors_manager.clone();
        task_handles.push(tokio::spawn(async move {
            let sectors_start = sectors_batch * i;
            let sectors_end = sectors_start + sectors_batch;

            for sector_idx in sectors_start..sectors_end {
                let sector_idx = sector_idx as u64;
                let data = SectorVec(
                    (0..4096)
                        .map(|_| rand::thread_rng().gen_range(0..255))
                        .collect(),
                );

                sectors_manager
                    .write(sector_idx, &(data.clone(), 1, 1))
                    .await;
                assert_eq!(sectors_manager.read_metadata(sector_idx).await, (1, 1));
                assert_eq!(sectors_manager.read_data(sector_idx).await, data);
            }
        }));
    }

    // then
    for handle in task_handles {
        assert!(handle.await.is_ok())
    }
}

#[tokio::test]
#[timeout(300)]
async fn drive_can_update_sector() {
    // Test writes to the same sector multiple times, each time checking whether data were saved correctly.
    let root_drive_dir = tempdir().unwrap();
    let sectors_manager = build_sectors_manager(root_drive_dir.into_path()).await;

    let sector_idx = 10;

    let timestamp = 3;
    let write_rank = 7;
    let sector_to_write = SectorVec(vec![5; 4096]);
    sectors_manager.write(sector_idx, &(sector_to_write.clone(), timestamp, write_rank)).await;

    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

    assert_eq!(timestamp_read_from_drive, timestamp);
    assert_eq!(write_rank_read_from_drive, write_rank);
    assert_eq!(sector_read_from_drive.0.len(), sector_to_write.0.len());
    assert_eq!(sector_read_from_drive.0, sector_to_write.0);

    let timestamp = 4;
    let write_rank = 9;
    let sector_to_write = SectorVec(vec![6; 4096]);
    sectors_manager.write(sector_idx, &(sector_to_write.clone(), timestamp, write_rank)).await;

    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

    assert_eq!(timestamp_read_from_drive, timestamp);
    assert_eq!(write_rank_read_from_drive, write_rank);
    assert_eq!(sector_read_from_drive.0.len(), sector_to_write.0.len());
    assert_eq!(sector_read_from_drive.0, sector_to_write.0);

    let timestamp = 13;
    let write_rank = 2;
    let sector_to_write = SectorVec(vec![7; 4096]);
    sectors_manager.write(sector_idx, &(sector_to_write.clone(), timestamp, write_rank)).await;

    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

    assert_eq!(timestamp_read_from_drive, timestamp);
    assert_eq!(write_rank_read_from_drive, write_rank);
    assert_eq!(sector_read_from_drive.0.len(), sector_to_write.0.len());
    assert_eq!(sector_read_from_drive.0, sector_to_write.0);
}

#[tokio::test]
#[timeout(300)]
async fn uncertain_drive_can_handle_repeating_same_write() {
    // Test makes the same write to the same sector three times, each time checking whether data were saved correctly.
    // Marked as uncertain as I'm not sure if we should handle such case.
    let root_drive_dir = tempdir().unwrap();
    let sectors_manager = build_sectors_manager(root_drive_dir.into_path()).await;

    let sector_idx = 10;

    let timestamp = 3;
    let write_rank = 7;
    let sector_to_write = SectorVec(vec![5; 4096]);
    sectors_manager.write(sector_idx, &(sector_to_write.clone(), timestamp, write_rank)).await;

    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

    assert_eq!(timestamp_read_from_drive, timestamp);
    assert_eq!(write_rank_read_from_drive, write_rank);
    assert_eq!(sector_read_from_drive.0.len(), sector_to_write.0.len());
    assert_eq!(sector_read_from_drive.0, sector_to_write.0);

    let timestamp = 3;
    let write_rank = 7;
    let sector_to_write = SectorVec(vec![5; 4096]);
    sectors_manager.write(sector_idx, &(sector_to_write.clone(), timestamp, write_rank)).await;

    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

    assert_eq!(timestamp_read_from_drive, timestamp);
    assert_eq!(write_rank_read_from_drive, write_rank);
    assert_eq!(sector_read_from_drive.0.len(), sector_to_write.0.len());
    assert_eq!(sector_read_from_drive.0, sector_to_write.0);

    let timestamp = 3;
    let write_rank = 7;
    let sector_to_write = SectorVec(vec![5; 4096]);
    sectors_manager.write(sector_idx, &(sector_to_write.clone(), timestamp, write_rank)).await;

    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

    assert_eq!(timestamp_read_from_drive, timestamp);
    assert_eq!(write_rank_read_from_drive, write_rank);
    assert_eq!(sector_read_from_drive.0.len(), sector_to_write.0.len());
    assert_eq!(sector_read_from_drive.0, sector_to_write.0);
}

#[tokio::test]
#[timeout(300)]
async fn uncertain_drive_can_handle_repeating_same_write_but_with_different_sector_data() {
    // Test makes three writes with the same metadata, only different sector data, to the same sector,
    // each time checking whether data were saved correctly.
    // Marked as uncertain as I'm not sure if we should handle such case.
    let root_drive_dir = tempdir().unwrap();
    let sectors_manager = build_sectors_manager(root_drive_dir.into_path()).await;

    let sector_idx = 10;

    let timestamp = 3;
    let write_rank = 7;
    let sector_to_write = SectorVec(vec![5; 4096]);
    sectors_manager.write(sector_idx, &(sector_to_write.clone(), timestamp, write_rank)).await;

    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

    assert_eq!(timestamp_read_from_drive, timestamp);
    assert_eq!(write_rank_read_from_drive, write_rank);
    assert_eq!(sector_read_from_drive.0.len(), sector_to_write.0.len());
    assert_eq!(sector_read_from_drive.0, sector_to_write.0);

    let timestamp = 3;
    let write_rank = 7;
    let sector_to_write = SectorVec(vec![6; 4096]);
    sectors_manager.write(sector_idx, &(sector_to_write.clone(), timestamp, write_rank)).await;

    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

    assert_eq!(timestamp_read_from_drive, timestamp);
    assert_eq!(write_rank_read_from_drive, write_rank);
    assert_eq!(sector_read_from_drive.0.len(), sector_to_write.0.len());
    assert_eq!(sector_read_from_drive.0, sector_to_write.0);

    let timestamp = 3;
    let write_rank = 7;
    let sector_to_write = SectorVec(vec![7; 4096]);
    sectors_manager.write(sector_idx, &(sector_to_write.clone(), timestamp, write_rank)).await;

    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

    assert_eq!(timestamp_read_from_drive, timestamp);
    assert_eq!(write_rank_read_from_drive, write_rank);
    assert_eq!(sector_read_from_drive.0.len(), sector_to_write.0.len());
    assert_eq!(sector_read_from_drive.0, sector_to_write.0);
}

#[tokio::test]
#[timeout(300)]
async fn uncertain_drive_can_handle_not_necessarily_increasing_metadata() {
    // Test makes three writes with metadata, with not necessarily increasing (timestamp, write_rank) metadata.
    // Marked as uncertain as I'm not sure if we should handle such case.
    let root_drive_dir = tempdir().unwrap();
    let sectors_manager = build_sectors_manager(root_drive_dir.into_path()).await;

    let sector_idx = 10;

    let timestamp = 3;
    let write_rank = 7;
    let sector_to_write = SectorVec(vec![5; 4096]);
    sectors_manager.write(sector_idx, &(sector_to_write.clone(), timestamp, write_rank)).await;

    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

    assert_eq!(timestamp_read_from_drive, timestamp);
    assert_eq!(write_rank_read_from_drive, write_rank);
    assert_eq!(sector_read_from_drive.0.len(), sector_to_write.0.len());
    assert_eq!(sector_read_from_drive.0, sector_to_write.0);

    let timestamp = 3;
    let write_rank = 8;
    let sector_to_write = SectorVec(vec![6; 4096]);
    sectors_manager.write(sector_idx, &(sector_to_write.clone(), timestamp, write_rank)).await;

    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

    assert_eq!(timestamp_read_from_drive, timestamp);
    assert_eq!(write_rank_read_from_drive, write_rank);
    assert_eq!(sector_read_from_drive.0.len(), sector_to_write.0.len());
    assert_eq!(sector_read_from_drive.0, sector_to_write.0);

    let timestamp = 2;
    let write_rank = 4;
    let sector_to_write = SectorVec(vec![7; 4096]);
    sectors_manager.write(sector_idx, &(sector_to_write.clone(), timestamp, write_rank)).await;

    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

    assert_eq!(timestamp_read_from_drive, timestamp);
    assert_eq!(write_rank_read_from_drive, write_rank);
    assert_eq!(sector_read_from_drive.0.len(), sector_to_write.0.len());
    assert_eq!(sector_read_from_drive.0, sector_to_write.0);
}

#[tokio::test]
#[timeout(300)]
async fn sectors_that_were_never_written_should_return_zero() {
    let root_drive_dir = tempdir().unwrap();
    let sectors_manager = build_sectors_manager(root_drive_dir.into_path()).await;

    let sector_idx = 10;
    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

    assert_eq!(timestamp_read_from_drive, 0);
    assert_eq!(write_rank_read_from_drive, 0);
    assert_eq!(sector_read_from_drive.0.len(), 4096);
    assert_eq!(sector_read_from_drive.0, vec![0; 4096]);

    let sector_idx = 0;
    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

    assert_eq!(timestamp_read_from_drive, 0);
    assert_eq!(write_rank_read_from_drive, 0);
    assert_eq!(sector_read_from_drive.0.len(), 4096);
    assert_eq!(sector_read_from_drive.0, vec![0; 4096]);
}

#[tokio::test]
#[timeout(300)]
async fn drive_preserves_data_across_restarts() {
    // Does several writes to the drive, then drops it, recreates it, and checks if the data is still there and can
    // be written.
    let root_drive_dir = tempdir().unwrap();

    let mut writes_to_be_done: HashMap<u64, (u64, u8, u8)> = HashMap::new();
    writes_to_be_done.insert(0, (3, 7, 5));
    writes_to_be_done.insert(3, (3, 7, 6));
    writes_to_be_done.insert(6, (1, 5, 7));
    writes_to_be_done.insert(7, (2, 9, 8));

    {
        let sectors_manager = build_sectors_manager(root_drive_dir.path().to_path_buf()).await;

        for (sector_idx, (timestamp, write_rank, data)) in writes_to_be_done.iter() {
            let sector_to_write = SectorVec(vec![*data; 4096]);
            sectors_manager.write(*sector_idx, &(sector_to_write.clone(), *timestamp, *write_rank)).await;
        }

        // I know, unnecessary, but just to be sure...
        drop(sectors_manager);
    }

    let sectors_manager = build_sectors_manager(root_drive_dir.path().to_path_buf()).await;

    for sector_idx in 0u64..10 {
        match writes_to_be_done.get(&sector_idx) {
            Some((timestamp, write_rank, data)) => {
                let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
                let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

                assert_eq!(timestamp_read_from_drive, *timestamp);
                assert_eq!(write_rank_read_from_drive, *write_rank);
                assert_eq!(sector_read_from_drive.0.len(), 4096);
                assert_eq!(sector_read_from_drive.0, vec![*data; 4096]);
            },
            None => {
                let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
                let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

                assert_eq!(timestamp_read_from_drive, 0);
                assert_eq!(write_rank_read_from_drive, 0);
                assert_eq!(sector_read_from_drive.0.len(), 4096);
                assert_eq!(sector_read_from_drive.0, vec![0; 4096]);

            }
        }
    }
}

#[tokio::test]
#[timeout(300)]
async fn uncertain_drive_shall_create_directory_if_it_does_not_exist() {
    // I am not sure if we can assume that the directory given in Configuration will already exist.
    // This is a one-line change in solution, so I think it's best if you just add it.
    // Test works as follows:
    // 1. Writes data to tmpdir/blep/non-existing-directory-1 and dies.
    // 2. Restarts to see if the files are still there and dies again.
    // 3. Starts new drive in tmpdir/blep/non-existing-directory-2 and makes sure it doesn't have sectors written and dies.
    // 4. Writes to tmpdir/blep/non-existing-directory-2 again and checks if the data is there and dies.
    // 5. Restarts drive for tmpdir/blep/non-existing-directory-1 to see if we can still read the correct data.
    let root_drive_dir = tempdir().unwrap();
    let path_1 = root_drive_dir.path().to_path_buf().join("blep/non-existing-directory-1");
    let path_2 = root_drive_dir.path().to_path_buf().join("blep/non-existing-directory-2");

    let mut writes_to_be_done: HashMap<u64, (u64, u8, u8)> = HashMap::new();
    writes_to_be_done.insert(0, (3, 7, 5));
    writes_to_be_done.insert(3, (3, 7, 6));
    writes_to_be_done.insert(6, (1, 5, 7));
    writes_to_be_done.insert(7, (2, 9, 8));

    let writes_to_be_done_backed_up = writes_to_be_done.clone();

    {
        let sectors_manager = build_sectors_manager(path_1.clone()).await;

        for (sector_idx, (timestamp, write_rank, data)) in writes_to_be_done.iter() {
            let sector_to_write = SectorVec(vec![*data; 4096]);
            sectors_manager.write(*sector_idx, &(sector_to_write.clone(), *timestamp, *write_rank)).await;
        }

        // I know, unnecessary, but just to be sure...
        drop(sectors_manager);
    }

    {
        let sectors_manager = build_sectors_manager(path_1.clone()).await;

        for sector_idx in 0u64..10 {
            match writes_to_be_done.get(&sector_idx) {
                Some((timestamp, write_rank, data)) => {
                    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
                    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

                    assert_eq!(timestamp_read_from_drive, *timestamp);
                    assert_eq!(write_rank_read_from_drive, *write_rank);
                    assert_eq!(sector_read_from_drive.0.len(), 4096);
                    assert_eq!(sector_read_from_drive.0, vec![*data; 4096]);
                },
                None => {
                    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
                    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

                    assert_eq!(timestamp_read_from_drive, 0);
                    assert_eq!(write_rank_read_from_drive, 0);
                    assert_eq!(sector_read_from_drive.0.len(), 4096);
                    assert_eq!(sector_read_from_drive.0, vec![0; 4096]);
                }
            }
        }

        // I know, unnecessary, but just to be sure...
        drop(sectors_manager);
    }

    {
        let sectors_manager = build_sectors_manager(path_2.clone()).await;

        for sector_idx in 0u64..10 {
            let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
            let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

            assert_eq!(timestamp_read_from_drive, 0);
            assert_eq!(write_rank_read_from_drive, 0);
            assert_eq!(sector_read_from_drive.0.len(), 4096);
            assert_eq!(sector_read_from_drive.0, vec![0; 4096]);
        }

        // I know, unnecessary, but just to be sure...
        drop(sectors_manager);
    }

    let mut writes_to_be_done: HashMap<u64, (u64, u8, u8)> = HashMap::new();
    writes_to_be_done.insert(0, (3, 7, 5));
    writes_to_be_done.insert(3, (3, 7, 6));
    writes_to_be_done.insert(6, (1, 5, 7));
    writes_to_be_done.insert(7, (2, 9, 8));

    {
        let sectors_manager = build_sectors_manager(path_2.clone()).await;

        for (sector_idx, (timestamp, write_rank, data)) in writes_to_be_done.iter() {
            let sector_to_write = SectorVec(vec![*data; 4096]);
            sectors_manager.write(*sector_idx, &(sector_to_write.clone(), *timestamp, *write_rank)).await;
        }

        // I know, unnecessary, but just to be sure...
        drop(sectors_manager);
    }

    {
        let sectors_manager = build_sectors_manager(path_2.clone()).await;

        for sector_idx in 0u64..10 {
            match writes_to_be_done.get(&sector_idx) {
                Some((timestamp, write_rank, data)) => {
                    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
                    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

                    assert_eq!(timestamp_read_from_drive, *timestamp);
                    assert_eq!(write_rank_read_from_drive, *write_rank);
                    assert_eq!(sector_read_from_drive.0.len(), 4096);
                    assert_eq!(sector_read_from_drive.0, vec![*data; 4096]);
                },
                None => {
                    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
                    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

                    assert_eq!(timestamp_read_from_drive, 0);
                    assert_eq!(write_rank_read_from_drive, 0);
                    assert_eq!(sector_read_from_drive.0.len(), 4096);
                    assert_eq!(sector_read_from_drive.0, vec![0; 4096]);
                }
            }
        }

        // I know, unnecessary, but just to be sure...
        drop(sectors_manager);
    }

    let writes_to_be_done = writes_to_be_done_backed_up;

    {
        let sectors_manager = build_sectors_manager(path_1.clone()).await;

        for sector_idx in 0u64..10 {
            match writes_to_be_done.get(&sector_idx) {
                Some((timestamp, write_rank, data)) => {
                    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
                    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

                    assert_eq!(timestamp_read_from_drive, *timestamp);
                    assert_eq!(write_rank_read_from_drive, *write_rank);
                    assert_eq!(sector_read_from_drive.0.len(), 4096);
                    assert_eq!(sector_read_from_drive.0, vec![*data; 4096]);
                },
                None => {
                    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
                    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

                    assert_eq!(timestamp_read_from_drive, 0);
                    assert_eq!(write_rank_read_from_drive, 0);
                    assert_eq!(sector_read_from_drive.0.len(), 4096);
                    assert_eq!(sector_read_from_drive.0, vec![0; 4096]);
                }
            }
        }

        // I know, unnecessary, but just to be sure...
        drop(sectors_manager);
    }
}

#[tokio::test]
#[timeout(300)]
async fn drive_shall_use_the_provided_directory() {
    // Test checks whether the drive uses the provided directory and not for example current working directory.
    // Test works as follows:
    // 1. Writes data to one directory and kills the drive.
    // 2. Writes data to another directory and kills the drive.
    // 3. Checks if we still can read data from first directory.
    let root_drive_dir_1 = tempdir().unwrap();
    let path_1 = root_drive_dir_1.path().to_path_buf();

    let root_drive_dir_2 = tempdir().unwrap();
    let path_2 = root_drive_dir_2.path().to_path_buf();

    let mut writes_to_be_done: HashMap<u64, (u64, u8, u8)> = HashMap::new();
    writes_to_be_done.insert(0, (3, 7, 5));
    writes_to_be_done.insert(3, (3, 7, 6));
    writes_to_be_done.insert(6, (1, 5, 7));
    writes_to_be_done.insert(7, (2, 9, 8));

    let writes_to_be_done_backed_up = writes_to_be_done.clone();

    {
        let sectors_manager = build_sectors_manager(path_1.clone()).await;

        for (sector_idx, (timestamp, write_rank, data)) in writes_to_be_done.iter() {
            let sector_to_write = SectorVec(vec![*data; 4096]);
            sectors_manager.write(*sector_idx, &(sector_to_write.clone(), *timestamp, *write_rank)).await;
        }

        // I know, unnecessary, but just to be sure...
        drop(sectors_manager);
    }

    let mut writes_to_be_done: HashMap<u64, (u64, u8, u8)> = HashMap::new();
    writes_to_be_done.insert(0, (3, 7, 5));
    writes_to_be_done.insert(3, (3, 7, 6));
    writes_to_be_done.insert(6, (1, 5, 7));
    writes_to_be_done.insert(7, (2, 9, 8));

    {
        let sectors_manager = build_sectors_manager(path_2.clone()).await;

        for (sector_idx, (timestamp, write_rank, data)) in writes_to_be_done.iter() {
            let sector_to_write = SectorVec(vec![*data; 4096]);
            sectors_manager.write(*sector_idx, &(sector_to_write.clone(), *timestamp, *write_rank)).await;
        }

        // I know, unnecessary, but just to be sure...
        drop(sectors_manager);
    }

    let writes_to_be_done = writes_to_be_done_backed_up;

    {
        let sectors_manager = build_sectors_manager(path_1.clone()).await;

        for sector_idx in 0u64..10 {
            match writes_to_be_done.get(&sector_idx) {
                Some((timestamp, write_rank, data)) => {
                    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
                    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

                    assert_eq!(timestamp_read_from_drive, *timestamp);
                    assert_eq!(write_rank_read_from_drive, *write_rank);
                    assert_eq!(sector_read_from_drive.0.len(), 4096);
                    assert_eq!(sector_read_from_drive.0, vec![*data; 4096]);
                },
                None => {
                    let sector_read_from_drive = sectors_manager.read_data(sector_idx).await;
                    let (timestamp_read_from_drive, write_rank_read_from_drive) = sectors_manager.read_metadata(sector_idx).await;

                    assert_eq!(timestamp_read_from_drive, 0);
                    assert_eq!(write_rank_read_from_drive, 0);
                    assert_eq!(sector_read_from_drive.0.len(), 4096);
                    assert_eq!(sector_read_from_drive.0, vec![0; 4096]);
                }
            }
        }

        // I know, unnecessary, but just to be sure...
        drop(sectors_manager);
    }
}