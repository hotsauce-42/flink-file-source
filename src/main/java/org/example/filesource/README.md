# Modification time based file source

### Usage
```java
Path dirToWatch = Path.fromLocalFile(new File("src/main/resources/testDirectory"));

final OwnFileSource<List<String>> source =
        OwnFileSource.forRecordStreamFormat(new ListStringInputFormat(), dirToWatch)
            .monitorContinuously(Duration.ofSeconds(1))
            .build();

DataStream<List<String>> stream =
    env.fromSource(source, WatermarkStrategy.noWatermarks(), "source");
```

### Background 
For unbounded file sources, the `SplitEnumerator` currently keeps track of all already processed file paths (`HashSet<Path> alreadyProcessedPaths`) to determine which files have been read.
This approach introduces two problems:   
1. The file source does not re-read a file if its content has changed.
2. The state containing all processed file paths can grow very large over time.  

### Adaptions
To address these issues, we implemented an [OwnContinuousFileSplitEnumerator](./OwnContinuousFileSplitEnumerator.java) that no longer stores `alreadyProcessedPaths`. Instead, it maintains a `long alreadyProcessedModificationTime`,  which records up to which modification timestamp files have been processed.

Because the `ContinuousSplitEnumerator` is tightly integrated into the `AbstractFileSource`, that class also required modification. 
The [OwnAbstractFileSource](OwnAbstractFileSource.java) is largely identical to the original `AbstractFileSource`, except that it now uses the `OwnContinuousSplitEnumerator`. 
Since the `AbstractFileSource` is itself directly invoked by the `FileSource`, the same approach was applied to create the [OwnFileSource](OwnFileSource.java).

To correctly capture the state of the `OwnContinuousSplitEnumerator` in checkpoints and savepoints, both the `PendingSplitsCheckpoint` and the `PendingSplitsCheckpointSerializer` were adapted ([OwnPendingSplitsCheckpoint](OwnPendingSplitsCheckpoint.java) and [OwnPendingSplitsCheckpointSerializer](OwnPendingSplitsCheckpointSerializer.java)). The new checkpoint now stores the `alreadyProcessedModificationTime` instead of the `alreadyProcessedPaths`.

### Benefits
The `OwnFileSource` has a smaller state that does not grow indefinitely as the application runs and now re-reads files if they have been modified.

### Important notes
Since it does not make sense to adapt all classes for `modification time` in the `bounded` case - where the feature is not used - the `OwnAbstractFileSource` throws an `UnsupportedOperationException` in bounded mode. If a bounded file source is required, the standard Flink `FileSource` should be used instead.

### List of adapted flink classes
- `org.apache.flink.connector.file.src.impl.ContinuousFileSplitEnumerator`
- `org.apache.flink.connector.file.src.AbstractFileSource`
- `org.apache.flink.connector.file.src.FileSource`
- `org.apache.flink.connector.file.src.PendingSplitsCheckpoint`
- `org.apache.flink.connector.file.src.PendingSplitsCheckpointSerializer`
- `org.apache.flink.connector.file.src.impl.StreamFormatAdapter` (see [optional S3 client](#optional-different-s3--minio-client) below)

----

## Optional different S3 / MinIO client

By default the `OwnFileSource` resolves files through Flink's globally initialised `FileSystem`, which is configured once per cluster. Sometimes a single source needs to read files from a *different* S3 / MinIO than the cluster default. For that case the builder accepts optional, serializable connection settings:

```java
S3FileSystemSettings settings =
    new S3FileSystemSettings(
        "http://localhost:9000", // endpoint
        "accessKey",
        "secretKey",
        "my-bucket");

final OwnFileSource<List<String>> source =
    OwnFileSource.forRecordStreamFormat(new ListStringInputFormat(), new Path("s3://my-bucket/dir"))
        .monitorContinuously(Duration.ofSeconds(1))
        .withFileSystem(settings) // <-- read through this endpoint instead of the global file system
        .build();
```

### How it works
The settings are serializable so they travel with the source to the JobManager (enumeration) and to every TaskManager (reading); the actual client is built lazily, once per JVM. When `withFileSystem(...)` is set, the source swaps two components:

- [`OwnFileSystemEnumerator`](OwnFileSystemEnumerator.java) lists objects and their modification times through the configured endpoint instead of `path.getFileSystem()`. It is a thin subclass of Flink's `NonSplittingRecursiveEnumerator` and reuses its hidden-file filtering and split conversion, so the modification-time mechanism keeps working unchanged.
- [`MinioStreamFormatAdapter`](MinioStreamFormatAdapter.java) opens the file content through the configured endpoint. It is a fork of Flink's `StreamFormatAdapter` where the only behavioural change is which `FileSystem` opens the stream.

Both use [`MinioFileSystem`](MinioFileSystem.java), a small read-only Flink `FileSystem` backed by a `MinioClient` ([`MinioFileStatus`](MinioFileStatus.java), [`MinioInputStream`](MinioInputStream.java)), built from [`S3FileSystemSettings`](S3FileSystemSettings.java).

### Limitations
- Only supported together with `forRecordStreamFormat(...)` (the `forBulkFileFormat(...)` path is not adapted). Calling `withFileSystem(...)` on a bulk-format source fails fast at `build()`.
- When `withFileSystem(...)` is **not** called, behaviour is unchanged and the global file system is used (fully backward compatible).
- The checkpoint format is unchanged: the settings ride along in the serialized source, not in the checkpoint.

----

## additional `SimpleStreamFormat` 
[ListStringInputFormat](ListStringInputFormat.java)  
For another use case, a different input format was required. This format is similar to the `TextLineInputFormat` (`org.apache.flink.connector.file.src.reader.TextLineInputFormat`), but instead of reading the file line by line and returning individual strings, it reads the entire file into a `List<String>`.