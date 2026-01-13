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

----

## additional `SimpleStreamFormat` 
[ListStringInputFormat](ListStringInputFormat.java)  
For another use case, a different input format was required. This format is similar to the `TextLineInputFormat` (`org.apache.flink.connector.file.src.reader.TextLineInputFormat`), but instead of reading the file line by line and returning individual strings, it reads the entire file into a `List<String>`.