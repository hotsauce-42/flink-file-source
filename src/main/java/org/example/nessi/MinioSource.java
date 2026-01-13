package org.example.nessi;

import java.util.List;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class MinioSource
    implements Source<List<String>, FileSourceSplit, PendingSplitsCheckpoint<FileSourceSplit>> {

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint<FileSourceSplit>>
      createEnumerator(SplitEnumeratorContext<FileSourceSplit> enumContext) throws Exception {
    return null;
  }

  @Override
  public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint<FileSourceSplit>>
      restoreEnumerator(
          SplitEnumeratorContext<FileSourceSplit> enumContext,
          PendingSplitsCheckpoint<FileSourceSplit> checkpoint)
          throws Exception {
    return null;
  }

  @Override
  public SimpleVersionedSerializer<FileSourceSplit> getSplitSerializer() {
    return null;
  }

  @Override
  public SimpleVersionedSerializer<PendingSplitsCheckpoint<FileSourceSplit>>
      getEnumeratorCheckpointSerializer() {
    return null;
  }

  @Override
  public SourceReader<List<String>, FileSourceSplit> createReader(SourceReaderContext readerContext)
      throws Exception {
    return null;
  }
}
