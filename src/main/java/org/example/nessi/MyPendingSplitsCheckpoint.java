package org.example.nessi;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.PendingSplitsCheckpointSerializer;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class MyPendingSplitsCheckpoint <SplitT extends FileSourceSplit> {

  /** The splits in the checkpoint. */
  private final Collection<SplitT> splits;

  /**
   * The paths that are no longer in the enumerator checkpoint, but have been processed before and
   * should this be ignored. Relevant only for sources in continuous monitoring mode.
   */
  private final long alreadyProcessedModificationTime;

  /**
   * The cached byte representation from the last serialization step. This helps to avoid paying
   * repeated serialization cost for the same checkpoint object. This field is used by {@link
   * PendingSplitsCheckpointSerializer}.
   */
  @Nullable
  byte[] serializedFormCache;

  protected MyPendingSplitsCheckpoint(
      Collection<SplitT> splits, long alreadyProcessedModificationTime) {
    this.splits = Collections.unmodifiableCollection(splits);
    this.alreadyProcessedModificationTime = alreadyProcessedModificationTime;
  }

  // ------------------------------------------------------------------------

  public Collection<SplitT> getSplits() {
    return splits;
  }

  public long getAlreadyProcessedPaths() {
    return alreadyProcessedModificationTime;
  }

  // ------------------------------------------------------------------------

  @Override
  public String toString() {
    return "PendingSplitsCheckpoint:\n"
        + "\t\t Pending Splits: "
        + splits
        + '\n'
        + "\t\t Processed ModificationTime: "
        + alreadyProcessedModificationTime
        + '\n';
  }

  // ------------------------------------------------------------------------
  //  factories
  // ------------------------------------------------------------------------

  public static <T extends FileSourceSplit> MyPendingSplitsCheckpoint<T> fromCollectionSnapshot(
      final Collection<T> splits) {
    checkNotNull(splits);

    // create a copy of the collection to make sure this checkpoint is immutable
    final Collection<T> copy = new ArrayList<>(splits);
    return new MyPendingSplitsCheckpoint<>(copy, 0);
  }

  public static <T extends FileSourceSplit> MyPendingSplitsCheckpoint<T> fromCollectionSnapshot(
      final Collection<T> splits, final long alreadyProcessedModificationTime) {
    checkNotNull(splits);

    // create a copy of the collection to make sure this checkpoint is immutable
    final Collection<T> splitsCopy = new ArrayList<>(splits);
    final long modTimeCopy = alreadyProcessedModificationTime;

    return new MyPendingSplitsCheckpoint<>(splitsCopy, modTimeCopy);
  }

  static <T extends FileSourceSplit> MyPendingSplitsCheckpoint<T> reusingCollection(
      final Collection<T> splits, final long alreadyProcessedModificationTime) {
    return new MyPendingSplitsCheckpoint<>(splits, alreadyProcessedModificationTime);
  }
}
