package org.example.filesource; /*
                                 * Licensed to the Apache Software Foundation (ASF) under one
                                 * or more contributor license agreements.  See the NOTICE file
                                 * distributed with this work for additional information
                                 * regarding copyright ownership.  The ASF licenses this file
                                 * to you under the Apache License, Version 2.0 (the
                                 * "License"); you may not use this file except in compliance
                                 * with the License.  You may obtain a copy of the License at
                                 *
                                 *     http://www.apache.org/licenses/LICENSE-2.0
                                 *
                                 * Unless required by applicable law or agreed to in writing, software
                                 * distributed under the License is distributed on an "AS IS" BASIS,
                                 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
                                 * See the License for the specific language governing permissions and
                                 * limitations under the License.
                                 */

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import javax.annotation.Nullable;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.PendingSplitsCheckpointSerializer;

/**
 * A checkpoint of the current state of the containing the currently pending splits that are not yet
 * assigned.
 */
@PublicEvolving
public class OwnPendingSplitsCheckpoint<SplitT extends FileSourceSplit> {

  /** The splits in the checkpoint. */
  private final Collection<SplitT> splits;

  /** The modification timestamp of the newest file that has been processed. */
  private final long lastProcessedModificationTime;

  /**
   * The cached byte representation from the last serialization step. This helps to avoid paying
   * repeated serialization cost for the same checkpoint object. This field is used by {@link
   * PendingSplitsCheckpointSerializer}.
   */
  @Nullable byte[] serializedFormCache;

  protected OwnPendingSplitsCheckpoint(
      Collection<SplitT> splits, long lastProcessedModificationTime) {
    this.splits = Collections.unmodifiableCollection(splits);
    this.lastProcessedModificationTime = lastProcessedModificationTime;
  }

  // ------------------------------------------------------------------------

  public Collection<SplitT> getSplits() {
    return splits;
  }

  public long getLastProcessedModificationTime() {
    return lastProcessedModificationTime;
  }

  // ------------------------------------------------------------------------

  @Override
  public String toString() {
    return "PendingSplitsCheckpoint:\n"
        + "\t\t Pending Splits: "
        + splits
        + '\n'
        + "\t\t Last Processed Modification Time: "
        + lastProcessedModificationTime
        + '\n';
  }

  // ------------------------------------------------------------------------
  //  factories
  // ------------------------------------------------------------------------

  public static <T extends FileSourceSplit> OwnPendingSplitsCheckpoint<T> fromCollectionSnapshot(
      final Collection<T> splits, long lastProcessedModificationTime) {
    checkNotNull(splits);

    // create a copy of the collection to make sure this checkpoint is immutable
    final Collection<T> splitsCopy = new ArrayList<>(splits);

    return new OwnPendingSplitsCheckpoint<>(splitsCopy, lastProcessedModificationTime);
  }

  static <T extends FileSourceSplit> OwnPendingSplitsCheckpoint<T> reusingCollection(
      final Collection<T> splits, long lastProcessedModificationTime) {
    return new OwnPendingSplitsCheckpoint<>(splits, lastProcessedModificationTime);
  }
}
