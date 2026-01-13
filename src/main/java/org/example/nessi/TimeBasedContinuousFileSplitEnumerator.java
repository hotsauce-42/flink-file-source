package org.example.nessi;

import java.io.IOException;
import java.util.*;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SupportsBatchSnapshot;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class TimeBasedContinuousFileSplitEnumerator implements SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint<FileSourceSplit>>,
    SupportsBatchSnapshot {

  private static final Logger LOG = LoggerFactory.getLogger(TimeBasedContinuousFileSplitEnumerator.class);

  private final SplitEnumeratorContext<FileSourceSplit> context;

  private final FileSplitAssigner splitAssigner;

  private final FileEnumerator enumerator;

  private final HashSet<Path> pathsAlreadyProcessed;

  private long modificationTimeAlreadyProcessed;

  private final LinkedHashMap<Integer, String> readersAwaitingSplit;

  private final Path[] paths;

  private final long discoveryInterval;

  // ------------------------------------------------------------------------

  public TimeBasedContinuousFileSplitEnumerator(
      SplitEnumeratorContext<FileSourceSplit> context,
      FileEnumerator enumerator,
      FileSplitAssigner splitAssigner,
      Path[] paths,
      long modificationTimeAlreadyProcessed,
      Collection<Path> alreadyDiscoveredPaths,
      long discoveryInterval) {

    checkArgument(discoveryInterval > 0L);
    this.context = checkNotNull(context);
    this.enumerator = checkNotNull(enumerator);
    this.splitAssigner = checkNotNull(splitAssigner);
    this.paths = paths;
    this.discoveryInterval = discoveryInterval;
    this.pathsAlreadyProcessed = new HashSet<>(alreadyDiscoveredPaths);
    this.modificationTimeAlreadyProcessed = modificationTimeAlreadyProcessed;
    this.readersAwaitingSplit = new LinkedHashMap<>();
  }

  @Override
  public void start() {
    context.callAsync(
        () -> enumerator.enumerateSplits(paths, 1),
        this::processDiscoveredSplits,
        discoveryInterval,
        discoveryInterval);
  }

  @Override
  public void close() throws IOException {
    // no resources to close
  }

  @Override
  public void addReader(int subtaskId) {
    // this source is purely lazy-pull-based, nothing to do upon registration
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    readersAwaitingSplit.put(subtaskId, requesterHostname);
    assignSplits();
  }

  @Override
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    LOG.error("Received unrecognized event: {}", sourceEvent);
  }

  @Override
  public void addSplitsBack(List<FileSourceSplit> splits, int subtaskId) {
    LOG.debug("File Source Enumerator adds splits back: {}", splits);
    splitAssigner.addSplits(splits);
  }

  @Override
  public PendingSplitsCheckpoint<FileSourceSplit> snapshotState(long checkpointId)
      throws Exception {
    final PendingSplitsCheckpoint<FileSourceSplit> checkpoint =
        PendingSplitsCheckpoint.fromCollectionSnapshot(
            splitAssigner.remainingSplits(), pathsAlreadyProcessed);

    LOG.debug("Source Checkpoint is {}", checkpoint);
    return checkpoint;
  }

  // ------------------------------------------------------------------------

  private void processDiscoveredSplits(Collection<FileSourceSplit> splits, Throwable error) {
    if (error != null) {
      LOG.error("Failed to enumerate files", error);
      return;
    }

    final Collection<FileSourceSplit> newSplits =
        splits.stream()
            .filter(split -> split.fileModificationTime() > modificationTimeAlreadyProcessed)
            .toList();

    for (FileSourceSplit split : newSplits ) {
      if(split.fileModificationTime() > modificationTimeAlreadyProcessed) {
        modificationTimeAlreadyProcessed = split.fileModificationTime();
      }
    }

    splitAssigner.addSplits(newSplits);

    assignSplits();
  }

  private void assignSplits() {
    final Iterator<Map.Entry<Integer, String>> awaitingReader =
        readersAwaitingSplit.entrySet().iterator();

    while (awaitingReader.hasNext()) {
      final Map.Entry<Integer, String> nextAwaiting = awaitingReader.next();

      // if the reader that requested another split has failed in the meantime, remove
      // it from the list of waiting readers
      if (!context.registeredReaders().containsKey(nextAwaiting.getKey())) {
        awaitingReader.remove();
        continue;
      }

      final String hostname = nextAwaiting.getValue();
      final int awaitingSubtask = nextAwaiting.getKey();
      final Optional<FileSourceSplit> nextSplit = splitAssigner.getNext(hostname);
      if (nextSplit.isPresent()) {
        context.assignSplit(nextSplit.get(), awaitingSubtask);
        awaitingReader.remove();
      } else {
        break;
      }
    }
  }
}
