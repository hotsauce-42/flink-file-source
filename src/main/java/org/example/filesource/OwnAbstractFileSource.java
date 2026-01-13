package org.example.filesource;

/*
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

// package org.apache.flink.connector.file.src;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.file.src.ContinuousEnumerationSettings;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.connector.file.src.impl.FileSourceReader;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.FlinkRuntimeException;

/**
 * The base class for File Sources. The main implementation to use is the {@link OwnFileSource},
 * which also has the majority of the documentation.
 *
 * <p>To read new formats, one commonly does NOT need to extend this class, but should implement a
 * new Format Reader (like {@link StreamFormat}, {@link BulkFormat} and use it with the {@code
 * FileSource}.
 *
 * <p>The only reason to extend this class is when a source needs a different type of <i>split</i>,
 * meaning an extension of the {@link FileSourceSplit} to carry additional information.
 *
 * @param <T> The type of the events/records produced by this source.
 * @param <SplitT> The subclass type of the FileSourceSplit used by the source implementation.
 */
@PublicEvolving
public abstract class OwnAbstractFileSource<T, SplitT extends FileSourceSplit>
    implements Source<T, SplitT, OwnPendingSplitsCheckpoint<SplitT>>, ResultTypeQueryable<T> {

  private static final long serialVersionUID = 1L;

  final Path[] inputPaths;

  private final FileEnumerator.Provider enumeratorFactory;

  private final FileSplitAssigner.Provider assignerFactory;

  private final BulkFormat<T, SplitT> readerFormat;

  @Nullable private final ContinuousEnumerationSettings continuousEnumerationSettings;

  // ------------------------------------------------------------------------

  protected OwnAbstractFileSource(
      final Path[] inputPaths,
      final FileEnumerator.Provider fileEnumerator,
      final FileSplitAssigner.Provider splitAssigner,
      final BulkFormat<T, SplitT> readerFormat,
      @Nullable final ContinuousEnumerationSettings continuousEnumerationSettings) {

    checkArgument(inputPaths.length > 0);
    this.inputPaths = inputPaths;
    this.enumeratorFactory = checkNotNull(fileEnumerator);
    this.assignerFactory = checkNotNull(splitAssigner);
    this.readerFormat = checkNotNull(readerFormat);
    this.continuousEnumerationSettings = continuousEnumerationSettings;
  }

  // ------------------------------------------------------------------------
  //  Getters
  // ------------------------------------------------------------------------

  protected FileEnumerator.Provider getEnumeratorFactory() {
    return enumeratorFactory;
  }

  public FileSplitAssigner.Provider getAssignerFactory() {
    return assignerFactory;
  }

  @Nullable
  public ContinuousEnumerationSettings getContinuousEnumerationSettings() {
    return continuousEnumerationSettings;
  }

  // ------------------------------------------------------------------------
  //  Source API Methods
  // ------------------------------------------------------------------------

  @Override
  public Boundedness getBoundedness() {
    return continuousEnumerationSettings == null
        ? Boundedness.BOUNDED
        : Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SourceReader<T, SplitT> createReader(SourceReaderContext readerContext) {
    return new FileSourceReader<>(readerContext, readerFormat, readerContext.getConfiguration());
  }

  @Override
  public SplitEnumerator<SplitT, OwnPendingSplitsCheckpoint<SplitT>> createEnumerator(
      SplitEnumeratorContext<SplitT> enumContext) {

    final FileEnumerator enumerator = enumeratorFactory.create();

    // read the initial set of splits (which is also the total set of splits for bounded
    // sources)
    final Collection<FileSourceSplit> splits;
    try {
      // TODO - in the next cleanup pass, we should try to remove the need to "wrap unchecked"
      // here
      splits = enumerator.enumerateSplits(inputPaths, enumContext.currentParallelism());
    } catch (IOException e) {
      throw new FlinkRuntimeException("Could not enumerate file splits", e);
    }

    long lastProcessedModificationTime = 0;
    for (FileSourceSplit split : splits) {
      if (split.fileModificationTime() > lastProcessedModificationTime) {
        lastProcessedModificationTime = split.fileModificationTime();
      }
    }

    return createSplitEnumerator(enumContext, enumerator, splits, lastProcessedModificationTime);
  }

  @Override
  public SplitEnumerator<SplitT, OwnPendingSplitsCheckpoint<SplitT>> restoreEnumerator(
      SplitEnumeratorContext<SplitT> enumContext, OwnPendingSplitsCheckpoint<SplitT> checkpoint) {

    final FileEnumerator enumerator = enumeratorFactory.create();

    // cast this to a collection of FileSourceSplit because the enumerator code work
    // non-generically just on that base split type
    @SuppressWarnings("unchecked")
    final Collection<FileSourceSplit> splits = (Collection<FileSourceSplit>) checkpoint.getSplits();

    return createSplitEnumerator(
        enumContext, enumerator, splits, checkpoint.getLastProcessedModificationTime());
  }

  @Override
  public abstract SimpleVersionedSerializer<SplitT> getSplitSerializer();

  @Override
  public SimpleVersionedSerializer<OwnPendingSplitsCheckpoint<SplitT>>
      getEnumeratorCheckpointSerializer() {
    return new OwnPendingSplitsCheckpointSerializer<>(getSplitSerializer());
  }

  @Override
  public TypeInformation<T> getProducedType() {
    return readerFormat.getProducedType();
  }

  // ------------------------------------------------------------------------
  //  helpers
  // ------------------------------------------------------------------------

  private SplitEnumerator<SplitT, OwnPendingSplitsCheckpoint<SplitT>> createSplitEnumerator(
      SplitEnumeratorContext<SplitT> context,
      FileEnumerator enumerator,
      Collection<FileSourceSplit> splits,
      long lastProcessedModificationTime) {

    // cast this to a collection of FileSourceSplit because the enumerator code work
    // non-generically just on that base split type
    @SuppressWarnings("unchecked")
    final SplitEnumeratorContext<FileSourceSplit> fileSplitContext =
        (SplitEnumeratorContext<FileSourceSplit>) context;

    final FileSplitAssigner splitAssigner = assignerFactory.create(splits);

    if (continuousEnumerationSettings == null) {
      // bounded case
      // TODO
      // return castGeneric(new StaticFileSplitEnumerator(fileSplitContext, splitAssigner));
      return null;
    } else {
      // unbounded case

      return castGeneric(
          new OwnContinuousFileSplitEnumerator(
              fileSplitContext,
              enumerator,
              splitAssigner,
              inputPaths,
              lastProcessedModificationTime,
              continuousEnumerationSettings.getDiscoveryInterval().toMillis()));
    }
  }

  @SuppressWarnings("unchecked")
  private SplitEnumerator<SplitT, OwnPendingSplitsCheckpoint<SplitT>> castGeneric(
      final SplitEnumerator<FileSourceSplit, OwnPendingSplitsCheckpoint<FileSourceSplit>>
          enumerator) {

    // cast arguments away then cast them back. Java Generics Hell :-/
    return (SplitEnumerator<SplitT, OwnPendingSplitsCheckpoint<SplitT>>)
        (SplitEnumerator<?, ?>) enumerator;
  }

  private static Collection<Path> splitsToPaths(Collection<FileSourceSplit> splits) {
    return splits.stream()
        .map(FileSourceSplit::path)
        .collect(Collectors.toCollection(HashSet::new));
  }

  // ------------------------------------------------------------------------
  //  Builder
  // ------------------------------------------------------------------------

  /**
   * The generic base builder. This builder carries a <i>SELF</i> type to make it convenient to
   * extend this for subclasses, using the following pattern.
   *
   * <pre>{@code
   * public class SubBuilder<T> extends AbstractFileSourceBuilder<T, SubBuilder<T>> {
   *     ...
   * }
   * }</pre>
   *
   * <p>That way, all return values from builder method defined here are typed to the sub-class type
   * and support fluent chaining.
   *
   * <p>We don't make the publicly visible builder generic with a SELF type, because it leads to
   * generic signatures that can look complicated and confusing.
   */
  protected abstract static class AbstractFileSourceBuilder<
      T, SplitT extends FileSourceSplit, SELF extends AbstractFileSourceBuilder<T, SplitT, SELF>> {

    // mandatory - have no defaults
    protected final Path[] inputPaths;
    protected final BulkFormat<T, SplitT> readerFormat;

    // optional - have defaults
    protected FileEnumerator.Provider fileEnumerator;
    protected FileSplitAssigner.Provider splitAssigner;
    @Nullable protected ContinuousEnumerationSettings continuousSourceSettings;

    protected AbstractFileSourceBuilder(
        final Path[] inputPaths,
        final BulkFormat<T, SplitT> readerFormat,
        final FileEnumerator.Provider defaultFileEnumerator,
        final FileSplitAssigner.Provider defaultSplitAssigner) {

      this.inputPaths = checkNotNull(inputPaths);
      this.readerFormat = checkNotNull(readerFormat);
      this.fileEnumerator = defaultFileEnumerator;
      this.splitAssigner = defaultSplitAssigner;
    }

    /** Creates the file source with the settings applied to this builder. */
    public abstract OwnAbstractFileSource<T, SplitT> build();

    /**
     * Sets this source to streaming ("continuous monitoring") mode.
     *
     * <p>This makes the source a "continuous streaming" source that keeps running, monitoring for
     * new files, and reads these files when they appear and are discovered by the monitoring.
     *
     * <p>The interval in which the source checks for new files is the {@code discoveryInterval}.
     * Shorter intervals mean that files are discovered more quickly, but also imply more frequent
     * listing or directory traversal of the file system / object store.
     */
    public SELF monitorContinuously(Duration discoveryInterval) {
      checkNotNull(discoveryInterval, "discoveryInterval");
      checkArgument(
          !(discoveryInterval.isNegative() || discoveryInterval.isZero()),
          "discoveryInterval must be > 0");

      this.continuousSourceSettings = new ContinuousEnumerationSettings(discoveryInterval);
      return self();
    }

    /**
     * Sets this source to bounded (batch) mode.
     *
     * <p>In this mode, the source processes the files that are under the given paths when the
     * application is started. Once all files are processed, the source will finish.
     *
     * <p>This setting is also the default behavior. This method is mainly here to "switch back" to
     * bounded (batch) mode, or to make it explicit in the source construction.
     */
    public SELF processStaticFileSet() {
      this.continuousSourceSettings = null;
      return self();
    }

    /**
     * Configures the {@link FileEnumerator} for the source. The File Enumerator is responsible for
     * selecting from the input path the set of files that should be processed (and which to filter
     * out). Furthermore, the File Enumerator may split the files further into sub-regions, to
     * enable parallelization beyond the number of files.
     */
    public SELF setFileEnumerator(FileEnumerator.Provider fileEnumerator) {
      this.fileEnumerator = checkNotNull(fileEnumerator);
      return self();
    }

    /**
     * Configures the {@link FileSplitAssigner} for the source. The File Split Assigner determines
     * which parallel reader instance gets which {@link FileSourceSplit}, and in which order these
     * splits are assigned.
     */
    public SELF setSplitAssigner(FileSplitAssigner.Provider splitAssigner) {
      this.splitAssigner = checkNotNull(splitAssigner);
      return self();
    }

    @SuppressWarnings("unchecked")
    private SELF self() {
      return (SELF) this;
    }
  }
}
