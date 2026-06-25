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

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

/**
 * A {@link NonSplittingRecursiveEnumerator} that enumerates files through a {@link FileSystem}
 * built from {@link S3FileSystemSettings} instead of the globally initialised one.
 *
 * <p>This is the enumeration counterpart that lets a single {@link OwnFileSource} read from a
 * different S3 / MinIO endpoint. It reuses the parent's hidden-file filtering and split conversion
 * (including the modification times that {@link OwnContinuousFileSplitEnumerator} relies on); the
 * only difference is which {@code FileSystem} the listing runs against.
 */
@Internal
public class OwnFileSystemEnumerator extends NonSplittingRecursiveEnumerator {

  private final S3FileSystemSettings settings;

  public OwnFileSystemEnumerator(S3FileSystemSettings settings) {
    this.settings = checkNotNull(settings, "settings");
  }

  @Override
  public Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits)
      throws IOException {
    final ArrayList<FileSourceSplit> splits = new ArrayList<>();

    final FileSystem fs = settings.getFileSystem();
    for (Path path : paths) {
      addSplitsForPath(fs.getFileStatus(path), fs, splits);
    }

    return splits;
  }
}
