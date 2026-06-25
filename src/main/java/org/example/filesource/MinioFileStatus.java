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

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

/**
 * A {@link FileStatus} describing a single object (or prefix) in a MinIO / S3 bucket.
 *
 * <p>The {@link #getModificationTime() modification time} is what drives the {@link
 * OwnContinuousFileSplitEnumerator}'s re-reading logic, so it is taken directly from the object's
 * {@code lastModified} timestamp.
 */
public class MinioFileStatus implements FileStatus {

  private final Path path;
  private final long length;
  private final long modificationTime;
  private final boolean isDir;

  public MinioFileStatus(Path path, long length, long modificationTime, boolean isDir) {
    this.path = path;
    this.length = length;
    this.modificationTime = modificationTime;
    this.isDir = isDir;
  }

  @Override
  public long getLen() {
    return length;
  }

  @Override
  public long getBlockSize() {
    // object stores read a whole object at once; treat it as a single block
    return length;
  }

  @Override
  public short getReplication() {
    return 1;
  }

  @Override
  public long getModificationTime() {
    return modificationTime;
  }

  @Override
  public long getAccessTime() {
    return 0;
  }

  @Override
  public boolean isDir() {
    return isDir;
  }

  @Override
  public Path getPath() {
    return path;
  }
}
