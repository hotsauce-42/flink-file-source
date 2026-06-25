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

import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.Item;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

/**
 * A read-only Flink {@link FileSystem} backed by a {@link MinioClient}.
 *
 * <p>This adapter exposes a single MinIO / S3 bucket through Flink's {@code FileSystem} abstraction
 * so that the {@link OwnFileSystemEnumerator} (for listing and modification times) and the {@link
 * MinioStreamFormatAdapter} (for reading) can use a per-source endpoint instead of the globally
 * initialised file system.
 *
 * <p>Only the operations required by the {@code OwnFileSource} are implemented: {@link
 * #getFileStatus(Path)}, {@link #listStatus(Path)} and {@link #open(Path)}. Mutating operations are
 * unsupported. Because object stores are flat, {@link #listStatus(Path)} lists all objects under a
 * prefix recursively (returning only files), which is exactly what the recursive enumerator needs.
 */
public class MinioFileSystem extends FileSystem {

  private final MinioClient client;
  private final String bucket;
  private final URI uri;

  public MinioFileSystem(MinioClient client, String bucket, URI uri) {
    this.client = checkNotNull(client, "client");
    this.bucket = checkNotNull(bucket, "bucket");
    this.uri = checkNotNull(uri, "uri");
  }

  // ------------------------------------------------------------------------
  //  reading / listing
  // ------------------------------------------------------------------------

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    final String key = toObjectKey(f);
    try {
      final StatObjectResponse stat =
          client.statObject(StatObjectArgs.builder().bucket(bucket).object(key).build());
      return new MinioFileStatus(f, stat.size(), toEpochMillis(stat.lastModified()), false);
    } catch (ErrorResponseException e) {
      // no object with this exact key -> treat it as a (virtual) directory / prefix
      return new MinioFileStatus(f, 0L, 0L, true);
    } catch (Exception e) {
      throw new IOException("Failed to stat object '" + key + "'", e);
    }
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    String prefix = toObjectKey(f);
    if (!prefix.isEmpty() && !prefix.endsWith("/")) {
      prefix = prefix + "/";
    }

    final List<FileStatus> result = new ArrayList<>();
    final Iterable<Result<Item>> objects =
        client.listObjects(
            ListObjectsArgs.builder().bucket(bucket).prefix(prefix).recursive(true).build());
    try {
      for (Result<Item> object : objects) {
        final Item item = object.get();
        if (item.isDir()) {
          continue;
        }
        result.add(
            new MinioFileStatus(
                toPath(item.objectName()), item.size(), toEpochMillis(item.lastModified()), false));
      }
    } catch (Exception e) {
      throw new IOException("Failed to list objects under prefix '" + prefix + "'", e);
    }
    return result.toArray(new FileStatus[0]);
  }

  @Override
  public FSDataInputStream open(Path f) throws IOException {
    return new MinioInputStream(client, bucket, toObjectKey(f));
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return open(f);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) {
    // object stores expose no host locality information
    return new BlockLocation[0];
  }

  // ------------------------------------------------------------------------
  //  metadata
  // ------------------------------------------------------------------------

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public Path getWorkingDirectory() {
    return new Path(uri);
  }

  @Override
  public Path getHomeDirectory() {
    return new Path(uri);
  }

  @Override
  public boolean isDistributedFS() {
    return true;
  }

  // ------------------------------------------------------------------------
  //  unsupported mutating operations
  // ------------------------------------------------------------------------

  @Override
  public FSDataOutputStream create(Path f, WriteMode overwriteMode) {
    throw new UnsupportedOperationException("MinioFileSystem is read-only");
  }

  @Override
  public boolean delete(Path f, boolean recursive) {
    throw new UnsupportedOperationException("MinioFileSystem is read-only");
  }

  @Override
  public boolean mkdirs(Path f) {
    throw new UnsupportedOperationException("MinioFileSystem is read-only");
  }

  @Override
  public boolean rename(Path src, Path dst) {
    throw new UnsupportedOperationException("MinioFileSystem is read-only");
  }

  // ------------------------------------------------------------------------
  //  helpers
  // ------------------------------------------------------------------------

  /** Maps a Flink {@link Path} to the object key within the bucket. */
  private String toObjectKey(Path path) {
    final String p = path.toUri().getPath();
    return p.startsWith("/") ? p.substring(1) : p;
  }

  /** Builds a Flink {@link Path} for an object key, preserving the configured scheme and bucket. */
  private Path toPath(String objectName) {
    final String scheme = uri.getScheme() == null ? "s3" : uri.getScheme();
    return new Path(scheme, bucket, "/" + objectName);
  }

  private static long toEpochMillis(java.time.ZonedDateTime time) {
    return time == null ? 0L : time.toInstant().toEpochMilli();
  }
}
