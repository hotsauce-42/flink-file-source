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

import io.minio.MinioClient;
import java.io.Serializable;
import java.net.URI;
import javax.annotation.Nullable;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.FileSystem;

/**
 * Serializable connection settings for an alternative S3 / MinIO endpoint.
 *
 * <p>By default the {@link OwnFileSource} resolves files through Flink's globally initialised
 * {@link FileSystem} (configured once per cluster). Passing an instance of this class to {@link
 * OwnFileSource.FileSourceBuilder#withFileSystem(S3FileSystemSettings)} makes a single source read
 * from a <i>different</i> S3 / MinIO endpoint without touching the global configuration. This is
 * useful when a job needs to ingest files from another object store than the cluster default.
 *
 * <p>The settings themselves are serializable so they can be shipped to the JobManager (for split
 * enumeration) and to every TaskManager (for reading). The actual {@link FileSystem} / {@link
 * MinioClient} is not serializable and is therefore built lazily, once per JVM, in {@link
 * #getFileSystem()}.
 */
@PublicEvolving
public class S3FileSystemSettings implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String endpoint;
  private final String accessKey;
  private final String secretKey;
  @Nullable private final String region;
  private final String bucket;

  /** Lazily built once per JVM (JobManager enumerator and each TaskManager reader). */
  private transient volatile FileSystem fileSystem;

  /**
   * Creates settings for the given endpoint and bucket without an explicit region.
   *
   * @param endpoint the S3 / MinIO endpoint, for example {@code http://localhost:9000}
   * @param accessKey the access key used to authenticate
   * @param secretKey the secret key used to authenticate
   * @param bucket the bucket the input paths refer to
   */
  public S3FileSystemSettings(String endpoint, String accessKey, String secretKey, String bucket) {
    this(endpoint, accessKey, secretKey, null, bucket);
  }

  /**
   * Creates settings for the given endpoint and bucket.
   *
   * @param endpoint the S3 / MinIO endpoint, for example {@code http://localhost:9000}
   * @param accessKey the access key used to authenticate
   * @param secretKey the secret key used to authenticate
   * @param region the region, or {@code null} to let the client decide
   * @param bucket the bucket the input paths refer to
   */
  public S3FileSystemSettings(
      String endpoint, String accessKey, String secretKey, @Nullable String region, String bucket) {
    this.endpoint = checkNotNull(endpoint, "endpoint");
    this.accessKey = checkNotNull(accessKey, "accessKey");
    this.secretKey = checkNotNull(secretKey, "secretKey");
    this.region = region;
    this.bucket = checkNotNull(bucket, "bucket");
  }

  public String getBucket() {
    return bucket;
  }

  /**
   * Returns the {@link FileSystem} backed by these settings, building and caching it lazily.
   *
   * <p>The instance is cached per JVM so that the JobManager enumerator and each TaskManager reader
   * reuse a single {@link MinioClient}.
   */
  public FileSystem getFileSystem() {
    FileSystem fs = fileSystem;
    if (fs == null) {
      synchronized (this) {
        fs = fileSystem;
        if (fs == null) {
          fs = new MinioFileSystem(createClient(), bucket, URI.create(endpoint));
          fileSystem = fs;
        }
      }
    }
    return fs;
  }

  private MinioClient createClient() {
    MinioClient.Builder builder =
        MinioClient.builder().endpoint(endpoint).credentials(accessKey, secretKey);
    if (region != null) {
      builder.region(region);
    }
    return builder.build();
  }
}
