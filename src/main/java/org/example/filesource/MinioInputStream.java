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

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import java.io.IOException;
import java.io.InputStream;
import org.apache.flink.core.fs.FSDataInputStream;

/**
 * A seekable {@link FSDataInputStream} over a single MinIO / S3 object.
 *
 * <p>MinIO object streams are not seekable on their own, so {@link #seek(long)} is implemented by
 * re-opening the object with a ranged {@code GetObject} request starting at the desired offset.
 */
public class MinioInputStream extends FSDataInputStream {

  private final MinioClient client;
  private final String bucket;
  private final String objectKey;

  private InputStream stream;
  private long position;

  public MinioInputStream(MinioClient client, String bucket, String objectKey) throws IOException {
    this.client = client;
    this.bucket = bucket;
    this.objectKey = objectKey;
    seek(0L);
  }

  @Override
  public void seek(long desired) throws IOException {
    if (desired < 0L) {
      throw new IOException("Cannot seek to a negative position: " + desired);
    }
    if (stream != null) {
      stream.close();
    }
    final GetObjectArgs.Builder args = GetObjectArgs.builder().bucket(bucket).object(objectKey);
    if (desired > 0L) {
      args.offset(desired);
    }
    try {
      stream = client.getObject(args.build());
    } catch (Exception e) {
      throw new IOException("Failed to open object '" + objectKey + "' at offset " + desired, e);
    }
    position = desired;
  }

  @Override
  public long getPos() {
    return position;
  }

  @Override
  public int read() throws IOException {
    final int b = stream.read();
    if (b != -1) {
      position++;
    }
    return b;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    final int n = stream.read(b, off, len);
    if (n > 0) {
      position += n;
    }
    return n;
  }

  @Override
  public void close() throws IOException {
    if (stream != null) {
      stream.close();
    }
  }
}
