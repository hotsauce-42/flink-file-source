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

package org.example.filesource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.Path;
import org.junit.jupiter.api.Test;

/** Unit tests for the optional {@code withFileSystem(...)} wiring on the {@link OwnFileSource}. */
class S3FileSystemBuilderTest {

  private static final S3FileSystemSettings SETTINGS =
      new S3FileSystemSettings("http://localhost:9000", "access", "secret", "bucket");

  @Test
  void streamFormatSourceWithCustomFileSystemBuilds() {
    final OwnFileSource<List<String>> source =
        OwnFileSource.forRecordStreamFormat(
                new ListStringInputFormat(), new Path("s3://bucket/dir"))
            .monitorContinuously(Duration.ofSeconds(1))
            .withFileSystem(SETTINGS)
            .build();

    assertThat(source.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
  }

  @Test
  void withoutCustomFileSystemBehaviourIsUnchanged() {
    final OwnFileSource<List<String>> source =
        OwnFileSource.forRecordStreamFormat(
                new ListStringInputFormat(), new Path("s3://bucket/dir"))
            .monitorContinuously(Duration.ofSeconds(1))
            .build();

    assertThat(source.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
  }

  @Test
  void bulkFormatSourceWithCustomFileSystemFailsFast() {
    @SuppressWarnings("unchecked")
    final BulkFormat<List<String>, FileSourceSplit> bulkFormat =
        (BulkFormat<List<String>, FileSourceSplit>)
            (BulkFormat<?, FileSourceSplit>)
                new org.apache.flink.connector.file.src.impl.StreamFormatAdapter<>(
                    new ListStringInputFormat());

    assertThatThrownBy(
            () ->
                OwnFileSource.forBulkFileFormat(bulkFormat, new Path("s3://bucket/dir"))
                    .monitorContinuously(Duration.ofSeconds(1))
                    .withFileSystem(SETTINGS)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("forRecordStreamFormat");
  }
}
