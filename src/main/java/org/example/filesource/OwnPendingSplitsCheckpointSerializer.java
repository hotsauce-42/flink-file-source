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

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/** A serializer for the {@link OwnPendingSplitsCheckpointSerializer}. */
@PublicEvolving
public final class OwnPendingSplitsCheckpointSerializer<T extends FileSourceSplit>
    implements SimpleVersionedSerializer<OwnPendingSplitsCheckpoint<T>> {

  private static final int VERSION = 1;

  private static final int VERSION_1_MAGIC_NUMBER = 0xDEADBEEF;

  private final SimpleVersionedSerializer<T> splitSerializer;

  public OwnPendingSplitsCheckpointSerializer(SimpleVersionedSerializer<T> splitSerializer) {
    this.splitSerializer = checkNotNull(splitSerializer);
  }

  // ------------------------------------------------------------------------

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(OwnPendingSplitsCheckpoint<T> checkpoint) throws IOException {
    checkArgument(
        checkpoint.getClass() == OwnPendingSplitsCheckpoint.class,
        "Cannot serialize subclasses of PendingSplitsCheckpoint");

    // optimization: the splits lazily cache their own serialized form
    if (checkpoint.serializedFormCache != null) {
      return checkpoint.serializedFormCache;
    }

    final SimpleVersionedSerializer<T> splitSerializer = this.splitSerializer; // stack cache
    final Collection<T> splits = checkpoint.getSplits();
    final long lastProcessedModificationTime = checkpoint.getLastProcessedModificationTime();

    final ArrayList<byte[]> serializedSplits = new ArrayList<>(splits.size());

    int totalLen =
        12 + 8; // three ints: magic, version of split serializer, count splits; one long:
    // lastModTime

    for (T split : splits) {
      final byte[] serSplit = splitSerializer.serialize(split);
      serializedSplits.add(serSplit);
      totalLen += serSplit.length + 4; // 4 bytes for the length field
    }

    final byte[] result = new byte[totalLen];
    final ByteBuffer byteBuffer = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
    byteBuffer.putInt(VERSION_1_MAGIC_NUMBER);
    byteBuffer.putInt(splitSerializer.getVersion());
    byteBuffer.putInt(serializedSplits.size());
    byteBuffer.putLong(lastProcessedModificationTime);

    for (byte[] splitBytes : serializedSplits) {
      byteBuffer.putInt(splitBytes.length);
      byteBuffer.put(splitBytes);
    }

    assert byteBuffer.remaining() == 0;

    // optimization: cache the serialized from, so we avoid the byte work during repeated
    // serialization
    checkpoint.serializedFormCache = result;

    return result;
  }

  @Override
  public OwnPendingSplitsCheckpoint<T> deserialize(int version, byte[] serialized)
      throws IOException {
    if (version == 1) {
      return deserializeV1(serialized);
    }
    throw new IOException("Unknown version: " + version);
  }

  private OwnPendingSplitsCheckpoint<T> deserializeV1(byte[] serialized) throws IOException {
    final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

    final int magic = bb.getInt();
    if (magic != VERSION_1_MAGIC_NUMBER) {
      throw new IOException(
          String.format(
              "Invalid magic number for PendingSplitsCheckpoint. " + "Expected: %X , found %X",
              VERSION_1_MAGIC_NUMBER, magic));
    }

    final int splitSerializerVersion = bb.getInt();
    final int numSplits = bb.getInt();
    final long lastProcessedModificationTime = bb.getLong();

    final SimpleVersionedSerializer<T> splitSerializer = this.splitSerializer; // stack cache
    final ArrayList<T> splits = new ArrayList<>(numSplits);

    for (int remaining = numSplits; remaining > 0; remaining--) {
      final byte[] bytes = new byte[bb.getInt()];
      bb.get(bytes);
      final T split = splitSerializer.deserialize(splitSerializerVersion, bytes);
      splits.add(split);
    }

    return OwnPendingSplitsCheckpoint.reusingCollection(splits, lastProcessedModificationTime);
  }
}
