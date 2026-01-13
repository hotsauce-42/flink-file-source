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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.jetbrains.annotations.Nullable;

/**
 * A reader format that text lines from a file.
 *
 * <p>The reader uses Java's built-in {@link InputStreamReader} to decode the byte stream using
 * various supported charset encodings.
 *
 * <p>This format does not support optimized recovery from checkpoints. On recovery, it will re-read
 * and discard the number of lined that were processed before the last checkpoint. That is due to
 * the fact that the offsets of lines in the file cannot be tracked through the charset decoders
 * with their internal buffering of stream input and charset decoder state.
 */
@PublicEvolving
public class ListStringInputFormat extends SimpleStreamFormat<List<String>> {

  private static final long serialVersionUID = 1L;

  public static final String DEFAULT_CHARSET_NAME = "UTF-8";

  private final String charsetName;

  public ListStringInputFormat() {
    this(DEFAULT_CHARSET_NAME);
  }

  public ListStringInputFormat(String charsetName) {
    this.charsetName = charsetName;
  }

  @Override
  public Reader createReader(Configuration config, FSDataInputStream stream) throws IOException {
    final BufferedReader reader = new BufferedReader(new InputStreamReader(stream, charsetName));
    return new Reader(reader);
  }

  @Override
  public TypeInformation<List<String>> getProducedType() {
    return Types.LIST(Types.STRING);
  }

  // ------------------------------------------------------------------------

  /** The actual reader for the {@code TextLineInputFormat}. */
  @PublicEvolving
  public static final class Reader implements StreamFormat.Reader<List<String>> {

    private final BufferedReader reader;

    private boolean alreadyRead;

    Reader(final BufferedReader reader) {
      this.reader = reader;
      this.alreadyRead = false;
    }

    @Nullable
    @Override
    public List<String> read() throws IOException {
      if (alreadyRead) {
        return null; // End of file
      }

      alreadyRead = true;
      return reader.lines().toList();
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
