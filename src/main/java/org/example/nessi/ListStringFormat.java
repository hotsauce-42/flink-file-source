package org.example.nessi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;

public class ListStringFormat extends SimpleStreamFormat<List<String>> {
  private static final long serialVersionUID = 1L;

  public static final String DEFAULT_CHARSET_NAME = "UTF-8";

  private final String charsetName;

  public ListStringFormat() {
    this.charsetName = DEFAULT_CHARSET_NAME;
  }

  @Override
  public Reader<List<String>> createReader(Configuration config, FSDataInputStream stream)
      throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
    List<String> lines = new ArrayList<>();
    String line;
    while ((line = reader.readLine()) != null) {
      lines.add(line);
    }
    return new Reader<List<String>>() {
      private boolean read = false;

      @Override
      public List<String> read() throws IOException {
        if (!read) {
          read = true;
          return lines;
        } else {
          return null; // End of file
        }
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }
    };
  }

  @Override
  public TypeInformation<List<String>> getProducedType() {
    return Types.LIST(Types.STRING);
  }
}
