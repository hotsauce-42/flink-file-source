package org.example.filesource;

import java.io.File;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {
  public static void main(String[] args) throws Exception {
    // System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "off");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(5000);

    Path dirToWatch = Path.fromLocalFile(new File("src/main/resources/testDirectory"));

    final OwnFileSource<List<String>> source =
        OwnFileSource.forRecordStreamFormat(new ListStringInputFormat(), dirToWatch)
            .monitorContinuously(Duration.ofSeconds(1))
            .build();

    DataStream<List<String>> stream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "source");

    stream.print();

    env.execute();
  }
}
