package org.example.nessi;

import java.io.File;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {
  public static void main(String[] args) throws Exception {
    System.out.println("Hello world!");

    // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
    Configuration conf = new Configuration();
    conf.setString("env.log.level", "WARN");
    env.configure(conf);
    env.setParallelism(1);

    Path dirToWatch =
        Path.fromLocalFile(
            new File(
                //
                // "C:\\Users\\vze\\Documents\\JavaProgramme\\SourceFunction\\src\\main\\resources\\testDirectory"));
                "C:\\Users\\vze\\Documents\\JavaProgramme\\SourceFunction\\src\\main\\resources\\d"));
    /*
       final FileSource<String> source =
           FileSource.<String>forRecordStreamFormat(new TextLineInputFormat(), dirToWatch)
               .monitorContinuously(Duration.ofMinutes(1))
               .build();



       final KrasseFileSource<String> source =
           KrasseFileSource.<String>forRecordStreamFormat(new TextLineInputFormat(), dirToWatch)
               .monitorContinuously(Duration.ofMinutes(1))
               .build();

       DataStream<String> stream =
           env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

    */

    final KrasseFileSource<List<String>> listSource =
        KrasseFileSource.<List<String>>forRecordStreamFormat(new ListStringFormat(), dirToWatch)
            .monitorContinuously(Duration.ofMinutes(1))
            .build();
    /*
       final FileSource<List<String>> listSource =
           FileSource.forRecordStreamFormat(new ListStringFormat(), dirToWatch)
               .monitorContinuously(Duration.ofSeconds(30))
               .build();

    */

    DataStream<List<String>> stream =
        env.fromSource(listSource, WatermarkStrategy.noWatermarks(), "list-source");

    stream.print();

    env.execute();
  }
}
