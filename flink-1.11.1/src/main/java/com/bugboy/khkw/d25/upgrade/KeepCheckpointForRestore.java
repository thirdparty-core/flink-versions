package com.bugboy.khkw.d25.upgrade;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KeepCheckpointForRestore {
  public static void main(String[] args) throws Exception {
    Logger log = LoggerFactory.getLogger(KeepCheckpointForRestore.class);
    StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(2, TimeUnit.SECONDS)));
    env.enableCheckpointing(20);

    env.setStateBackend(new FsStateBackend("file:///tmp/chkDir", false));
    env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    DataStreamSource<Tuple3<String, Integer, Long>> source = env.addSource(new SourceFunction<Tuple3<String, Integer, Long>>() {
      private AtomicBoolean isRunning = new AtomicBoolean(true);

      @Override
      public void run(SourceContext<Tuple3<String, Integer, Long>> ctx) throws Exception {
        int index = 1;
        while (isRunning.get()) {
          ctx.collect(new Tuple3<>("key", index++, System.currentTimeMillis()));
          Thread.sleep(100);
        }
      }

      @Override
      public void cancel() {
        isRunning.set(false);
      }
    });
    source.map((MapFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, String>>) event -> {
      if (event.f1 % 10 == 0) {
        String msg = String.format("Bad data[%d]...", event.f1);
        log.error(msg);
        throw new RuntimeException(msg);
      }
      return new Tuple3<>(event.f0, event.f1, new Timestamp(event.f2).toString());
    })
            .returns(new TypeHint<Tuple3<String, Integer, String>>() {
            })
            .keyBy(t3 -> t3.f0)
            .sum(1)
            .print();

    env.execute("KeepCheckpointForRestore");
  }
}
