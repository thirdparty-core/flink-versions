package com.bugboy.khkw.d24.restart;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicBoolean;

public class EnableCheckpointRestartJob {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    // env.setRestartStrategy(RestartStrategies.noRestart());
    // 1. 不设置重启策略并开启chk，会默认无限重启
    env.enableCheckpointing(2000);

    DataStream<Tuple3<String, Integer, Long>> source = env.addSource(new SourceFunction<Tuple3<String, Integer, Long>>() {
      private AtomicBoolean isRunning = new AtomicBoolean(true);

      @Override
      public void run(SourceContext<Tuple3<String, Integer, Long>> sc) throws Exception {
        int index = 1;
        while (isRunning.get()) {
          sc.collect(new Tuple3<>("key", index++, System.currentTimeMillis()));
          Thread.sleep(100);
        }
      }

      @Override
      public void cancel() {
        isRunning.set(false);
      }
    });
    source.map(t3 -> {
      if (t3.f1 % 10 == 0) {
        String msg = String.format("Bad data [%d]...", t3.f1);
        throw new RuntimeException(msg);
      }
      return new Tuple3<>(t3.f0, t3.f1, new Timestamp(t3.f2).toString());
    }).returns(new TypeHint<Tuple3<String, Integer, String>>() {
    }.getTypeInfo())
            .print();

    env.execute();
  }
}
