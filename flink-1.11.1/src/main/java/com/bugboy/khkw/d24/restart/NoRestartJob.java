package com.bugboy.khkw.d24.restart;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.atomic.AtomicBoolean;

public class NoRestartJob {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.setRestartStrategy(RestartStrategies.noRestart());

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
      if (t3.f1 % 100 == 0) {
        String msg = String.format("Bad data [%d]...", t3.f1);
        throw new RuntimeException(msg);
      }
      return new Tuple2<>(t3.f0, t3.f1);
    }).returns(new TypeHint<Tuple2<String, Integer>>() {
    }.getTypeInfo())
            .print();

    env.execute();
  }
}
