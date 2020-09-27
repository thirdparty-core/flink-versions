package com.bugboy.khkw.d25.upgrade;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 项目名称: Apache Flink 知其然，知其所以然 - upgrade
 * 功能描述: 演示开启Checkpoint之后,failover之后可以从失败之前的状态进行续跑。
 * 操作步骤:
 *        0. 修改 pom 文件的依赖配置，增加 <scope>provided</scope>
 *        1. mvn 打包
 *        2.下载flink发布包https://www.apache.org/dyn/closer.lua/flink/flink-1.10.1/flink-1.10.1-bin-scala_2.11.tgz
 *        3. 配置 flink-cong.yaml
 *          配置statebackend
 *          - state.backend: filesystem
 *          配置checkpoint&savepoint
 *          - state.checkpoints.dir: file:///tmp/chkdir
 *          - state.savepoints.dir: file:///tmp/chkdir
 *          配置失败重启策略
 *          - restart-strategy: fixed-delay
 *          - restart-strategy.fixed-delay.attempts: 3
 *          - restart-strategy.fixed-delay.delay: 2 s
 *          配置checkpoint保存个数
 *          - state.checkpoints.num-retained: 2
 *          配置local recovery for this state backend
 *          - state.backend.local-recovery: true
 *
 *        4. bin/start-cluster.sh local
 *        5. bin/flink run -m localhost:8081 -c com.bugboy.khkw.d25.upgrade.SavepointForRestore /Users/lijiayan/bugboy/work-space/mvn/users/flink-versions/flink-1.11.1/target/flink-1.11.1-1.0.jar
 *        6. bin/flink run -m localhost:8081 -s file:///tmp/flink-1.11.1/chkdir/ed8b8f1fe6738e6a6994915ff1ca78ca
 *        -c com.bugboy.khkw.d25.upgrade.SavepointForRestore /Users/lijiayan/bugboy/work-space/mvn/users/flink-versions/flink-1.11.1/target/flink-1.11.1-1.0.jar
 *
 *        7. 将程序去除异常，运行之后，触发savepoint
 */
public class SavepointForRestore {
  public static void main(String[] args) throws Exception {
    Logger log = LoggerFactory.getLogger(SavepointForRestore.class);
    StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(10);
    env.getCheckpointConfig()
            .enableExternalizedCheckpoints(
                    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
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
    source.map(tup3 -> {
      /*if (tup3.f1 % 10 == 0) {
        String msg = String.format("Bad data [%d]...", tup3.f1);
        log.error(msg);
        throw new RuntimeException(msg);
      }*/
      return new Tuple3<>(tup3.f0, tup3.f1, new Timestamp(tup3.f2).toString());
    }).returns(new TypeHint<Tuple3<String, Integer, String>>() {
    }).keyBy(tu -> tu.f0)
            .sum(1)
            .print();
    env.execute("SavepointForRestore");
  }
}
