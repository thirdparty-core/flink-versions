package com.bugboy

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object TableFileSource {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tEnv = TableEnvironment.create(settings)
    tEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
    tEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, java.time.Duration.ofSeconds(10))
    // TO_TIMESTAMP
    val demoSource =
      """
        |create table demo(
        |id string,
        |name string,
        |age int,
        |gender string,
        |address string,
        |createTime bigint,
        |ts AS TO_TIMESTAMP(FROM_UNIXTIME(createTime/1000)),
        |WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
        |)
        |with(
        |'connector' = 'filesystem',
        |'path' = 'file:///Users/lijiayan/bugboy/work-space/mvn/users/flink-versions/flink-1.12.0/files/demo-data',
        |'format' = 'json'
        |)
        |""".stripMargin
    tEnv.executeSql(demoSource)

    // 每两秒钟统计一次count
    val sql =
      """
        |select HOP_START(ts, INTERVAL '2' SECOND, INTERVAL '1' DAY) as startTime, count(*) as cnt from demo
        |group by HOP(ts, INTERVAL '2' SECOND, INTERVAL '1' DAY)
        |""".stripMargin
    tEnv.sqlQuery(sql).execute().print()
  }
}
