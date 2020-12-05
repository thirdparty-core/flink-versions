package com.bugboy

import com.bugboy.clz.Demo
import com.bugboy.source.DemoSource
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object DataStreamToTable {
  def main(args: Array[String]): Unit = {
    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tEnv = StreamTableEnvironment.create(sEnv, settings)
    val demoStream: DataStream[Demo] = sEnv.addSource(new DemoSource())
    tEnv.createTemporaryView("demo", demoStream)

    val sql =
      """
        |select * from demo
        |""".stripMargin

    tEnv.executeSql(sql).print()

    sEnv.execute(this.getClass.getSimpleName)
  }

}
