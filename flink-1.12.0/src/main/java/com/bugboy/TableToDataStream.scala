package com.bugboy

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object TableToDataStream {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tEnv = TableEnvironment.create(settings)
    //tEnv.fromTableSource(new FileSys)
  }
}
