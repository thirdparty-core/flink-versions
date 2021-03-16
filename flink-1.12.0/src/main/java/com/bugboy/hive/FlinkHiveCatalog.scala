package com.bugboy.hive

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object FlinkHiveCatalog {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance()
      .inBatchMode()
      .useBlinkPlanner()
      .build()

    val tEnv = TableEnvironment.create(settings)

    val catalog =
      """
        |CREATE CATALOG hive_catalog WITH (
        |  'type'='hive',
        |  'default-database' = 'default',
        |  'hive-conf-dir' = 'flink-1.12.0/src/main/resources'
        |)
        |""".stripMargin
    tEnv.executeSql(catalog)

    val sql =
      """
        |show tables
        |""".stripMargin
    tEnv.executeSql(sql).print()
  }
}
