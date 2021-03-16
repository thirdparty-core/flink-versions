package com.bugboy.hive

import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
 * 依赖：
 * <!-- Flink Dependency -->
 * <dependency>
 * <groupId>org.apache.flink</groupId>
 * <artifactId>flink-connector-hive_2.11</artifactId>
 * <version>1.11.2</version>
 * <scope>provided</scope>
 * </dependency>
 * *
 * <dependency>
 * <groupId>org.apache.flink</groupId>
 * <artifactId>flink-table-api-java-bridge_2.11</artifactId>
 * <version>1.11.2</version>
 * <scope>provided</scope>
 * </dependency>
 * *
 * <!-- Hive Dependency -->
 * <dependency>
 * <groupId>org.apache.hive</groupId>
 * <artifactId>hive-exec</artifactId>
 * <version>${hive.version}</version>
 * <scope>provided</scope>
 * </dependency>
 *
 * <dependency>
 * <groupId>org.apache.flink</groupId>
 * <artifactId>flink-connector-hive_2.12</artifactId>
 * <version>1.11.2</version>
 * <scope>provided</scope>
 * </dependency>
 */
object FlinkUseHiveCatalog {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance()
      .inBatchMode()
      .useBlinkPlanner()
      .build()
    val tEnv = TableEnvironment.create(settings)

    val catalogName = "hive"
    val database = "default"
    val hiveVersion = "3.1.2"
    val hiveConfigDir = "flink-1.12.0/src/main/resources"
    val hiveCatalog = new HiveCatalog(catalogName, database, hiveConfigDir, hiveVersion)

    tEnv.registerCatalog(catalogName, hiveCatalog)

    tEnv.useCatalog(catalogName)
    tEnv.useDatabase(database)

    tEnv.executeSql("show tables").print()

    val  sql =
      """
        |select * from student_tb_txt
        |""".stripMargin

    tEnv.executeSql(sql).print()
  }
}
