package com.bugboy.cdc

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * 依赖：
 * <pre>
 *    <dependency>
 *       <groupId>com.alibaba.ververica</groupId>
 *       <artifactId>flink-connector-mysql-cdc</artifactId>
 *       <version>1.1.0</version>
 *    </dependency>
 * </pre>
 */
object FlinkCDCDemo {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tEnv = TableEnvironment.create(settings)
    val cdc_person =
      """
        |create table cdc_person
        |(
        |    id      string primary key,
        |    name    string,
        |    age     int,
        |    gender  string,
        |    address string,
        |    salary  decimal(20, 4)
        |)
        |with(
        | 'connector' = 'mysql-cdc',
        | 'hostname' = 'henghe66',
        | 'port' = '3306',
        | 'username' = 'root',
        | 'password' = 'root1234',
        | 'database-name' = 'flink_cdc',
        | 'table-name' = 'cdc_person'
        |)
        |""".stripMargin
    tEnv.executeSql(cdc_person)

    val cdc_person_print =
      """
        |create table cdc_person_print
        |(
        |    id      string primary key,
        |    name    string,
        |    age     int,
        |    gender  string,
        |    address string,
        |    salary  decimal(20, 4)
        |)
        |with(
        | 'connector' = 'print'
        |)
        |""".stripMargin
    tEnv.executeSql(cdc_person_print)

    val insert =
      """
        |insert into cdc_person_print
        |select * from cdc_person
        |""".stripMargin

    tEnv.executeSql(insert)
  }
}
