package com.bugboy

import com.bugboy.clz.Demo
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableResult
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment

object DataSetToTable {
  def main(args: Array[String]): Unit = {
    val bEnv = ExecutionEnvironment.getExecutionEnvironment
    val demoSet: DataSet[Demo] = bEnv.fromCollection(Seq(
      Demo("001", "张三", 21, "女", "北京"),
      Demo("002", "李四", 22, "男", "上海"),
      Demo("003", "王五", 23, "女", "广州"),
      Demo("004", "赵六", 24, "男", "深圳"),
      Demo("005", "朱琪", 25, "女", "重庆")
    ))

    val tEnv = BatchTableEnvironment.create(bEnv)

    tEnv.createTemporaryView("demo", demoSet)

    val sql =
      """
        |select * from demo
        |""".stripMargin

    val table: TableResult = tEnv.executeSql(sql)
    table.print()
  }
}
