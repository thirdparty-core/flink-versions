package com.bugboy.analysis

import java.time.Duration

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * 1、地区消费能力TopN
 * ----
 * 2、学历购物爱好TopN
 * 3、热门商品TOPN
 * 4、消费总金额
 */
object AnalysisCase {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tEnv = TableEnvironment.create(settings)
    val tableCfg = tEnv.getConfig.getConfiguration
    tableCfg.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
    tableCfg.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10))
    tableCfg.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, java.lang.Boolean.valueOf(true))
    tableCfg.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(2))
    tableCfg.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, java.lang.Long.valueOf(10000L))

    val sql1 =
      """
        |create table if not exists ods_order(
        |id int PRIMARY KEY comment '订单id',
        |timestamps bigint comment '订单创建时间',
        |orderInformationId string comment '订单信息ID',
        |userId string comment '用户ID',
        |categoryId int comment '商品类别',
        |productId int comment '商品ID',
        |price decimal(10,2) comment '单价',
        |productCount int comment '购买数量',
        |priceSum decimal(10,2) comment '订单总价',
        |shipAddress string comment '商家地址',
        |receiverAddress string comment '收货地址'
        |--ts AS TO_TIMESTAMP(FROM_UNIXTIME(timestamps/1000)),
        |--WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
        |)
        |with(
        |'connector' = 'kafka',
        |'format' = 'debezium-avro-confluent', -- avro-confluent
        |'debezium-avro-confluent.schema-registry.url' = 'http://192.168.101.43:8081', -- avro-confluent.schema-registry.url
        |'topic' = 'ods.userAnalysis.order',
        |'properties.bootstrap.servers' = '192.168.101.42:9092',
        |'properties.group.id' = 'flink-analysis',
        |'scan.startup.mode' = 'earliest-offset' --earliest-offset   latest-offset
        |)
        |""".stripMargin

    tEnv.executeSql(sql1)

    val sql2 =
      """
        |create table if not exists ods_evaluate(
        |id int PRIMARY KEY comment 'ID',
        |userId string comment '用户ID',
        |productId int comment '商品ID',
        |described int comment '评分',
        |`comment` string comment '评价',
        |logisticsAttitude int comment '物流评分',
        |serviceAttitude int comment '服务评分',
        |merchantId string comment '商家ID',
        |orderInformationId string comment '订单ID'
        |-- ts AS PROCTIME()
        |)
        |with(
        |'connector' = 'kafka',
        |'format' = 'debezium-avro-confluent', -- avro-confluent
        |'debezium-avro-confluent.schema-registry.url' = 'http://192.168.101.43:8081', -- avro-confluent.schema-registry.url
        |'topic' = 'ods.userAnalysis.evaluate',
        |'properties.bootstrap.servers' = '192.168.101.42:9092',
        |'properties.group.id' = 'flink-analysis',
        |'scan.startup.mode' = 'earliest-offset' --latest-offset   earliest-offset
        |)
        |""".stripMargin
    tEnv.executeSql(sql2)
    val sq3 =
      """
        |create table if not exists dwd_paid_order_detail(
        |id int  comment 'ID',
        |userId string comment '用户ID',
        |described int comment '评分',
        |evaluate string comment '评价',
        |logisticsAttitude int comment '物流评分',
        |serviceAttitude int comment '服务评分',
        |merchantId string comment '商家ID',
        |timestamps bigint comment '订单创建时间',
        |orderInformationId string comment '订单信息ID',
        |categoryId int comment '商品类别',
        |productId int comment '商品ID',
        |price decimal(10,2) comment '单价',
        |productCount int comment '购买数量',
        |priceSum decimal(10,2) comment '订单总价',
        |shipAddress string comment '商家地址',
        |receiverAddress string comment '收货地址'
        |)
        |with(
        |'connector' = 'kafka',
        |'format' = 'changelog-json',
        |'topic' = 'dwd_paid_order_detail',
        |'properties.bootstrap.servers' = 'henghe66:9092',
        |'properties.group.id' = 'flink-analysis',
        |'sink.partitioner' = 'fixed'
        |)
        |""".stripMargin
    tEnv.executeSql(sq3)

    val sql4 =
      """
        |insert into dwd_paid_order_detail
        |select
        |o.id
        |,e.userId
        |,e.described
        |,e.`comment` as evaluate
        |,e.logisticsAttitude
        |,e.serviceAttitude
        |,e.merchantId
        |,o.timestamps
        |,o.orderInformationId
        |,o.categoryId
        |,o.productId
        |,o.price
        |,o.productCount
        |,o.priceSum
        |,o.shipAddress
        |,o.receiverAddress
        |from
        |ods_order as o inner join ods_evaluate e
        |on o.orderInformationId=e.orderInformationId
        |""".stripMargin
   tEnv.executeSql(sql4)

    val sql =
      """
        |create table if not exists dwd_paid_order_detail1(
        |id int  comment 'ID',
        |userId string comment '用户ID',
        |described int comment '评分',
        |evaluate string comment '评价',
        |logisticsAttitude int comment '物流评分',
        |serviceAttitude int comment '服务评分',
        |merchantId string comment '商家ID',
        |timestamps bigint comment '订单创建时间',
        |orderInformationId string comment '订单信息ID',
        |categoryId int comment '商品类别',
        |productId int comment '商品ID',
        |price decimal(10,2) comment '单价',
        |productCount int comment '购买数量',
        |priceSum decimal(10,2) comment '订单总价',
        |shipAddress string comment '商家地址',
        |receiverAddress string comment '收货地址'
        |)
        |with(
        |'connector' = 'kafka',
        |'format' = 'changelog-json',
        |'topic' = 'dwd_paid_order_detail',
        |'properties.bootstrap.servers' = 'henghe66:9092',
        |'properties.group.id' = 'flink-analysis'
        |)
        |""".stripMargin
  tEnv.executeSql(sql)
    val ss =
      """
        |select * from dwd_paid_order_detail1
        |""".stripMargin
    tEnv.executeSql(ss).print()
  }
}
