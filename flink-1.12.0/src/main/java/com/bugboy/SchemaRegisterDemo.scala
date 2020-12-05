package com.bugboy

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object SchemaRegisterDemo {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tEnv = TableEnvironment.create(settings)

    // CREATE TABLE `order` (
    //  `id` int(11) NOT NULL AUTO_INCREMENT,
    //  `timestamps` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
    //  `orderInformationId` varchar(50) DEFAULT NULL,
    //  `userId` varchar(50) DEFAULT NULL,
    //  `categoryId` int(10) DEFAULT NULL,
    //  `productId` int(10) DEFAULT NULL,
    //  `price` decimal(10,2) DEFAULT NULL,
    //  `productCount` int(10) DEFAULT NULL,
    //  `priceSum` decimal(10,2) DEFAULT NULL,
    //  `shipAddress` varchar(50) DEFAULT NULL,
    //  `receiverAddress` varchar(50) DEFAULT NULL,
    //  PRIMARY KEY (`id`)
    //) ENGINE=InnoDB AUTO_INCREMENT=8208 DEFAULT CHARSET=utf8

    val kafkaAvroData =
      """
        |create table kafkaAvro(
        | `id` int,
        |`timestamps` timestamp(3) ,
        |`orderInformationId` string,
        |`userId` string,
        |`categoryId` int,
        |`productId` int,
        |`price` decimal(10,2),
        |`productCount` int,
        |`priceSum` decimal(10,2) ,
        |`shipAddress` string,
        |`receiverAddress` string
        |)
        |with(
        |'connector' = 'kafka',
        |'format' = 'debezium-avro-confluent', -- avro-confluent
        |'debezium-avro-confluent.schema-registry.url' = 'http://192.168.101.43:8081', -- avro-confluent.schema-registry.url
        |'topic' = 'yss.userAnalysis.order',
        |'properties.bootstrap.servers' = '192.168.101.42:9092',
        |'properties.group.id' = 'test',
        |'scan.startup.mode' = 'earliest-offset'
        |)
        |""".stripMargin
    tEnv.executeSql(kafkaAvroData)

   val esSink =
     """
       |create table esSink(
       |`id` int,
       |`timestamps` timestamp(3) ,
       |`orderInformationId` string,
       |`userId` string,
       |`categoryId` int,
       |`productId` int,
       |`price` decimal(10,2),
       |`productCount` int,
       |`priceSum` decimal(10,2) ,
       |`shipAddress` string,
       |`receiverAddress` string
       |)
       |with(
       |'connector' = 'elasticsearch-7',
       |'hosts' = 'http://henghe66:9200;http://henghe67:9200;http://henghe68:9200',
       |'index' = 'analysis_order'
       |)
       |""".stripMargin
    tEnv.executeSql(esSink)

    val sql =
      """
        |insert into esSink
        |select * from kafkaAvro
        |""".stripMargin

    // tEnv.executeSql(sql)

  }
}
