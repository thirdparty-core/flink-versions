package com.bugboy.analysis

import java.sql.Date
import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.formats.avro.registry.confluent.debezium.DebeziumAvroDeserializationSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.{DataTypes, TableSchema}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.RowKind
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.net.InetAddress
import java.net.InetSocketAddress
import java.util

import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
 * 消费总金额
 */
object TotalCost {
  val TOPIC: String = "ods.userAnalysis.order"
  val SCHEMA_REGIST_URL: String = "http://192.168.101.43:8081"
  val BOOTSTRAP_SERVERS: String = "henghe-042:9092,henghe-043:9092,henghe-044:9092"
  val GROUP_ID: String = "flink-analysis"
  val ES_INDEX: String = "tmp_total_money"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(200L)
    env.getCheckpointConfig.setCheckpointInterval(1000L)
    val schema = TableSchema.builder()
      .field("id", DataTypes.INT)
      .field("timestamps", DataTypes.BIGINT())
      .field("orderInformationId", DataTypes.STRING())
      .field("userId", DataTypes.STRING())
      .field("categoryId", DataTypes.INT())
      .field("productId", DataTypes.INT())
      .field("price", DataTypes.DECIMAL(10, 2))
      .field("productCount", DataTypes.INT())
      .field("priceSum", DataTypes.DECIMAL(10, 2))
      .field("shipAddress", DataTypes.STRING())
      .field("receiverAddress", DataTypes.STRING())
      .build()
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID)

    val rowType = schema.toRowDataType.getLogicalType.asInstanceOf[RowType]
    val deserialization: DebeziumAvroDeserializationSchema = new DebeziumAvroDeserializationSchema(rowType, InternalTypeInfo.of(rowType), SCHEMA_REGIST_URL)
    val kafkaConsumerSource = new FlinkKafkaConsumer(TOPIC, deserialization, props)
      .setStartFromEarliest()
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[RowData] {
        val maxOutOfOrderness = 1000L
        var currentMaxTime: Long = 0L

        override def extractTimestamp(t: RowData, l: Long): Long = {
          val currentTime = t.getLong(1)
          currentMaxTime = scala.math.max(currentMaxTime, currentTime)
          currentTime
        }

        override def getCurrentWatermark: Watermark = {
          new Watermark(currentMaxTime - maxOutOfOrderness)
        }
      })
    val sumDs = env.addSource(kafkaConsumerSource)
      .windowAll(SlidingEventTimeWindows.of(Time.minutes(1), Time.minutes(1)))
      .process(new ProcessAllWindowFunction[RowData, (String, BigDecimal), TimeWindow]() {
        var sum: BigDecimal = BigDecimal(0)
        val FORMAT: String = "yyyy-MM-dd HH:mm"

        def formatTime(time: Long): String = {
          new SimpleDateFormat(FORMAT).format(new Date(time))
        }

        override def process(context: Context, elements: Iterable[RowData], out: Collector[(String, BigDecimal)]): Unit = {
          elements.foreach(row => {
            val priceSum = row.getDecimal(8, 10, 2).toBigDecimal
            row.getRowKind match {
              case RowKind.INSERT => sum = sum + priceSum
              case RowKind.UPDATE_BEFORE => sum = sum - priceSum
              case RowKind.UPDATE_AFTER => sum = sum + priceSum
              case RowKind.DELETE => sum = sum - priceSum
            }
          })
          val windowEnd = context.window.getEnd
          out.collect((formatTime(windowEnd), sum))
        }
      })
    val config = new util.HashMap[String, String]()
    config.put("cluster.name", "henghe")
    // This instructs the sink to emit after every element, otherwise they would be buffered
    config.put("bulk.flush.max.actions", "1")

    val transportAddresses = new util.ArrayList[HttpHost]
    transportAddresses.add(new HttpHost("henghe66", 9200))
    transportAddresses.add(new HttpHost("henghe67", 9200))
    transportAddresses.add(new HttpHost("henghe68", 9200))

    val esSinkFunction: ElasticsearchSinkFunction[(String, BigDecimal)] = new ElasticsearchSinkFunction[(String, BigDecimal)]() {

      def createIndex(element: (String, BigDecimal)): IndexRequest = {
        val json = new util.HashMap[String, Object]()
        json.put("timeline", element._1)
        json.put("t_money", element._2.bigDecimal)
        Requests.indexRequest(ES_INDEX)
          .source(json)
      }

      override def process(t: (String, BigDecimal), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        requestIndexer.add(createIndex(t))
      }
    }
    val esSink = new ElasticsearchSink.Builder(transportAddresses, esSinkFunction).build()
    sumDs.addSink(esSink)
    env.execute()
  }
}
