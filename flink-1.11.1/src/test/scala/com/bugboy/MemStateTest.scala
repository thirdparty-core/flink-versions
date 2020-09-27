package com.bugboy

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object MemStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(
      ("0001", "张三", 1),
      ("0002", "李四", 2),
      ("0003", "王五", 3),
      ("0004", "赵六", 4))

    ds.keyBy(_ => "1")
      .process(new KeyedProcessFunction[String, (String, String, Int), (String, String, Int)]() {

        var valueState: ValueState[Int] = _

        override def open(parameters: Configuration): Unit = {
          valueState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("indexSate", classOf[Int]))
        }

        override def processElement(value: (String, String, Int),
                                    ctx: KeyedProcessFunction[String, (String, String, Int), (String, String, Int)]#Context,
                                    out: Collector[(String, String, Int)]): Unit = {
          valueState.update(value._3)
          println("state:" + valueState.value())
          out.collect(value)
        }
      }).print()
    env.execute("MemStateTest")
  }
}
