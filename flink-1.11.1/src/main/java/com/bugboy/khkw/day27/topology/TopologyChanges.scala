package com.bugboy.khkw.day27.topology

import com.bugboy.khkw.day27.topology.functions.{SideOutputProcessFunction, SimpleSourceFunction, StateProcessFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TopologyChanges {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(20)
    // version1(env)
    version2(env)
    env.execute("TopologyChanges")
  }

  def version1(env: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.streaming.api.scala._
    val outputTag = new OutputTag[(String, Int, Long)]("side-output")
    val mainResult = env.addSource(new SimpleSourceFunction())
      .keyBy(tup3 => tup3._1)
      .sum(1)
      .process(new SideOutputProcessFunction[(String, Int, Long)](outputTag))
    mainResult.print().name("main-result")
    mainResult.getSideOutput(outputTag).print().name("side-output")
  }

  def version2(env: StreamExecutionEnvironment): Unit = {
    env.enableCheckpointing(20)
    import org.apache.flink.streaming.api.scala._
    val mainResult = env.addSource(new SimpleSourceFunction())
      .keyBy(tup3 => tup3._1)
      .process(new StateProcessFunction())
      .keyBy(tup3 => tup3._1)
      .sum(2)
    mainResult.print.name("main-result")
  }
}
