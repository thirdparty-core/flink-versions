package com.bugboy.process

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 如果10s内输入的数字连续上升，则触发报警
 */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.socketTextStream("localhost", 7777)
      .map(_.toInt)
      .keyBy(_ => "0")
      .process(new WarningProcessFunction(10000))
      .print()
    env.execute("processFunction Test")
  }

  class WarningProcessFunction(interval: Long) extends KeyedProcessFunction[String, Int, String] {

    // 定义状态，存储上一次的输入
    // 定义状态，存储定时器的时间
    lazy val inputValueState: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("lastInput", classOf[Int]))
    lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", classOf[Long]))

    override def processElement(i: Int,
                                context: KeyedProcessFunction[String, Int,
                                  String]#Context, collector: Collector[String]): Unit = {
      // 取出上一次的输入以及定时器
      val lastInput = inputValueState.value()
      println(lastInput)
      val timer = timerState.value()
      // 将当前输入写入到状态中
      inputValueState.update(i)
      // 如果当前输出大于上一次输入，且没有定时器，则注册一个定时器
      if (i > lastInput && timer == 0) {
        // 获取当前的processTime
        val timerLine: Long = context.timerService().currentProcessingTime() + interval
        context.timerService().registerProcessingTimeTimer(timerLine) // 注意，这里使用的是ProcessingTimeTimer
        // 设置定时器状态
        timerState.update(timerLine)
        println("开启定时器")
      } else if (i < lastInput) {
        // 输入下降，则清空定时器
        context.timerService().deleteEventTimeTimer(timer)
        timerState.clear()
        println("清空定时器")
      }
    }

    // 触发定时器
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Int, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect(s"连续${interval}秒内输入上升！")
      // 同时清空定时器的状态
      timerState.clear()
    }
  }

}
