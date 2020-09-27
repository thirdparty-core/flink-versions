package com.bugboy.khkw.day27.topology.functions

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Random

class SideOutputProcessFunction[T](val outputTag: OutputTag[T]) extends ProcessFunction[T, T] {

  val log: Logger = LoggerFactory.getLogger(classOf[SideOutputProcessFunction[T]])
  val random = new Random()

  override def processElement(value: T, ctx: ProcessFunction[T, T]#Context, out: Collector[T]): Unit = {
    out.collect(value)
    if (random.nextInt() % 5 == 0) {
      log.warn("side-output...[%s]".format(value))
      ctx.output(outputTag, value)
    }
  }
}

