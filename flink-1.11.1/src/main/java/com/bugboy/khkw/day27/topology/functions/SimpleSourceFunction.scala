package com.bugboy.khkw.day27.topology.functions

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.flink.streaming.api.functions.source.SourceFunction

class SimpleSourceFunction extends SourceFunction[(String, Int, Long)] {
  val isRunning = new AtomicBoolean(true)

  override def run(ctx: SourceFunction.SourceContext[(String, Int, Long)]): Unit = {
    var index = 1
    while (isRunning.get()) {
      index += 1
      ctx.collect(("key1", index, System.currentTimeMillis()))
      ctx.collect(("key2", index, System.currentTimeMillis()))
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning.set(false)
  }
}
