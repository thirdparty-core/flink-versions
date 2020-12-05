package com.bugboy.source

import java.sql.{Date, Timestamp}
import java.util.concurrent.atomic.AtomicBoolean

import com.bugboy.clz.Demo
import org.apache.flink.streaming.api.functions.source.SourceFunction

class DemoSource extends SourceFunction[Demo] {
  val isRunning: AtomicBoolean = new AtomicBoolean(true)
  var initTime: Long = Timestamp.valueOf("2020-11-17 16:00:00").getTime

  override def run(sc: SourceFunction.SourceContext[Demo]): Unit = {
    var i = 0
    while (isRunning.get()) {
      val demo = Demo("%08d".format(i), "name%d".format(i), i % 100, if (i % 2 == 0) "男" else "女", "address%d".format(i), initTime)
      sc.collect(demo)
      i += 1
      initTime += 2000L
      if (i >= 100)
        isRunning.set(false)
    }
  }

  override def cancel(): Unit = {
    isRunning.set(false)
  }
}
