package com.bugboy

import com.bugboy.clz.Demo
import com.bugboy.sink.{FORMAT, FileSink}
import com.bugboy.source.DemoSource
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object DataStreamToFile {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val demoStream: DataStream[Demo] = env.addSource(new DemoSource())

    val fileSink = new FileSink(FORMAT.JSON, "flink-1.12.0/files/demo-data")
    demoStream.addSink(fileSink)
    env.execute(this.getClass.getSimpleName)
  }
}
