package com.bugboy.sink

import java.io.{File, FileWriter, PrintWriter}

import com.bugboy.clz.Demo
import com.bugboy.sink.FORMAT.FORMAT
import com.google.gson.Gson
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class FileSink(format: FORMAT, path: String) extends RichSinkFunction[Demo] {
  lazy val file = new File(path)
  lazy val pw = new PrintWriter(new FileWriter(file), true)
  lazy val gson = new Gson()

  private[this] def line(value: Demo, delimiter: String = ","): String = {
    val sb = new StringBuilder()
    sb.append(value.id).append(delimiter)
      .append(value.name).append(delimiter)
      .append(value.gender).append(delimiter)
      .append(value.address).append(delimiter)
      .append(value.createTime)
    sb.toString()
  }

  private[this] def sinkAsTsv(value: Demo): Unit = {
    pw.println(line(value, "\t"))
    pw.flush()
  }

  private[this] def sinkAsCsv(value: Demo): Unit = {
    pw.println(line(value))
    pw.flush()
  }

  private[this] def sinkAsJson(value: Demo): Unit = {
    val json = gson.toJson(value)
    pw.println(json)
    pw.flush()
  }

  override def invoke(value: Demo, context: SinkFunction.Context): Unit = {
    format match {
      case FORMAT.CSV => sinkAsCsv(value)
      case FORMAT.TSV => sinkAsTsv(value)
      case FORMAT.JSON => sinkAsJson(value)
    }
  }

  override def close(): Unit = {
    pw.close()
  }
}
