package com.bugboy.source

import java.sql.Timestamp

import com.bugboy.data.mock.{AgeMock, NameMock, SalaryMock}
import com.bugboy.projo.Person
import org.apache.flink.streaming.api.functions.source.SourceFunction

class PersonSourceFunction extends SourceFunction[Person] {
  var isRunning: Boolean = true
  var id: Long = 0

  override def run(ctx: SourceFunction.SourceContext[Person]): Unit = {
    while (isRunning) {
      ctx.collect(buildPersonById(id))
      id += 1
      Thread.sleep(1000)
    }
  }

  def buildPersonById(id: Long): Person = {
    val pid = "%06d".format(id)
    Person(pid, NameMock.name(), AgeMock.age(), SalaryMock.salary(), new Timestamp(System.currentTimeMillis()))
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
