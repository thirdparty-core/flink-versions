package com.bugboy.projo

import java.sql.Timestamp

case class Person(id: String, name: String, age: Int, salary: java.math.BigDecimal, createTime: Timestamp) {
  override def toString: String = {
    s"""{"id":"$id","name":"$name","age":$age,"salary":$salary,"createTime":"${createTime.toString}"}""".stripMargin
  }
}
