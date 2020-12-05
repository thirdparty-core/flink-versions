package com.bugboy.sink

object FORMAT extends Enumeration {
  type FORMAT = Value
  val TSV: Value = Value(1)
  val CSV: Value = Value(2)
  val JSON: Value = Value(3)
}