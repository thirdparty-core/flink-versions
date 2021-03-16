package com.bugboy.data.mock

import scala.util.Random

object AgeMock {
  val random = new Random()

  def age(): Int = random.nextInt(100)
}
