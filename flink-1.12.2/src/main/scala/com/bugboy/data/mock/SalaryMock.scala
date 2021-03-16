package com.bugboy.data.mock

import scala.util.Random

object SalaryMock {
  val random = new Random()
  val BASE_SALARY: java.math.BigDecimal = java.math.BigDecimal.valueOf(3000)
  var FIX: Int = 1000000

  def salary(): java.math.BigDecimal = {
    BASE_SALARY.add(new java.math.BigDecimal(random.nextInt(FIX) / 100.00))
      .setScale(2, java.math.BigDecimal.ROUND_HALF_UP)
  }
}
