package com.bugboy.state

import com.bugboy.projo.Person
import com.bugboy.source.PersonSourceFunction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._

object ValueStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val pDs: DataStream[Person] = env.addSource(new PersonSourceFunction)
    pDs.keyBy(_.id).map(new RichMapFunction[Person, Person] {
      val valueStateDscr = new ValueStateDescriptor[Int]("valuestate", classOf[Int])
      lazy val valueState: ValueState[Int] = getRuntimeContext.getState(valueStateDscr)

      override def map(in: Person): Person = {
        if (valueState == null) {
          println("no state")
        } else {
          println(s"state:${in.age}")
        }
        valueState.update(in.age)
        in
      }
    }).printToErr("result")
    env.execute("ValueState Test")
  }
}
