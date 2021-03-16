package com.bugboy.state

import com.bugboy.projo.Person
import com.bugboy.source.PersonSourceFunction
import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.streaming.api.scala._

object ReduceStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val pDs: DataStream[Person] = env.addSource(new PersonSourceFunction)
    pDs.keyBy(_ => "0")
      .map(new RichMapFunction[Person, Person] {
        lazy val reducingStateDesc = new ReducingStateDescriptor[Int]("reduceState", new ReduceFunction[Int] {
          override def reduce(t: Int, t1: Int): Int = {
            scala.math.max(t, t1)
          }
        }, classOf[Int])
        lazy val reduceState: ReducingState[Int] = getRuntimeContext.getReducingState(reducingStateDesc)

        override def map(in: Person): Person = {
          reduceState.add(in.age)
          println(reduceState.get())
          in
        }
      }).printToErr("result")
    env.execute("reduce state test")
  }
}
