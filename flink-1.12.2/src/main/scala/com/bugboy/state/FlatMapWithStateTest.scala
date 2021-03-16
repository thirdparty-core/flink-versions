package com.bugboy.state

import com.bugboy.projo.Person
import com.bugboy.source.PersonSourceFunction
import org.apache.flink.streaming.api.scala._

object FlatMapWithStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val pDs: DataStream[Person] = env.addSource(new PersonSourceFunction)
    val keyedStream: KeyedStream[Person, String] = pDs.keyBy(_ => "0")

    // mapWithState
    keyedStream.mapWithState[Person, Person] {
      case (p, None) => (p, Some(p))
      case (p, Some(lastP)) => if (p.age > lastP.age) {
        (p, Some(p))
      } else {
        (lastP, Some(p))
      }
    }.printToErr("result")

    // flatMapWithState
    // keyedStream.flatMapWithState(...)
    env.execute("flatMapWithState test")
  }
}
