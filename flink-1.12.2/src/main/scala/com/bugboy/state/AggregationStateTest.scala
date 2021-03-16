package com.bugboy.state

import com.bugboy.projo.Person
import com.bugboy.source.PersonSourceFunction
import org.apache.flink.api.common.functions.{AggregateFunction, RichMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor}
import org.apache.flink.streaming.api.scala._

object AggregationStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val pDs: DataStream[Person] = env.addSource(new PersonSourceFunction)
    pDs.keyBy(_ => "0")
      .map(new RichMapFunction[Person, Person] {
        lazy val aggStateDesc = new AggregatingStateDescriptor[Int, (Int, Int), (Int, Int)]("aggState",
          new AggregateFunction[Int, (Int, Int), (Int, Int)] {
            override def createAccumulator(): (Int, Int) = {
              (0, 0)
            }

            override def add(in: Int, acc: (Int, Int)): (Int, Int) = {
              val min = scala.math.min(in, acc._1)
              val max = scala.math.max(in, acc._2)
              (min, max)
            }

            override def getResult(acc: (Int, Int)): (Int, Int) = {
              acc
            }

            override def merge(acc: (Int, Int), acc1: (Int, Int)): (Int, Int) = {
              val min = scala.math.min(acc._1, acc1._1)
              val max = scala.math.max(acc._2, acc1._2)
              (min, max)
            }
          }, classOf[(Int, Int)])
        lazy val aggState: AggregatingState[Int, (Int, Int)] = getRuntimeContext.getAggregatingState(aggStateDesc)

        override def map(in: Person): Person = {
          aggState.add(in.age)
          println(s"min age:${aggState.get()._1}, max age:${aggState.get()._2}")
          in
        }
      }).printToErr("reslut")
    env.execute("aggregation state")
  }
}
