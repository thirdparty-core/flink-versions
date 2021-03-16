package com.bugboy.state

import com.bugboy.projo.Person
import com.bugboy.source.PersonSourceFunction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.scala._

object MapStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val pDs: DataStream[Person] = env.addSource(new PersonSourceFunction)
    pDs.keyBy(_ => "0").map(new RichMapFunction[Person, Person] {
      lazy val mapStateDesc = new MapStateDescriptor[String, Int]("mapstate", classOf[String], classOf[Int])
      lazy val mapState: MapState[String, Int] = getRuntimeContext.getMapState(mapStateDesc)

      override def map(in: Person): Person = {
        if (!mapState.contains(in.id)) {
          mapState.put(in.id, in.age)
        } else {
          mapState.put(in.id, mapState.get(in.id) + in.age)
        }

        mapState.entries().forEach(entry => {
          print(s"key:${entry.getKey},value:${entry.getValue} \t")
        })
        println()
        in
      }
    }).printToErr("result")
    env.execute("map state test")
  }
}
