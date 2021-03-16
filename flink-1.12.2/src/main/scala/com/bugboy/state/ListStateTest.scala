package com.bugboy.state

import com.bugboy.projo.Person
import com.bugboy.source.PersonSourceFunction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.scala._

object ListStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val pDs: DataStream[Person] = env.addSource(new PersonSourceFunction)
    pDs.keyBy(_ => "0").map(new RichMapFunction[Person, Person] {
      lazy val listStateDesc = new ListStateDescriptor[String]("liststate", classOf[String])
      lazy val listState: ListState[String] = getRuntimeContext.getListState(listStateDesc)

      override def map(in: Person): Person = {
        listState.add(in.id)
        listState.get().forEach(id => {
          print(id + "\t")
        })
        print("\n")
        in
      }
    }).printToErr("list state test")
    env.execute("ListState Test")
  }
}
