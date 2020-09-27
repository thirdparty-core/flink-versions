package com.bugboy.khkw.day27.topology.functions

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class StateProcessFunction extends KeyedProcessFunction[String, (String, Int, Long), (String, Int, Long)] {

  val log: Logger = LoggerFactory.getLogger(classOf[StateProcessFunction])
  @transient var indexState: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    indexState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("indexState", classOf[Long]))
  }

  override def processElement(event: (String, Int, Long),
                              ctx: KeyedProcessFunction[String, (String, Int, Long), (String, Int, Long)]#Context,
                              out: Collector[(String, Int, Long)]): Unit = {
    var currentValue = indexState.value()
    if (currentValue == 0) {
      log.warn("Initialize when first run or failover...")
      currentValue = 0L
    }
    log.debug("Current Value [%d]".format(currentValue))
    indexState.update(currentValue + 1)
    out.collect((event._1, event._2, currentValue))
  }
}
