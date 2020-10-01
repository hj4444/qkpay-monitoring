package com.qk365.qkpay

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class TopHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, RequestViewCount, ListBuffer[RequestViewCount]] {
  private var itemState: ListState[RequestViewCount] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val itemsStateDesc = new ListStateDescriptor[RequestViewCount]("ApiState-state", classOf[RequestViewCount])

    itemState = getRuntimeContext.getListState(itemsStateDesc)
  }

  override def processElement(input: RequestViewCount,
                              context: KeyedProcessFunction[Tuple, RequestViewCount, ListBuffer[RequestViewCount]]#Context,
                              collector: Collector[ListBuffer[RequestViewCount]]): Unit = {

    itemState.add(input)
    context.timerService().registerEventTimeTimer(input.windowEnd + 1)
  }


  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, RequestViewCount, ListBuffer[RequestViewCount]]#OnTimerContext, out: Collector[ListBuffer[RequestViewCount]]): Unit = {
    val allItems: ListBuffer[RequestViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itemState.get) {
      allItems += item
    }

    itemState.clear()

    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    out.collect(sortedItems)
  }
}
