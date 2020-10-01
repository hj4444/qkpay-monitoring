package com.qk365.qkpay

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class TopHotFlatMap extends FlatMapFunction[ListBuffer[RequestViewCount], RequestViewCount] {
  override def flatMap(list: ListBuffer[RequestViewCount], collector: Collector[RequestViewCount]): Unit = {
    list.foreach(item => {
      collector.collect(item)
    })
  }
}
