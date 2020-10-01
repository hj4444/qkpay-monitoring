package com.qk365.qkpay

import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class WindowResultFunction extends WindowFunction[Long, RequestViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple,
                     window: TimeWindow,
                     aggregateResult: Iterable[Long],
                     out: Collector[RequestViewCount]): Unit = {
    val requestUrl = key.asInstanceOf[Tuple1[String]].f0
    val count = aggregateResult.iterator.next

    out.collect(RequestViewCount(requestUrl, window.getEnd, count))
  }
}