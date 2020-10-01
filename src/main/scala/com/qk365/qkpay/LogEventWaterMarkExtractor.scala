package com.qk365.qkpay

import java.util.Date

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class LogEventWaterMarkExtractor extends AssignerWithPeriodicWatermarks[(ApiRequest)] with  Serializable {
  var currentMaxTimestamp = 0L
  var maxOutOfOrderness = 10000L

  override def getCurrentWatermark: Watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)

  override def extractTimestamp(element: (ApiRequest), previousElementTimestamp: Long): Long = {
    val timestamp = element.dt.getTime
    val myDate = new Date
    myDate.setTime(timestamp)
    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
    timestamp
  }
}
