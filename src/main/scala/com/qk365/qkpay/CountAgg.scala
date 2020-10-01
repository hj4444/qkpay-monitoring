package com.qk365.qkpay

import org.apache.flink.api.common.functions.AggregateFunction

class CountAgg extends AggregateFunction[ApiRequest, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: ApiRequest, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
}
