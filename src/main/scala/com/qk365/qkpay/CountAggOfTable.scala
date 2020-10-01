package com.qk365.qkpay

import org.apache.flink.table.functions.AggregateFunction

class CountAggOfTable extends AggregateFunction[Long, CountAccum] {
  override def getValue(acc: CountAccum): Long = acc.total

  override def createAccumulator(): CountAccum = CountAccum()

  def accumulate(accumulator: CountAccum, dt: String, uuidStr: String): Unit = accumulator.total += 1

  def merge(accumulator: CountAccum, iter: Iterable[CountAccum]): Unit = {
    val it = iter.iterator
    while (it.hasNext) {
      val accum = it.next()
      accumulator.total += accum.total
    }
  }

  def reSet(accumulator: CountAccum): CountAccum = {
    accumulator.total = 0L
    accumulator
  }
}

case class CountAccum(var total: Long = 0L)