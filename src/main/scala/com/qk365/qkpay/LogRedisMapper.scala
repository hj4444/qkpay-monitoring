package com.qk365.qkpay

import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

class LogRedisMapper extends RedisMapper[Tuple3[String, String, Long]] {
  override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "qkPay-api-monitoring")

  override def getKeyFromData(t: Tuple3[String, String, Long]): String = t.f0 + t.f1

  override def getValueFromData(t: Tuple3[String, String, Long]): String = t.f2.toString
}
