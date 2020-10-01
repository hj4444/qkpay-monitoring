package com.qk365.qkpay

import java.net.InetSocketAddress

import com.alibaba.fastjson.JSON
import com.qk365.qkpay.config._
import com.qk365.qkpay.utils.PathUtil
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.time.DateUtils
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object ApiRequestTop {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val config = ConfigFactory.load
    val kafkaProperties = propsFromConfig(config.getConfig("kafka"))

    env.setParallelism(1)

    env.enableCheckpointing(60 * 1000)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(15)))
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
    env.getCheckpointConfig.setCheckpointTimeout(30 * 1000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    val consumer = new FlinkKafkaConsumer[String](
      "qkpay_kafka_1",
      new SimpleStringSchema(),
      kafkaProperties
    )

    consumer.setStartFromLatest()
    val logEventStream = env.addSource(consumer)

    val mapData = logEventStream.map(log => {
      val logJsonObject = JSON.parseObject(log)
      val message = logJsonObject.getString("jsonContent")
      val jsonObject = JSON.parseObject(message)
      val dt = DateUtils.parseDate(jsonObject.getString("time"), "yyyy-MM-dd HH:mm:ss")
      val requestUrl = PathUtil.getRequestUrlAction(jsonObject.getString("requestUrl"))
      val userId = jsonObject.getString("userId").toLong
      val exchangePattern = jsonObject.getString("exchangePattern")
      ApiRequest(userId, requestUrl, exchangePattern, dt)
    }).filter(new LogEventFilter())

    val resultData = mapData.rebalance.assignTimestampsAndWatermarks(new LogEventWaterMarkExtractor())
      .keyBy("requestUrl")
      .timeWindow(Time.minutes(30), Time.minutes(10))
      .aggregate(new CountAgg, new WindowResultFunction)
      .keyBy("windowEnd")
      .process(new TopHotItems(3)).flatMap(new TopHotFlatMap())
    resultData.print
    /*val redisProperties = propsFromConfig(config.getConfig("redis"))
    val conf = new FlinkJedisPoolConfig.Builder().setHost(redisProperties.getProperty("host")).setPort(redisProperties.getProperty("port").toInt).setPassword(redisProperties.getProperty("password")).build()
    resultData.addSink(new RedisSink[Tuple3[String, String, Long]](conf, new LogRedisMapper))*/

    val esProperties = propsFromConfig(config.getConfig("elasticsearch"))
    val esConfig: java.util.Map[String, String] = new java.util.HashMap[String, String]
    esConfig.put("bulk.flush.max.actions", esProperties.getProperty("bulk.flush.max.actions"))
    esConfig.put("bulk.flush.max.size.mb", esProperties.getProperty("bulk.flush.max.size.mb"))
    esConfig.put("bulk.flush.interval.ms", esProperties.getProperty("bulk.flush.interval.ms"))
    esConfig.put("cluster.name", esProperties.getProperty("cluster.name"))
    val transports = new java.util.ArrayList[InetSocketAddress]()
    transports.add(new InetSocketAddress(esProperties.getProperty("host"), esProperties.getProperty("port").toInt))
    val esSink = new ElasticsearchSink(esConfig, transports, new LogElasticsearchSinkFunction)
    resultData.addSink(esSink)
    env.execute("qkpay-api-ranking-list")
  }


}
