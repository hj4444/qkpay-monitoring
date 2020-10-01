package com.qk365.qkpay


import java.util.TimeZone

import com.alibaba.fastjson.JSON
import com.qk365.qkpay.config.propsFromConfig
import com.qk365.qkpay.utils.PathUtil
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.time.DateUtils
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
import org.apache.flink.types.Row


object ApiRequestTopSql {
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

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    val tableConfig = tableEnv.getConfig
    tableConfig.setLocalTimeZone(TimeZone.getTimeZone("Asia/Shanghai").toZoneId)
    tableConfig.setIdleStateRetentionTime(Time.hours(12), Time.hours(24))

    tableEnv.connect(new Kafka()
      .version("universal")
      .topic("qkpay_kafka_3").startFromEarliest()
      .property("zookeeper.connect", kafkaProperties.getProperty("zookeeper.connect"))
      .property("bootstrap.servers", kafkaProperties.getProperty("bootstrap.servers")))
      .withFormat(new Json())
      .withSchema(new Schema().field("jsonContent", DataTypes.STRING()))
      .inAppendMode().createTemporaryTable("api_request_source")
    val apiRequestSourceTable = tableEnv.from("api_request_source")

    val mapData = apiRequestSourceTable.toAppendStream[Row].map(log => {
      val logJsonObject = JSON.parseObject(log.toString)
      val message = logJsonObject.getString("jsonContent")
      val jsonObject = JSON.parseObject(message)
      val dt = DateUtils.parseDate(jsonObject.getString("time"), "yyyy-MM-dd HH:mm:ss")
      val requestUrl = PathUtil.getRequestUrlAction(jsonObject.getString("requestUrl"))
      val userId = jsonObject.getString("userId").toLong
      val exchangePattern = jsonObject.getString("exchangePattern")
      ApiRequest(userId, requestUrl, exchangePattern, dt)
    }).filter(new LogEventFilter())

    val ds = mapData.rebalance.assignTimestampsAndWatermarks(new LogEventWaterMarkExtractor())
    tableEnv.createTemporaryView("api_request_hot", ds, 'userId, 'requestUrl, 'exchangePattern, 'dt, 'rowtime.rowtime)
    tableEnv.sqlQuery("select HOP_END(rowtime, INTERVAL '10' minute, INTERVAL '30' minute) as end_window,requestUrl, count(requestUrl) as total from api_request_hot group by HOP(rowtime, INTERVAL '10' minute, INTERVAL '30' minute), requestUrl")
      .toRetractStream(TypeInformation.of(classOf[Row])).print()
    tableEnv.execute("qkpay-api-ranking-list")
  }
}
