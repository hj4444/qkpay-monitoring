package com.qk365.qkpay

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
import org.apache.flink.types.Row
object TableConnectKafka {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    tableEnv.connect(new Kafka()
      .version("universal")
      .topic("qkpay_kafka_3")
      .property("zookeeper.connect", "172.16.0.60:2181")
      .property("bootstrap.servers", "172.16.0.60:9092")
    )
      .withFormat(new Json())
      .withSchema(new Schema()
        .field("jsonContent", DataTypes.STRING())
      )
      .createTemporaryTable("kafkaInputTable")

    //tableEnv.sqlQuery("select * from kafkaInputTable").toAppendStream[Row].print()
    tableEnv.from("kafkaInputTable").toAppendStream[Row].print()

    env.execute()
  }
}
