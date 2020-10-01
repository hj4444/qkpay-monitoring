package com.qk365.qkpay.utils

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

object ApiEventProducer {
  def main(args: Array[String]): Unit = {
    val prop = new Properties
    prop.put("bootstrap.servers", "172.16.0.60:9092")
    prop.put("key.serializer", classOf[StringSerializer].getName)
    prop.put("value.serializer", classOf[StringSerializer].getName)
    val topic = "qkpay_kafka_3"
    val producer = new KafkaProducer[String, String](prop)
    for (i<-1 to 1) {
      val message = "{\"jsonContent\":\"232\"}"
      System.out.println(message)
      producer.send(new ProducerRecord[String, String](topic, message))
      Thread.sleep(1000)
    }

    producer.close()
  }

  def getCurrentTime: String = {
    val sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")
    sdf.format(new Date)
  }

  def getRandomIdCard: String = {
    "36232219910516241" + Random.nextInt(10)
  }

}
