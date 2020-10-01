package com.qk365.qkpay

import java.util.Properties

import com.typesafe.config._

import scala.collection._

package object config {

  def propsFromConfig(cfg: Config): Properties = {
    import scala.collection.JavaConverters._
    val props = new Properties()
    val map: Map[String, Object] = cfg.entrySet().asScala.map({ entry =>
      entry.getKey -> entry.getValue.unwrapped()
    })(collection.breakOut)

    props.putAll(map.asJava)
    props
  }


  //not thread-safe
  private object UniqueId {
    private var currentId = 0

    def next: Int = {
      currentId += 1
      currentId
    }
  }

  implicit class propsWithClientId(props: Properties) {
    def withClientId(prefix: String): Properties = {
      val id = s"$prefix-${UniqueId.next}"
      val newProps = props.clone().asInstanceOf[Properties]
      newProps.put("client.id", id)
      newProps
    }
  }
}
