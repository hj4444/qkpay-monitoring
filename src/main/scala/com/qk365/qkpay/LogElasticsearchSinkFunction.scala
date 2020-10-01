package com.qk365.qkpay

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

class LogElasticsearchSinkFunction extends ElasticsearchSinkFunction[RequestViewCount] {
  override def process(t: RequestViewCount, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    requestIndexer.add(createIndexRequest(t))
  }

  def createIndexRequest(element: RequestViewCount): IndexRequest = {
    val json = new util.HashMap[String, Any]
    json.put("time", element.windowEnd)
    json.put("id", element.requestUrl)
    json.put("count", element.count)
    val id = element.windowEnd + ":" + element.count
    //println(element)
    Requests.indexRequest.index("qkpay").`type`("qkpaymonitoring").id(id).source(json)
  }
}
