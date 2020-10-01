package com.qk365.qkpay

import org.apache.flink.api.common.functions.FilterFunction

class LogEventFilter extends FilterFunction[(ApiRequest)] {
  override def filter(t: (ApiRequest)): Boolean = "request".equalsIgnoreCase(t.exchangePattern)
}
