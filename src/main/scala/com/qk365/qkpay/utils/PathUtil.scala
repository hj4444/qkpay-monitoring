package com.qk365.qkpay.utils

object PathUtil {
  def getRequestUrlAction(requestUrl: String): String = {
    val requestPath = java.net.URI.create(requestUrl).getPath.split("/")
    requestPath(requestPath.length - 1)
  }
}
