/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.loaders

import scala.annotation.tailrec

/**
 * Gets the true IP address events forwarded to the Scala Stream Collector.
 * See https://github.com/snowplow/snowplow/issues/1372
 */
object IpAddressExtractor {

  private val ipRegex =
    """\"?\[?(?:(?:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*)|((?:[0-9a-f]|\.|\:+)+).*)\]?\"?""" // 1 group IPv4 and 1 IPv6
  private val XForwardedForRegex = s"""^x-forwarded-for: $ipRegex.*""".r
  private val ForwardedForRegex = s"""^forwarded: for=$ipRegex.*""".r
  private val CloudfrontRegex = s"""^$ipRegex.*""".r

  /**
   * If a request has been forwarded, extract the original client IP address;
   * otherwise return the standard IP address
   * If both FORWARDED and X-FORWARDED-FOR are set,
   * the IP contained in X-FORWARDED-FOR will be used.
   * @param headers List of headers potentially containing X-FORWARDED-FOR or FORWARDED
   * @param lastIp Fallback IP address if no X-FORWARDED-FOR or FORWARDED header exists
   * @return True client IP address
   */
  @tailrec
  def extractIpAddress(
    headers: List[String],
    lastIp: String,
    maybeForwardedForIp: Option[String] = None
  ): String =
    headers match {
      case h :: t =>
        h.toLowerCase match {
          case XForwardedForRegex(ipv4, ipv6) => Option(ipv4).getOrElse(ipv6)
          case ForwardedForRegex(ipv4, ipv6) =>
            val ip = Option(ipv4).getOrElse(ipv6)
            extractIpAddress(t, lastIp, Some(ip))
          case _ => extractIpAddress(t, lastIp)
        }
      case Nil =>
        maybeForwardedForIp match {
          case Some(forwardedForIp) => forwardedForIp
          case _ => lastIp
        }
    }

  /**
   * If a request has been forwarded, extract the original client IP address;
   * otherwise return the standard IP address
   * @param xForwardedFor x-forwarded-for field from the Cloudfront log
   * @param lastIp Fallback IP address if no X-FORWARDED-FOR header exists
   * @return True client IP address
   */
  def extractIpAddress(xForwardedFor: String, lastIp: String): String =
    xForwardedFor match {
      case CloudfrontRegex(ipv4, ipv6) => Option(ipv4).getOrElse(ipv6)
      case _ => lastIp
    }
}
