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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import org.specs2.Specification

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

class EventFingerprintEnrichmentSpec extends Specification {
  def is = s2"""
  getEventFingerprint should combine fields into a hash                       $e1
  getEventFingerprint should not depend on the order of fields                $e2
  getEventFingerprint should not depend on excluded fields                    $e3
  getEventFingerprint should return different values even when fields overlap $e4
  getEventFingerprint should return SHA1 length of 40 bytes                   $e5
  getEventFingerprint should return SHA256 length of 64 bytes                 $e6
  getEventFingerprint should return SHA384 length of 96 bytes                 $e7
  getEventFingerprint should return SHA512 length of 128 bytes                $e8
  """

  val standardConfig =
    EventFingerprintEnrichment(
      EventFingerprintEnrichment.getAlgorithm("MD5").right.get,
      List("stm", "eid")
    )

  def e1 = {
    val config = EventFingerprintEnrichment(
      s => s.size.toString,
      List("stm")
    )

    config.getEventFingerprint(
      Map(
        "stm" -> "1000000000000",
        "e" -> "se",
        "se_ac" -> "buy"
      ).toOpt
    ) must_== "15"
  }

  def e2 = {
    val initialVersion = Map(
      "e" -> "se",
      "se_ac" -> "action",
      "se_ca" -> "category",
      "se_pr" -> "property"
    ).toOpt

    val permutedVersion = Map(
      "se_ca" -> "category",
      "se_ac" -> "action",
      "se_pr" -> "property",
      "e" -> "se"
    ).toOpt

    standardConfig.getEventFingerprint(permutedVersion) must_== standardConfig.getEventFingerprint(
      initialVersion
    )
  }

  def e3 = {
    val initialVersion = Map(
      "stm" -> "1000000000000",
      "eid" -> "123e4567-e89b-12d3-a456-426655440000",
      "e" -> "se",
      "se_ac" -> "buy"
    ).toOpt
    val delayedVersion = Map(
      "stm" -> "9999999999999",
      "e" -> "se",
      "se_ac" -> "buy"
    ).toOpt

    standardConfig.getEventFingerprint(delayedVersion) must_== standardConfig.getEventFingerprint(
      initialVersion
    )
  }

  def e4 = {
    val initialVersion = Map(
      "prefix" -> "suffix"
    ).toOpt

    standardConfig.getEventFingerprint(initialVersion) should not be standardConfig
      .getEventFingerprint(initialVersion)
  }

  def e5 = {
    val sha1Config =
      EventFingerprintEnrichment(
        EventFingerprintEnrichment.getAlgorithm("SHA1").toOption.get,
        List("stm", "eid")
      )

    val initialVersion = Map(
      "e" -> "se",
      "se_ac" -> "action"
    ).toOpt

    sha1Config.getEventFingerprint(initialVersion).length() must_== 40
  }

  def e6 = {
    val sha256Config =
      EventFingerprintEnrichment(
        EventFingerprintEnrichment.getAlgorithm("SHA256").toOption.get,
        List("stm", "eid")
      )

    val initialVersion = Map(
      "e" -> "se",
      "se_ac" -> "action"
    ).toOpt

    sha256Config.getEventFingerprint(initialVersion).length() must_== 64
  }

  def e7 = {
    val sha384Config =
      EventFingerprintEnrichment(
        EventFingerprintEnrichment.getAlgorithm("SHA384").toOption.get,
        List("stm", "eid")
      )

    val initialVersion = Map(
      "e" -> "se",
      "se_ac" -> "action"
    ).toOpt

    sha384Config.getEventFingerprint(initialVersion).length() must_== 96
  }

  def e8 = {
    val sha512Config =
      EventFingerprintEnrichment(
        EventFingerprintEnrichment.getAlgorithm("SHA512").toOption.get,
        List("stm", "eid")
      )

    val initialVersion = Map(
      "e" -> "se",
      "se_ac" -> "action"
    ).toOpt

    sha512Config.getEventFingerprint(initialVersion).length() must_== 128
  }

}
