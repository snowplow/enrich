/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.utils

import scala.beans.BeanProperty

import cats.syntax.either._
import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.commons.lang3.builder.HashCodeBuilder
import org.specs2.matcher.ValidatedMatchers
import org.specs2.mutable.Specification

import com.snowplowanalytics.iglu.client.validator.ValidatorReport

import com.snowplowanalytics.snowplow.enrich.common.enrichments.{ClientEnrichments, MiscEnrichments}
import com.snowplowanalytics.snowplow.enrich.common.utils.MapTransformer._

// Test Bean
final class TargetBean {
  @BeanProperty var platform: String = _
  @BeanProperty var br_features_pdf: Byte = _
  @BeanProperty var visit_id: Int = _
  @BeanProperty var tracker_v: String = _
  @BeanProperty var width: Int = _
  @BeanProperty var height: Int = _

  override def equals(other: Any): Boolean =
    other match {
      case that: TargetBean =>
        platform == that.platform &&
          br_features_pdf == that.br_features_pdf &&
          visit_id == that.visit_id &&
          tracker_v == that.tracker_v &&
          height == that.height &&
          width == that.width
      case _ => false
    }
  // No canEqual needed as the class is final

  // Use Reflection - perf hit is okay as this is only in the test suite
  override def hashCode: Int = HashCodeBuilder.reflectionHashCode(this, false)
  override def toString: String = ToStringBuilder.reflectionToString(this)
}

class MapTransformerSpec extends Specification with ValidatedMatchers {

  val identity: (String, String) => Either[ValidatorReport, String] =
    (_, value) => value.asRight

  val sourceMap = Map(
    "p" -> "web",
    "f_pdf" -> "1",
    "vid" -> "1",
    "tv" -> "no-js-0.1.1",
    "res" -> "720x1080",
    "missing" -> "Not in the transformation map"
  )

  val transformMap: TransformMap = Map(
    ("p", (MiscEnrichments.extractPlatform, "platform")),
    ("f_pdf", (ConversionUtils.stringToBooleanLikeJByte, "br_features_pdf")),
    ("vid", (ConversionUtils.stringToJInteger2, "visit_id")),
    ("res", (ClientEnrichments.extractViewDimensions, ("width", "height"))),
    ("tv", (identity, "tracker_v"))
  )

  val expected = {
    val t = new TargetBean()
    t.platform = "web"
    t.br_features_pdf = 1
    t.visit_id = 1
    t.tracker_v = "no-js-0.1.1"
    t.width = 720
    t.height = 1080
    t
  }

  "Applying a TransformMap to an existing POJO" should {
    "successfully set each of the target fields" in {
      val target = {
        val t = new TargetBean()
        t.platform = "old"
        t.tracker_v = "old"
        t
      }
      val result = target.transform(sourceMap, transformMap)

      result must beValid(6) // 6 fields updated
      target must_== expected
    }
  }

  "Executing TransformMap's generate() factory" should {
    "successfully instantiate a new POJO" in {
      val result = MapTransformer.generate[TargetBean](sourceMap, transformMap)
      result must beValid(expected)
    }
  }
}
