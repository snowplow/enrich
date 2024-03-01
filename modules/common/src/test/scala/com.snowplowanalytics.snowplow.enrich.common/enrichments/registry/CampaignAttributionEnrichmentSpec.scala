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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import org.specs2.Specification

class CampaignAttributionEnrichmentSpec extends Specification {
  def is = s2"""
  extractMarketingFields should create an empty MarketingCampaign if no campaign fields are specified $e1
  extractMarketingFields should create a MarketingCampaign using the standard Google-style settings   $e2
  extractMarketingFields should create a MarketingCampaign using the standard Omniture settings       $e3
  extractMarketingFields should create a MarketingCampaign using the correct order of precedence      $e4
  extractMarketingFields should create a MarketingCampaign with clickId and network fields            $e5
  """

  val google_uri = List(
    ("utm_source" -> Some("GoogleSearch")),
    ("utm_medium" -> Some("cpc")),
    ("utm_term" -> Some("native american tarot deck")),
    ("utm_content" -> Some("39254295088")),
    ("utm_campaign" -> Some("uk-tarot--native-american"))
  )

  val omniture_uri = List(("cid" -> Some("uk-tarot--native-american")))

  val heterogeneous_uri = List(
    ("utm_source" -> Some("GoogleSearch")),
    ("source" -> Some("bad_source")),
    ("utm_medium" -> Some("cpc")),
    ("legacy_term" -> Some("bad_term")),
    ("utm_term" -> Some("native american tarot deck")),
    ("legacy_campaign" -> Some("bad_campaign")),
    ("cid" -> Some("uk-tarot--native-american"))
  )

  val clickid_uri = List(
    ("utm_source" -> Some("GoogleSearch")),
    ("source" -> Some("bad_source")),
    ("utm_medium" -> Some("cpc")),
    ("legacy_term" -> Some("bad_term")),
    ("utm_term" -> Some("native american tarot deck")),
    ("legacy_campaign" -> Some("bad_campaign")),
    ("cid" -> Some("uk-tarot--native-american")),
    ("msclkid" -> Some("500"))
  )

  def e1 = {
    val config = CampaignAttributionEnrichment(
      List(),
      List(),
      List(),
      List(),
      List(),
      List()
    )

    config.extractMarketingFields(google_uri) must_==
      MarketingCampaign(None, None, None, None, None, None, None)
  }

  def e2 = {
    val config = CampaignAttributionEnrichment(
      List("utm_medium"),
      List("utm_source"),
      List("utm_term"),
      List("utm_content"),
      List("utm_campaign"),
      List()
    )

    config.extractMarketingFields(google_uri) must_==
      MarketingCampaign(
        Some("cpc"),
        Some("GoogleSearch"),
        Some("native american tarot deck"),
        Some("39254295088"),
        Some("uk-tarot--native-american"),
        None,
        None
      )
  }

  def e3 = {
    val config = CampaignAttributionEnrichment(
      List(),
      List(),
      List(),
      List(),
      List("cid"),
      List()
    )

    config.extractMarketingFields(omniture_uri) must_==
      MarketingCampaign(None, None, None, None, Some("uk-tarot--native-american"), None, None)
  }

  def e4 = {
    val config = CampaignAttributionEnrichment(
      List("utm_medium", "medium"),
      List("utm_source", "source"),
      List("utm_term", "legacy_term"),
      List("utm_content"),
      List("utm_campaign", "cid", "legacy_campaign"),
      List()
    )

    config.extractMarketingFields(heterogeneous_uri) must_==
      MarketingCampaign(
        Some("cpc"),
        Some("GoogleSearch"),
        Some("native american tarot deck"),
        None,
        Some("uk-tarot--native-american"),
        None,
        None
      )
  }

  def e5 = {
    val config = CampaignAttributionEnrichment(
      List("utm_medium", "medium"),
      List("utm_source", "source"),
      List("utm_term", "legacy_term"),
      List("utm_content"),
      List("utm_campaign", "cid", "legacy_campaign"),
      List(
        "gclid" -> "Google",
        "msclkid" -> "Microsoft",
        "dclid" -> "DoubleClick"
      )
    )

    config.extractMarketingFields(clickid_uri) must_==
      MarketingCampaign(
        Some("cpc"),
        Some("GoogleSearch"),
        Some("native american tarot deck"),
        None,
        Some("uk-tarot--native-american"),
        Some("500"),
        Some("Microsoft")
      )
  }

}
