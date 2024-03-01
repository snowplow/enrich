/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.adapters

import io.circe.literal._

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsEffect

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class PagerDutyAdapterSpec extends Specification with CatsEffect {
  "enrichWith" should {
    "enrich with PagerDutyAdapter" in {
      val body =
        json"""{"messages":[{"type":"incident.resolve","data":{"incident":{"id":"P850PWC","incident_number":597,"created_on":"2014-12-17T17:02:51Z","status":"resolved","html_url":"https://snowplow.pagerduty.com/incidents/P850PWC","incident_key":"/opt/snowplow-saas/bin/run-and-load-goeuro-adwords.shgoeuro@1418835766","service":{"id":"PE7H89B","name":"ManagedServiceBatchYaliCrons","html_url":"https://snowplow.pagerduty.com/services/PE7H89B","deleted_at":null},"escalation_policy":{"id":"P8ETVHU","name":"Yalifirst","deleted_at":null},"assigned_to_user":null,"trigger_summary_data":{"description":"executordetectedfailureforgoeuro"},"trigger_details_html_url":"https://snowplow.pagerduty.com/incidents/P850PWC/log_entries/Q2KN5OMT7QL5L0","trigger_type":"trigger_svc_event","last_status_change_on":"2014-12-17T17:11:46Z","last_status_change_by":{"id":"P9L426X","name":"YaliSassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X"},"number_of_escalations":0,"resolved_by_user":{"id":"P9L426X","name":"YaliSassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X"},"assigned_to":[]}},"id":"c8565510-860f-11e4-bbe8-22000ad9bf74","created_on":"2014-12-17T17:11:46Z"}]}"""
      val input = BlackBoxTesting.buildCollectorPayload(
        path = "/com.pagerduty/v1",
        body = body.noSpaces.some,
        contentType = "application/json".some
      )
      val expected = Map(
        "v_tracker" -> "com.pagerduty-v1",
        "event_vendor" -> "com.pagerduty",
        "event_name" -> "incident",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event" -> "unstruct",
        "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.pagerduty/incident/jsonschema/1-0-0","data":{"type":"resolve","data":{"incident":{"assigned_to_user":null,"incident_key":"/opt/snowplow-saas/bin/run-and-load-goeuro-adwords.shgoeuro@1418835766","trigger_summary_data":{"description":"executordetectedfailureforgoeuro"},"last_status_change_by":{"id":"P9L426X","name":"YaliSassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X"},"incident_number":597,"resolved_by_user":{"id":"P9L426X","name":"YaliSassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X"},"service":{"id":"PE7H89B","name":"ManagedServiceBatchYaliCrons","html_url":"https://snowplow.pagerduty.com/services/PE7H89B","deleted_at":null},"trigger_details_html_url":"https://snowplow.pagerduty.com/incidents/P850PWC/log_entries/Q2KN5OMT7QL5L0","id":"P850PWC","assigned_to":[],"number_of_escalations":0,"last_status_change_on":"2014-12-17T17:11:46Z","status":"resolved","escalation_policy":{"id":"P8ETVHU","name":"Yalifirst","deleted_at":null},"created_on":"2014-12-17T17:02:51Z","trigger_type":"trigger_svc_event","html_url":"https://snowplow.pagerduty.com/incidents/P850PWC"}},"id":"c8565510-860f-11e4-bbe8-22000ad9bf74","created_on":"2014-12-17T17:11:46Z"}}}""".noSpaces
      )
      BlackBoxTesting.runTest(input, expected)
    }
  }
}
