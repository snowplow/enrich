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
package com.snowplowanalytics.snowplow.enrich.common

import java.util.concurrent.Executors
import java.nio.file.NoSuchFileException

import scala.concurrent.ExecutionContext

import cats.implicits._

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global

import org.http4s.ember.client.EmberClientBuilder

import cats.effect.testing.specs2.CatsEffect

import fs2.io.file.{Files, Path}

import io.circe.Json
import io.circe.literal._

import org.apache.http.NameValuePair
import org.apache.http.message.BasicNameValuePair

import com.snowplowanalytics.iglu.client.{IgluCirceClient, Resolver}
import com.snowplowanalytics.iglu.client.resolver.registries.Registry
import com.snowplowanalytics.iglu.client.resolver.registries.JavaNetRegistryLookup

import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.lrumap.CreateLruMap._

import com.snowplowanalytics.snowplow.enrich.common.adapters._
import com.snowplowanalytics.snowplow.enrich.common.utils.{HttpClient, JsonUtils}

object SpecHelpers extends CatsEffect {

  val StaticTime = 1599750938180L

  val DefaultMaxJsonDepth = 40

  // Standard Iglu configuration
  private val igluConfig = json"""{
    "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
    "data": {
      "cacheSize": 500,
      "repositories": [
        {
          "name": "Iglu Central",
          "priority": 0,
          "vendorPrefixes": [ "com.snowplowanalytics" ],
          "connection": {
            "http": {
              "uri": "http://iglucentral.com"
            }
          }
        },
        {
          "name": "Embedded src/test/resources",
          "priority": 100,
          "vendorPrefixes": [ "com.snowplowanalytics" ],
          "connection": {
            "embedded": {
              "path": "/iglu-schemas"
            }
          }
        }
      ]
    }
  }"""

  /** Builds an Iglu client from the above Iglu configuration. */
  val client: IgluCirceClient[IO] = IgluCirceClient
    .parseDefault[IO](igluConfig, maxJsonDepth = 40)
    .value
    .unsafeRunSync()
    .getOrElse(throw new RuntimeException("invalid resolver configuration"))

  /** Builds an Iglu client with given max JSON depth. */
  def client(maxJsonDepth: Int): IgluCirceClient[IO] =
    IgluCirceClient
      .parseDefault[IO](igluConfig, maxJsonDepth)
      .value
      .unsafeRunSync()
      .getOrElse(throw new RuntimeException("invalid resolver configuration"))

  val registryLookup = JavaNetRegistryLookup.ioLookupInstance[IO]

  val blockingEC = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool)

  val httpClient = EmberClientBuilder.default[IO].build.map(HttpClient.fromHttp4sClient[IO])

  private type NvPair = (String, String)

  /**
   * Converts an NvPair into a
   * BasicNameValuePair
   *
   * @param pair The Tuple2[String, String] name-value
   * pair to convert
   * @return the basic name value pair
   */
  private def toNvPair(pair: NvPair): BasicNameValuePair =
    new BasicNameValuePair(pair._1, pair._2)

  /** Converts the supplied NvPairs into a NameValueNel */
  def toNameValuePairs(pairs: NvPair*): List[NameValuePair] =
    List(pairs.map(toNvPair): _*)

  /**
   * Builds a self-describing JSON by
   * wrapping the supplied JSON with
   * schema and data properties
   *
   * @param json The JSON to use as the request body
   * @param schema The name of the schema to insert
   * @return a self-describing JSON
   */
  def toSelfDescJson(json: String, schema: String): String =
    s"""{"schema":"iglu:com.snowplowanalytics.snowplow/${schema}/jsonschema/1-0-0","data":${json}}"""

  /** Parse a string containing a SDJ as [[SelfDescribingData]] */
  def jsonStringToSDJ(rawJson: String): Either[String, SelfDescribingData[Json]] =
    JsonUtils
      .extractJson(rawJson, DefaultMaxJsonDepth)
      .leftMap(err => s"Can't parse [$rawJson] as Json, error: [$err]")
      .flatMap(SelfDescribingData.parse[Json])
      .leftMap(err => s"Can't parse Json [$rawJson] as as SelfDescribingData, error: [$err]")

  def listContexts(rawContexts: String): List[SelfDescribingData[Json]] =
    jsonStringToSDJ(rawContexts)
      .map(_.data.asArray.get.toList)
      .flatMap(contexts => contexts.traverse(c => SelfDescribingData.parse[Json](c))) match {
      case Left(err) =>
        throw new IllegalArgumentException(s"Couldn't list contexts schemas. Error: [$err]")
      case Right(sdjs) => sdjs
    }

  def listContextsSchemas(rawContexts: String): List[SchemaKey] = listContexts(rawContexts).map(_.schema)

  def getUnstructSchema(rawUnstruct: String): SchemaKey =
    jsonStringToSDJ(rawUnstruct)
      .map(_.data)
      .flatMap(SelfDescribingData.parse[Json])
      .map(_.schema) match {
      case Left(err) =>
        throw new IllegalArgumentException(s"Couldn't get unstruct event schema. Error: [$err]")
      case Right(schema) => schema
    }

  implicit class MapOps[A, B](underlying: Map[A, B]) {
    def toOpt: Map[A, Option[B]] = underlying.map { case (a, b) => (a, Option(b)) }
  }

  /** Clean-up predefined list of files */
  def filesCleanup(files: List[Path]): IO[Unit] =
    files.traverse_ { path =>
      Files[IO].deleteIfExists(path).recover {
        case _: NoSuchFileException => false
      }
    }

  /** Make sure files don't exist before and after test starts */
  def filesResource(files: List[Path]): Resource[IO, Unit] =
    Resource.make(filesCleanup(files))(_ => filesCleanup(files))

  def createIgluClient(registries: List[Registry]): IO[IgluCirceClient[IO]] =
    IgluCirceClient.fromResolver[IO](Resolver[IO](registries, None), cacheSize = 0, maxJsonDepth = 40)

  val emitIncomplete = false

  val callrailSchemas = CallrailSchemas(
    call_complete = "iglu:com.callrail/call_complete/jsonschema/1-0-2"
  )

  val cloudfrontAccessLogSchemas = CloudfrontAccessLogSchemas(
    with_12_fields = "iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-0",
    with_15_fields = "iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-1",
    with_18_fields = "iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-2",
    with_19_fields = "iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-3",
    with_23_fields = "iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-4",
    with_24_fields = "iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-5",
    with_26_fields = "iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-6"
  )

  val sendgridSchemas = SendgridSchemas(
    processed = "iglu:com.sendgrid/processed/jsonschema/3-0-0",
    dropped = "iglu:com.sendgrid/dropped/jsonschema/3-0-0",
    delivered = "iglu:com.sendgrid/delivered/jsonschema/3-0-0",
    bounce = "iglu:com.sendgrid/bounce/jsonschema/3-0-0",
    deferred = "iglu:com.sendgrid/deferred/jsonschema/3-0-0",
    open = "iglu:com.sendgrid/open/jsonschema/3-0-0",
    click = "iglu:com.sendgrid/click/jsonschema/3-0-0",
    unsubscribe = "iglu:com.sendgrid/unsubscribe/jsonschema/3-0-0",
    group_unsubscribe = "iglu:com.sendgrid/group_unsubscribe/jsonschema/3-0-0",
    group_resubscribe = "iglu:com.sendgrid/group_resubscribe/jsonschema/3-0-0",
    spamreport = "iglu:com.sendgrid/spamreport/jsonschema/3-0-0"
  )

  val googleAnalyticsSchemas = GoogleAnalyticsSchemas(
    page_view = "iglu:com.google.analytics.measurement-protocol/page_view/jsonschema/1-0-0",
    screen_view = "iglu:com.google.analytics.measurement-protocol/screen_view/jsonschema/1-0-0",
    event = "iglu:com.google.analytics.measurement-protocol/event/jsonschema/1-0-0",
    transaction = "iglu:com.google.analytics.measurement-protocol/transaction/jsonschema/1-0-0",
    item = "iglu:com.google.analytics.measurement-protocol/item/jsonschema/1-0-0",
    social = "iglu:com.google.analytics.measurement-protocol/social/jsonschema/1-0-0",
    exception = "iglu:com.google.analytics.measurement-protocol/exception/jsonschema/1-0-0",
    timing = "iglu:com.google.analytics.measurement-protocol/timing/jsonschema/1-0-0",
    undocumented = "iglu:com.google.analytics/undocumented/jsonschema/1-0-0",
    `private` = "iglu:com.google.analytics/private/jsonschema/1-0-0",
    general = "iglu:com.google.analytics.measurement-protocol/general/jsonschema/1-0-0",
    user = "iglu:com.google.analytics.measurement-protocol/user/jsonschema/1-0-0",
    session = "iglu:com.google.analytics.measurement-protocol/session/jsonschema/1-0-0",
    traffic_source = "iglu:com.google.analytics.measurement-protocol/traffic_source/jsonschema/1-0-0",
    system_info = "iglu:com.google.analytics.measurement-protocol/system_info/jsonschema/1-0-0",
    link = "iglu:com.google.analytics.measurement-protocol/link/jsonschema/1-0-0",
    app = "iglu:com.google.analytics.measurement-protocol/app/jsonschema/1-0-0",
    product_action = "iglu:com.google.analytics.measurement-protocol/product_action/jsonschema/1-0-0",
    content_experiment = "iglu:com.google.analytics.measurement-protocol/content_experiment/jsonschema/1-0-0",
    hit = "iglu:com.google.analytics.measurement-protocol/hit/jsonschema/1-0-0",
    promotion_action = "iglu:com.google.analytics.measurement-protocol/promotion_action/jsonschema/1-0-0",
    product = "iglu:com.google.analytics.measurement-protocol/product/jsonschema/1-0-0",
    product_custom_dimension = "iglu:com.google.analytics.measurement-protocol/product_custom_dimension/jsonschema/1-0-0",
    product_custom_metric = "iglu:com.google.analytics.measurement-protocol/product_custom_metric/jsonschema/1-0-0",
    product_impression_list = "iglu:com.google.analytics.measurement-protocol/product_impression_list/jsonschema/1-0-0",
    product_impression = "iglu:com.google.analytics.measurement-protocol/product_impression/jsonschema/1-0-0",
    product_impression_custom_dimension =
      "iglu:com.google.analytics.measurement-protocol/product_impression_custom_dimension/jsonschema/1-0-0",
    product_impression_custom_metric = "iglu:com.google.analytics.measurement-protocol/product_impression_custom_metric/jsonschema/1-0-0",
    promotion = "iglu:com.google.analytics.measurement-protocol/promotion/jsonschema/1-0-0",
    custom_dimension = "iglu:com.google.analytics.measurement-protocol/custom_dimension/jsonschema/1-0-0",
    custom_metric = "iglu:com.google.analytics.measurement-protocol/custom_metric/jsonschema/1-0-0",
    content_group = "iglu:com.google.analytics.measurement-protocol/content_group/jsonschema/1-0-0"
  )

  val hubspotSchemas = HubspotSchemas(
    contact_creation = "iglu:com.hubspot/contact_creation/jsonschema/1-0-0",
    contact_deletion = "iglu:com.hubspot/contact_deletion/jsonschema/1-0-0",
    contact_change = "iglu:com.hubspot/contact_change/jsonschema/1-0-0",
    company_creation = "iglu:com.hubspot/company_creation/jsonschema/1-0-0",
    company_deletion = "iglu:com.hubspot/company_deletion/jsonschema/1-0-0",
    company_change = "iglu:com.hubspot/company_change/jsonschema/1-0-0",
    deal_creation = "iglu:com.hubspot/deal_creation/jsonschema/1-0-0",
    deal_deletion = "iglu:com.hubspot/deal_deletion/jsonschema/1-0-0",
    deal_change = "iglu:com.hubspot/deal_change/jsonschema/1-0-0"
  )

  val mailchimpSchemas = MailchimpSchemas(
    subscribe = "iglu:com.mailchimp/subscribe/jsonschema/1-0-0",
    unsubscribe = "iglu:com.mailchimp/unsubscribe/jsonschema/1-0-0",
    campaign_sending_status = "iglu:com.mailchimp/campaign_sending_status/jsonschema/1-0-0",
    cleaned_email = "iglu:com.mailchimp/cleaned_email/jsonschema/1-0-0",
    email_address_change = "iglu:com.mailchimp/email_address_change/jsonschema/1-0-0",
    profile_update = "iglu:com.mailchimp/profile_update/jsonschema/1-0-0"
  )

  val mailgunSchemas = MailgunSchemas(
    message_bounced = "iglu:com.mailgun/message_bounced/jsonschema/1-0-0",
    message_clicked = "iglu:com.mailgun/message_clicked/jsonschema/1-0-0",
    message_complained = "iglu:com.mailgun/message_complained/jsonschema/1-0-0",
    message_delivered = "iglu:com.mailgun/message_delivered/jsonschema/1-0-0",
    message_dropped = "iglu:com.mailgun/message_dropped/jsonschema/1-0-0",
    message_opened = "iglu:com.mailgun/message_opened/jsonschema/1-0-0",
    recipient_unsubscribed = "iglu:com.mailgun/recipient_unsubscribed/jsonschema/1-0-0"
  )

  val mandrillSchemas = MandrillSchemas(
    message_bounced = "iglu:com.mandrill/message_bounced/jsonschema/1-0-2",
    message_clicked = "iglu:com.mandrill/message_clicked/jsonschema/1-0-2",
    message_delayed = "iglu:com.mandrill/message_delayed/jsonschema/1-0-2",
    message_delivered = "iglu:com.mandrill/message_delivered/jsonschema/1-0-0",
    message_marked_as_spam = "iglu:com.mandrill/message_marked_as_spam/jsonschema/1-0-2",
    message_opened = "iglu:com.mandrill/message_opened/jsonschema/1-0-3",
    message_rejected = "iglu:com.mandrill/message_rejected/jsonschema/1-0-1",
    message_sent = "iglu:com.mandrill/message_sent/jsonschema/1-0-1",
    message_soft_bounced = "iglu:com.mandrill/message_soft_bounced/jsonschema/1-0-2",
    recipient_unsubscribed = "iglu:com.mandrill/recipient_unsubscribed/jsonschema/1-0-2"
  )

  val marketoSchemas = MarketoSchemas(
    event = "iglu:com.marketo/event/jsonschema/2-0-0"
  )

  val olarkSchemas = OlarkSchemas(
    transcript = "iglu:com.olark/transcript/jsonschema/1-0-0",
    offline_message = "iglu:com.olark/offline_message/jsonschema/1-0-0"
  )

  val pagerdutySchemas = PagerdutySchemas(
    incident = "iglu:com.pagerduty/incident/jsonschema/1-0-0"
  )

  val pingdomSchemas = PingdomSchemas(
    incident_assign = "iglu:com.pingdom/incident_assign/jsonschema/1-0-0",
    incident_notify_user = "iglu:com.pingdom/incident_notify_user/jsonschema/1-0-0",
    incident_notify_of_close = "iglu:com.pingdom/incident_notify_of_close/jsonschema/1-0-0"
  )

  val statusGatorSchemas = StatusGatorSchemas(
    status_change = "iglu:com.statusgator/status_change/jsonschema/1-0-0"
  )

  val unbounceSchemas = UnbounceSchemas(
    form_post = "iglu:com.unbounce/form_post/jsonschema/1-0-0"
  )

  val urbanAirshipSchemas = UrbanAirshipSchemas(
    close = "iglu:com.urbanairship.connect/CLOSE/jsonschema/1-0-0",
    custom = "iglu:com.urbanairship.connect/CUSTOM/jsonschema/1-0-0",
    first_open = "iglu:com.urbanairship.connect/FIRST_OPEN/jsonschema/1-0-0",
    in_app_message_display = "iglu:com.urbanairship.connect/IN_APP_MESSAGE_DISPLAY/jsonschema/1-0-0",
    in_app_message_expiration = "iglu:com.urbanairship.connect/IN_APP_MESSAGE_EXPIRATION/jsonschema/1-0-0",
    in_app_message_resolution = "iglu:com.urbanairship.connect/IN_APP_MESSAGE_RESOLUTION/jsonschema/1-0-0",
    location = "iglu:com.urbanairship.connect/LOCATION/jsonschema/1-0-0",
    open = "iglu:com.urbanairship.connect/OPEN/jsonschema/1-0-0",
    push_body = "iglu:com.urbanairship.connect/PUSH_BODY/jsonschema/1-0-0",
    region = "iglu:com.urbanairship.connect/REGION/jsonschema/1-0-0",
    rich_delete = "iglu:com.urbanairship.connect/RICH_DELETE/jsonschema/1-0-0",
    rich_delivery = "iglu:com.urbanairship.connect/RICH_DELIVERY/jsonschema/1-0-0",
    rich_head = "iglu:com.urbanairship.connect/RICH_HEAD/jsonschema/1-0-0",
    send = "iglu:com.urbanairship.connect/SEND/jsonschema/1-0-0",
    tag_change = "iglu:com.urbanairship.connect/TAG_CHANGE/jsonschema/1-0-0",
    uninstall = "iglu:com.urbanairship.connect/UNINSTALL/jsonschema/1-0-0"
  )

  val veroSchemas = VeroSchemas(
    bounced = "iglu:com.getvero/bounced/jsonschema/1-0-0",
    clicked = "iglu:com.getvero/clicked/jsonschema/1-0-0",
    delivered = "iglu:com.getvero/delivered/jsonschema/1-0-0",
    opened = "iglu:com.getvero/opened/jsonschema/1-0-0",
    sent = "iglu:com.getvero/sent/jsonschema/1-0-0",
    unsubscribed = "iglu:com.getvero/unsubscribed/jsonschema/1-0-0",
    created = "iglu:com.getvero/created/jsonschema/1-0-0",
    updated = "iglu:com.getvero/updated/jsonschema/1-0-0"
  )

  val adaptersSchemas = AdaptersSchemas(
    callrail = callrailSchemas,
    cloudfrontAccessLog = cloudfrontAccessLogSchemas,
    googleAnalytics = googleAnalyticsSchemas,
    hubspot = hubspotSchemas,
    mailchimp = mailchimpSchemas,
    mailgun = mailgunSchemas,
    mandrill = mandrillSchemas,
    marketo = marketoSchemas,
    olark = olarkSchemas,
    pagerduty = pagerdutySchemas,
    pingdom = pingdomSchemas,
    sendgrid = sendgridSchemas,
    statusgator = statusGatorSchemas,
    unbounce = unbounceSchemas,
    urbanAirship = urbanAirshipSchemas,
    vero = veroSchemas
  )

  val atomicFieldLimitsDefaults: Map[String, Int] = Map(
    "app_id" -> 255,
    "platform" -> 255,
    "event" -> 128,
    "event_id" -> 36,
    "name_tracker" -> 128,
    "v_tracker" -> 100,
    "v_collector" -> 100,
    "v_etl" -> 100,
    "user_id" -> 255,
    "user_ipaddress" -> 128,
    "user_fingerprint" -> 128,
    "domain_userid" -> 128,
    "network_userid" -> 128,
    "geo_country" -> 2,
    "geo_region" -> 3,
    "geo_city" -> 75,
    "geo_zipcode" -> 15,
    "geo_region_name" -> 100,
    "ip_isp" -> 100,
    "ip_organization" -> 128,
    "ip_domain" -> 128,
    "ip_netspeed" -> 100,
    "page_url" -> 10000,
    "page_title" -> 2000,
    "page_referrer" -> 10000,
    "page_urlscheme" -> 16,
    "page_urlhost" -> 255,
    "page_urlpath" -> 3000,
    "page_urlquery" -> 6000,
    "page_urlfragment" -> 3000,
    "refr_urlscheme" -> 16,
    "refr_urlhost" -> 255,
    "refr_urlpath" -> 6000,
    "refr_urlquery" -> 6000,
    "refr_urlfragment" -> 3000,
    "refr_medium" -> 25,
    "refr_source" -> 50,
    "refr_term" -> 255,
    "mkt_clickid" -> 1000,
    "mkt_network" -> 64,
    "mkt_medium" -> 255,
    "mkt_source" -> 255,
    "mkt_term" -> 255,
    "mkt_content" -> 500,
    "mkt_campaign" -> 255,
    "se_category" -> 1000,
    "se_action" -> 1000,
    "se_label" -> 4096,
    "se_property" -> 1000,
    "tr_orderid" -> 255,
    "tr_affiliation" -> 255,
    "tr_city" -> 255,
    "tr_state" -> 255,
    "tr_country" -> 255,
    "ti_orderid" -> 255,
    "ti_sku" -> 255,
    "ti_name" -> 255,
    "ti_category" -> 255,
    "useragent" -> 1000,
    "br_name" -> 50,
    "br_family" -> 50,
    "br_version" -> 50,
    "br_type" -> 50,
    "br_renderengine" -> 50,
    "br_lang" -> 255,
    "br_colordepth" -> 12,
    "os_name" -> 50,
    "os_family" -> 50,
    "os_manufacturer" -> 50,
    "os_timezone" -> 255,
    "dvce_type" -> 50,
    "doc_charset" -> 128,
    "tr_currency" -> 3,
    "ti_currency" -> 3,
    "base_currency" -> 3,
    "geo_timezone" -> 64,
    "etl_tags" -> 500,
    "refr_domain_userid" -> 128,
    "domain_sessionid" -> 128,
    "event_vendor" -> 1000,
    "event_name" -> 1000,
    "event_format" -> 128,
    "event_version" -> 128,
    "event_fingerprint" -> 128
  )
}
