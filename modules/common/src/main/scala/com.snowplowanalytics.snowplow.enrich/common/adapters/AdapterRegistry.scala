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
package com.snowplowanalytics.snowplow.enrich.common.adapters

import java.time.Instant

import cats.Monad
import cats.data.{NonEmptyList, Validated}

import cats.effect.Clock
import cats.implicits._

import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.client.IgluCirceClient

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.adapters.registry._
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.snowplow.{RedirectAdapter, Tp1Adapter, Tp2Adapter}
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload

/**
 * The AdapterRegistry lets us convert a CollectorPayload into one or more RawEvents, using a given
 * adapter.
 */
class AdapterRegistry[F[_]: Clock: Monad](
  remoteAdapters: Map[(String, String), RemoteAdapter[F]],
  adaptersSchemas: AdaptersSchemas
) {
  val adapters: Map[(String, String), Adapter] = Map(
    (Vendor.Snowplow, "tp1") -> Tp1Adapter,
    (Vendor.Snowplow, "tp2") -> Tp2Adapter,
    (Vendor.Redirect, "tp2") -> RedirectAdapter,
    (Vendor.Iglu, "v1") -> IgluAdapter,
    (Vendor.Callrail, "v1") -> CallrailAdapter(adaptersSchemas.callrail),
    (Vendor.Cloudfront, "wd_access_log") -> CloudfrontAccessLogAdapter(adaptersSchemas.cloudfrontAccessLog),
    (Vendor.Mailchimp, "v1") -> MailchimpAdapter(adaptersSchemas.mailchimp),
    (Vendor.Mailgun, "v1") -> MailgunAdapter(adaptersSchemas.mailgun),
    (Vendor.GoogleAnalytics, "v1") -> GoogleAnalyticsAdapter(adaptersSchemas.googleAnalytics),
    (Vendor.Mandrill, "v1") -> MandrillAdapter(adaptersSchemas.mandrill),
    (Vendor.Olark, "v1") -> OlarkAdapter(adaptersSchemas.olark),
    (Vendor.Pagerduty, "v1") -> PagerdutyAdapter(adaptersSchemas.pagerduty),
    (Vendor.Pingdom, "v1") -> PingdomAdapter(adaptersSchemas.pingdom),
    (Vendor.Sendgrid, "v3") -> SendgridAdapter(adaptersSchemas.sendgrid),
    (Vendor.StatusGator, "v1") -> StatusGatorAdapter(adaptersSchemas.statusgator),
    (Vendor.Unbounce, "v1") -> UnbounceAdapter(adaptersSchemas.unbounce),
    (Vendor.UrbanAirship, "v1") -> UrbanAirshipAdapter(adaptersSchemas.urbanAirship),
    (Vendor.Marketo, "v1") -> MarketoAdapter(adaptersSchemas.marketo),
    (Vendor.Vero, "v1") -> VeroAdapter(adaptersSchemas.vero),
    (Vendor.HubSpot, "v1") -> HubSpotAdapter(adaptersSchemas.hubspot)
  )

  private object Vendor {
    val Snowplow = "com.snowplowanalytics.snowplow"
    val Redirect = "r"
    val Iglu = "com.snowplowanalytics.iglu"
    val Callrail = "com.callrail"
    val Cloudfront = "com.amazon.aws.cloudfront"
    val GoogleAnalytics = "com.google.analytics"
    val Mailchimp = "com.mailchimp"
    val Mailgun = "com.mailgun"
    val Mandrill = "com.mandrill"
    val Olark = "com.olark"
    val Pagerduty = "com.pagerduty"
    val Pingdom = "com.pingdom"
    val Sendgrid = "com.sendgrid"
    val StatusGator = "com.statusgator"
    val Unbounce = "com.unbounce"
    val UrbanAirship = "com.urbanairship.connect"
    val Marketo = "com.marketo"
    val Vero = "com.getvero"
    val HubSpot = "com.hubspot"
  }

  /**
   * Router to determine which adapter we use to convert the CollectorPayload into one or more
   * RawEvents.
   * @param payload The CollectorPayload we are transforming
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Strings on
   * Failure
   */
  def toRawEvents(
    payload: CollectorPayload,
    client: IgluCirceClient[F],
    processor: Processor,
    registryLookup: RegistryLookup[F],
    maxJsonDepth: Int,
    etlTstamp: Instant
  ): F[Validated[BadRow, NonEmptyList[RawEvent]]] =
    (adapters.get((payload.api.vendor, payload.api.version)) match {
      case Some(adapter) =>
        adapter.toRawEvents(payload, client, registryLookup, maxJsonDepth)
      case None =>
        remoteAdapters.get((payload.api.vendor, payload.api.version)) match {
          case Some(adapter) =>
            adapter.toRawEvents(payload, maxJsonDepth)
          case None =>
            val f = FailureDetails.AdapterFailure.InputData(
              "vendor/version",
              Some(s"${payload.api.vendor}/${payload.api.version}"),
              "vendor/version combination is not supported"
            )
            Monad[F].pure(f.invalidNel[NonEmptyList[RawEvent]])
        }
    }).map(_.leftMap(enrichFailure(_, payload, payload.api.vendor, payload.api.version, processor, etlTstamp)))

  private def enrichFailure(
    fs: NonEmptyList[FailureDetails.AdapterFailureOrTrackerProtocolViolation],
    cp: CollectorPayload,
    vendor: String,
    vendorVersion: String,
    processor: Processor,
    etlTstamp: Instant
  ): BadRow = {
    val payload = cp.toBadRowPayload
    if (vendorVersion == "tp2" && (vendor == Vendor.Snowplow || vendor == Vendor.Redirect)) {
      val tpViolations = fs.asInstanceOf[NonEmptyList[FailureDetails.TrackerProtocolViolation]]
      val failure =
        Failure.TrackerProtocolViolations(etlTstamp, vendor, vendorVersion, tpViolations)
      BadRow.TrackerProtocolViolations(processor, failure, payload)
    } else {
      val adapterFailures = fs.asInstanceOf[NonEmptyList[FailureDetails.AdapterFailure]]
      val failure = Failure.AdapterFailures(etlTstamp, vendor, vendorVersion, adapterFailures)
      BadRow.AdapterFailures(processor, failure, payload)
    }
  }
}
