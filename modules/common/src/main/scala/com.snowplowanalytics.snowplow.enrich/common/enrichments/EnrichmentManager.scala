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
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import java.nio.charset.Charset
import java.net.URI
import java.time.Instant
import org.joda.time.DateTime
import io.circe.Json
import cats.{Applicative, Monad}
import cats.data.{EitherT, IorT, NonEmptyList, OptionT, StateT}
import cats.implicits._
import cats.effect.kernel.{Clock, Sync}

import com.snowplowanalytics.refererparser._

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.enrich.common.utils.{AtomicError, IgluUtils, OptionIor, ConversionUtils => CU}
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.badrows.{Failure => BadRowFailure}

import com.snowplowanalytics.snowplow.enrich.common.{EtlPipeline, QueryStringParameters, RawEventParameters}
import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{EventEnrichments => EE}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{MiscEnrichments => ME}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.{CrossNavigationEnrichment => CNE, _}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.apirequest.ApiRequestEnrichment
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.pii.PiiPseudonymizerEnrichment
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.sqlquery.SqlQueryEnrichment
import com.snowplowanalytics.snowplow.enrich.common.enrichments.web.{PageEnrichments => WPE}
import com.snowplowanalytics.snowplow.enrich.common.utils.OptionIorT
import com.snowplowanalytics.snowplow.enrich.common.utils.OptionIorT._
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

object EnrichmentManager {

  /**
   * Run the enrichment workflow
   * @param registry Contain configuration for all enrichments to apply
   * @param client Iglu Client, for schema lookups and validation
   * @param processor Meta information about processing asset, for bad rows
   * @param etlTstamp ETL timestamp
   * @param raw Canonical input event to enrich
   * @param featureFlags The feature flags available in the current version of Enrich
   * @param invalidCount Function to increment the count of invalid events
   * @return Right(EnrichedEvent) if everything went well.
   *         Left(BadRow) if something went wrong and incomplete events are not enabled.
   *         Both(BadRow, EnrichedEvent) if something went wrong but incomplete events are enabled.
   */
  def enrichEvent[F[_]: Sync](
    registry: EnrichmentRegistry[F],
    client: IgluCirceClient[F],
    processor: Processor,
    etlTstamp: DateTime,
    raw: RawEvent,
    featureFlags: EtlPipeline.FeatureFlags,
    invalidCount: F[Unit],
    registryLookup: RegistryLookup[F],
    atomicFields: AtomicFields,
    emitIncomplete: Boolean,
    maxJsonDepth: Int
  ): OptionIorT[F, BadRow, EnrichedEvent] = {
    def enrich(enriched: EnrichedEvent): OptionIorT[F, NonEmptyList[NonEmptyList[Failure]], List[SelfDescribingData[Json]]] =
      for {
        extractResult <- mapAndValidateInput(
                           raw,
                           enriched,
                           etlTstamp,
                           processor,
                           client,
                           registryLookup,
                           maxJsonDepth
                         )
                           .leftMap(NonEmptyList.one)
                           .toOptionIorT
        _ = {
          enriched.contexts = ME.formatContexts(extractResult.contexts).orNull
          enriched.unstruct_event = ME.formatUnstructEvent(extractResult.unstructEvent).orNull
        }
        enrichmentsContexts <- runEnrichments(
                                 registry,
                                 raw,
                                 enriched,
                                 extractResult.contexts,
                                 extractResult.unstructEvent,
                                 maxJsonDepth,
                                 Instant.ofEpochMilli(etlTstamp.getMillis)
                               )
                                 .leftMap(NonEmptyList.one)
        validContexts <- validateEnriched(
                           enriched,
                           enrichmentsContexts,
                           client,
                           registryLookup,
                           featureFlags.acceptInvalid,
                           invalidCount,
                           atomicFields,
                           emitIncomplete,
                           Instant.ofEpochMilli(etlTstamp.getMillis)
                         )
                           .leftMap(NonEmptyList.one)
                           .toOptionIorT
        derivedContexts = validContexts ::: extractResult.validationInfoContexts
      } yield derivedContexts

    // derived contexts are set lastly because we want to include failure entities
    // to derived contexts as well and we can get failure entities only in the end
    // of the enrichment process
    OptionIorT(
      for {
        enrichedEvent <- Sync[F].delay(new EnrichedEvent)
        enrichmentResult <- enrich(enrichedEvent).value
        _ = setDerivedContexts(enrichedEvent, enrichmentResult, processor)
        result = enrichmentResult
                   .leftMap { fe =>
                     createBadRow(
                       fe,
                       EnrichedEvent.toPartiallyEnrichedEvent(enrichedEvent),
                       RawEvent.toRawEvent(raw),
                       Instant.ofEpochMilli(etlTstamp.getMillis),
                       processor
                     )
                   }
                   .map(_ => enrichedEvent)
      } yield
        if (emitIncomplete)
          result
        else
          // if emitIncomplete is false, we don't need right side of
          // Both which is for incomplete event therefore we just return
          // its left side
          result match {
            case OptionIor.Both(l, _) => OptionIor.Left(l)
            case o => o
          }
    )
  }

  private def createBadRow(
    fe: NonEmptyList[NonEmptyList[Failure]],
    pe: Payload.PartiallyEnrichedEvent,
    re: Payload.RawEvent,
    etlTstamp: Instant,
    processor: Processor
  ): BadRow = {
    val firstList = fe.head
    firstList.head match {
      case h: Failure.SchemaViolation =>
        val sv = firstList.tail.collect { case f: Failure.SchemaViolation => f }
        BadRow.SchemaViolations(
          processor,
          BadRowFailure.SchemaViolations(etlTstamp, NonEmptyList(h, sv).map(_.schemaViolation)),
          Payload.EnrichmentPayload(pe, re)
        )
      case h: Failure.EnrichmentFailure =>
        val ef = firstList.tail.collect { case f: Failure.EnrichmentFailure => f }
        BadRow.EnrichmentFailures(
          processor,
          BadRowFailure.EnrichmentFailures(etlTstamp, NonEmptyList(h, ef).map(_.enrichmentFailure)),
          Payload.EnrichmentPayload(pe, re)
        )
    }
  }

  def setDerivedContexts(
    enriched: EnrichedEvent,
    enrichmentResult: OptionIor[NonEmptyList[NonEmptyList[Failure]], List[SelfDescribingData[Json]]],
    processor: Processor
  ): Unit = {
    val derivedContexts = enrichmentResult.leftMap { ll =>
      ll.flatten.toList
        .map(_.toSDJ(processor))
    }.merge
    ME.formatContexts(derivedContexts).foreach(c => enriched.derived_contexts = c)
  }

  private def mapAndValidateInput[F[_]: Sync](
    raw: RawEvent,
    enrichedEvent: EnrichedEvent,
    etlTstamp: DateTime,
    processor: Processor,
    client: IgluCirceClient[F],
    registryLookup: RegistryLookup[F],
    maxJsonDepth: Int
  ): IorT[F, NonEmptyList[Failure], IgluUtils.EventExtractResult] =
    for {
      _ <- setupEnrichedEvent[F](raw, enrichedEvent, etlTstamp, processor)
             .leftMap(NonEmptyList.one)
      extract <-
        IgluUtils
          .extractAndValidateInputJsons(enrichedEvent, client, registryLookup, maxJsonDepth, Instant.ofEpochMilli(etlTstamp.getMillis))
          .leftMap { l: NonEmptyList[Failure] => l }
    } yield extract

  /**
   * Run all the enrichments
   * @param enriched /!\ MUTABLE enriched event, mutated IN-PLACE /!\
   * @return All the contexts produced by the enrichments are in the Right.
   *         All the errors are aggregated in the bad row in the Left.
   */
  private def runEnrichments[F[_]: Monad](
    registry: EnrichmentRegistry[F],
    raw: RawEvent,
    enriched: EnrichedEvent,
    inputContexts: List[SelfDescribingData[Json]],
    unstructEvent: Option[SelfDescribingData[Json]],
    maxJsonDepth: Int,
    etlTstamp: Instant
  ): OptionIorT[F, NonEmptyList[Failure], List[SelfDescribingData[Json]]] =
    OptionIorT {
      accState(registry, raw, inputContexts, unstructEvent, maxJsonDepth)
        .runS(Accumulation.Enriched(enriched, Nil, Nil))
        .map {
          case Accumulation.Enriched(_, failures, contexts) =>
            failures.toNel match {
              case Some(nel) =>
                OptionIor.Both(
                  nel.map(f => Failure.EnrichmentFailure(f, etlTstamp)),
                  contexts
                )
              case None =>
                OptionIor.Right(contexts)
            }
          case Accumulation.Dropped => OptionIor.None
        }
    }

  private def validateEnriched[F[_]: Clock: Monad](
    enriched: EnrichedEvent,
    enrichmentsContexts: List[SelfDescribingData[Json]],
    client: IgluCirceClient[F],
    registryLookup: RegistryLookup[F],
    acceptInvalid: Boolean,
    invalidCount: F[Unit],
    atomicFields: AtomicFields,
    emitIncomplete: Boolean,
    etlTstamp: Instant
  ): IorT[F, NonEmptyList[Failure], List[SelfDescribingData[Json]]] =
    for {
      validContexts <- IgluUtils.validateEnrichmentsContexts[F](client, enrichmentsContexts, registryLookup, etlTstamp)
      _ <- AtomicFieldsLengthValidator
             .validate[F](enriched, acceptInvalid, invalidCount, atomicFields, emitIncomplete, etlTstamp)
             .leftMap { v: Failure => NonEmptyList.one(v) }
    } yield validContexts

  private[enrichments] sealed trait Accumulation extends Product with Serializable

  private[enrichments] object Accumulation {
    case class Enriched(
      event: EnrichedEvent,
      errors: List[FailureDetails.EnrichmentFailure],
      contexts: List[SelfDescribingData[Json]]
    ) extends Accumulation

    case object Dropped extends Accumulation
  }

  private[enrichments] type EStateT[F[_], A] = StateT[F, Accumulation, A]

  private object EStateT {
    def apply[F[_]: Applicative, A](f: Accumulation => F[(Accumulation, A)]): EStateT[F, A] =
      StateT(f)

    def fromEither[F[_]: Applicative](
      f: (EnrichedEvent,
        List[SelfDescribingData[Json]]) => Either[NonEmptyList[FailureDetails.EnrichmentFailure], List[SelfDescribingData[Json]]]
    ): EStateT[F, Unit] =
      fromEitherF[F] { case (x, y) => f(x, y).pure[F] }

    def fromEitherF[F[_]: Applicative](
      f: (EnrichedEvent,
        List[SelfDescribingData[Json]]) => F[Either[NonEmptyList[FailureDetails.EnrichmentFailure], List[SelfDescribingData[Json]]]]
    ): EStateT[F, Unit] =
      EStateT {
        case Accumulation.Enriched(event, errors, contexts) =>
          f(event, contexts).map {
            case Right(contexts2) => (Accumulation.Enriched(event, errors, contexts2 ::: contexts), ())
            case Left(moreErrors) => (Accumulation.Enriched(event, moreErrors.toList ::: errors, contexts), ())
          }
        case Accumulation.Dropped => Applicative[F].pure((Accumulation.Dropped, ()))
      }

    def fromEitherOpt[F[_]: Applicative, A](
      f: EnrichedEvent => Either[NonEmptyList[FailureDetails.EnrichmentFailure], Option[A]]
    ): EStateT[F, Option[A]] =
      EStateT {
        case acc @ Accumulation.Enriched(event, errors, contexts) =>
          f(event) match {
            case Right(opt) => Applicative[F].pure((acc, opt))
            case Left(moreErrors) =>
              Applicative[F].pure((Accumulation.Enriched(event, moreErrors.toList ::: errors, contexts), Option.empty[A]))
          }
        case Accumulation.Dropped => Applicative[F].pure((Accumulation.Dropped, Option.empty[A]))
      }

    def fromJsEnrichmentResult[F[_]: Applicative](
      f: (EnrichedEvent, List[SelfDescribingData[Json]]) => JavascriptScriptEnrichment.Result
    ): EStateT[F, Unit] =
      EStateT {
        case Accumulation.Enriched(event, errors, contexts) =>
          f(event, contexts) match {
            case JavascriptScriptEnrichment.Result.Success(c) =>
              Applicative[F].pure((Accumulation.Enriched(event, errors, c ::: contexts), ()))
            case JavascriptScriptEnrichment.Result.Failure(e) =>
              Applicative[F].pure((Accumulation.Enriched(event, e :: errors, contexts), ()))
            case JavascriptScriptEnrichment.Result.Dropped => Applicative[F].pure((Accumulation.Dropped, ()))
          }
        case Accumulation.Dropped => Applicative[F].pure((Accumulation.Dropped, ()))
      }
  }

  private def accState[F[_]: Monad](
    registry: EnrichmentRegistry[F],
    raw: RawEvent,
    inputContexts: List[SelfDescribingData[Json]],
    unstructEvent: Option[SelfDescribingData[Json]],
    maxJsonDepth: Int
  ): EStateT[F, Unit] = {
    val getCookieContexts = headerContexts[F, CookieExtractorEnrichment](
      raw.context.headers,
      registry.cookieExtractor,
      (e, hs) => e.extract(hs)
    )
    val getHttpHeaderContexts = headerContexts[F, HttpHeaderExtractorEnrichment](
      raw.context.headers,
      registry.httpHeaderExtractor,
      (e, hs) => e.extract(hs)
    )
    val sqlContexts = getSqlQueryContexts[F](inputContexts, unstructEvent, registry.sqlQuery)
    val apiContexts = getApiRequestContexts[F](inputContexts, unstructEvent, registry.apiRequest)

    for {
      // format: off
      _       <- getCollectorVersionSet[F]                                          // The load fails if the collector version is not set
      pageUri <- getPageUri[F](raw.context.refererUri)                              // Potentially update the page_url and set the page URL components
      _       <- getDerivedTstamp[F]                                                // Calculate the derived timestamp
      _       <- getIabContext[F](registry.iab)                                     // Fetch IAB enrichment context (before anonymizing the IP address)
      _       <- getUaUtils[F](registry.userAgentUtils)                             // Parse the useragent using user-agent-utils
      _       <- getUaParser[F](registry.uaParser)                                  // Create the ua_parser_context
      _       <- getCurrency[F](raw.context.timestamp, registry.currencyConversion) // Finalize the currency conversion
      _       <- getRefererUri[F](registry.refererParser)                           // Potentially set the referrer details and URL components
      qsMap   <- extractQueryString[F](pageUri, raw.source.encoding)                // Parse the page URI's querystring
      _       <- setCampaign[F](qsMap, registry.campaignAttribution)                // Marketing attribution
      _       <- getCrossDomain[F](qsMap, registry.crossNavigation)                 // Cross-domain tracking
      _       <- setEventFingerprint[F](raw.parameters, registry.eventFingerprint)  // This enrichment cannot fail
      _       <- getCookieContexts                                                  // Execute cookie extractor enrichment
      _       <- getHttpHeaderContexts                                              // Execute header extractor enrichment
      _       <- getWeatherContext[F](registry.weather)                             // Fetch weather context
      _       <- getYauaaContext[F](registry.yauaa, raw.context.headers)            // Runs YAUAA enrichment (gets info thanks to user agent)
      _       <- extractSchemaFields[F](unstructEvent)                              // Extract the event vendor/name/format/version
      _       <- geoLocation[F](registry.ipLookups)                                 // Execute IP lookup enrichment
      _       <- registry.javascriptScript.traverse(                                // Execute the JavaScript scripting enrichment
                   getJsScript[F](_, raw.context.headers, maxJsonDepth)
                 )
      _       <- sqlContexts                                                        // Derive some contexts with custom SQL Query enrichment
      _       <- apiContexts                                                        // Derive some contexts with custom API Request enrichment
      _       <- anonIp[F](registry.anonIp)                                         // Anonymize the IP
      _       <- piiTransform[F](registry.piiPseudonymizer, raw.context.headers)    // Run PII pseudonymization
      // format: on
    } yield ()

  }

  /** Initialize the mutable [[EnrichedEvent]]. */
  private def setupEnrichedEvent[F[_]: Sync](
    raw: RawEvent,
    e: EnrichedEvent,
    etlTstamp: DateTime,
    processor: Processor
  ): IorT[F, Failure.SchemaViolation, Unit] =
    IorT {
      Sync[F].delay {
        e.event_id = EE.generateEventId() // May be updated later if we have an `eid` parameter
        e.v_collector = raw.source.name // May be updated later if we have a `cv` parameter
        e.v_etl = ME.etlVersion(processor)
        e.etl_tstamp = EE.toTimestamp(etlTstamp)
        e.network_userid = raw.context.userId.map(_.toString).orNull // May be updated later by 'nuid'
        e.user_ipaddress = ME
          .extractIp("user_ipaddress", raw.context.ipAddress.orNull)
          .toOption
          .orNull // May be updated later by 'ip'
        // May be updated later if we have a `ua` parameter
        setUseragent(e, raw.context.useragent)
        // Validate that the collectorTstamp exists and is Redshift-compatible
        val collectorTstamp = setCollectorTstamp(e, raw.context.timestamp).toValidatedNel
        // Map/validate/transform input fields to enriched event fields
        val transformed = Transform.transform(raw, e)

        (collectorTstamp |+| transformed).void.toIor
          .leftMap(errors => AtomicFields.errorsToSchemaViolation(errors, Instant.ofEpochMilli(etlTstamp.getMillis)))
          .putRight(())
      }
    }

  def setCollectorTstamp(event: EnrichedEvent, timestamp: DateTime): Either[AtomicError.ParseError, Unit] =
    EE.formatCollectorTstamp(timestamp).map { t =>
      event.collector_tstamp = t
      ().asRight
    }

  def setUseragent(
    event: EnrichedEvent,
    useragent: Option[String]
  ): Unit =
    useragent match {
      case Some(ua) =>
        val s = ua.replaceAll("(\\r|\\n)", "").replaceAll("\\t", "    ")
        event.useragent = s
      case None => () // No fields updated
    }

  // The load fails if the collector version is not set
  def getCollectorVersionSet[F[_]: Applicative]: EStateT[F, Unit] =
    EStateT.fromEither {
      case (event, _) =>
        event.v_collector match {
          case "" | null =>
            NonEmptyList
              .one(
                FailureDetails
                  .EnrichmentFailure(
                    None,
                    FailureDetails.EnrichmentFailureMessage
                      .InputData("v_collector", None, "should be set")
                  )
              )
              .asLeft
          case _ => Nil.asRight
        }
    }

  // If our IpToGeo enrichment is enabled, get the geo-location from the IP address
  // enrichment doesn't fail to maintain the previous approach where failures were suppressed
  // c.f. https://github.com/snowplow/snowplow/issues/351
  def geoLocation[F[_]: Monad](ipLookups: Option[IpLookupsEnrichment[F]]): EStateT[F, Unit] =
    EStateT.fromEitherF {
      case (event, _) =>
        val ipLookup = for {
          enrichment <- OptionT.fromOption[F](ipLookups)
          ip <- OptionT.fromOption[F](Option(event.user_ipaddress))
          result <- OptionT.liftF(enrichment.extractIpInformation(ip))
        } yield result

        ipLookup.value.map {
          case Some(lookup) =>
            lookup.ipLocation.flatMap(_.toOption).foreach { loc =>
              event.geo_country = loc.countryCode
              event.geo_region = loc.region.orNull
              event.geo_city = loc.city.orNull
              event.geo_zipcode = loc.postalCode.orNull
              event.geo_latitude = loc.latitude
              event.geo_longitude = loc.longitude
              event.geo_region_name = loc.regionName.orNull
              event.geo_timezone = loc.timezone.orNull
            }
            lookup.isp.flatMap(_.toOption).foreach { i =>
              event.ip_isp = i
            }
            lookup.organization.flatMap(_.toOption).foreach { org =>
              event.ip_organization = org
            }
            lookup.domain.flatMap(_.toOption).foreach { d =>
              event.ip_domain = d
            }
            lookup.connectionType.flatMap(_.toOption).foreach { ct =>
              event.ip_netspeed = ct
            }
            Nil.asRight
          case None =>
            Nil.asRight
        }
    }

  // Potentially update the page_url and set the page URL components
  def getPageUri[F[_]: Applicative](refererUri: Option[String]): EStateT[F, Option[URI]] =
    EStateT.fromEitherOpt { event =>
      val pageUri = WPE.extractPageUri(refererUri, Option(event.page_url))
      for {
        uri <- pageUri
        u <- uri
      } {
        // Update the page_url
        event.page_url = u.toString

        // Set the URL components
        val components = CU.explodeUri(u)
        event.page_urlscheme = components.scheme
        event.page_urlhost = components.host
        event.page_urlport = components.port
        event.page_urlpath = components.path.orNull
        event.page_urlquery = components.query.orNull
        event.page_urlfragment = components.fragment.orNull
      }
      pageUri.leftMap(NonEmptyList.one(_))
    }

  // Calculate the derived timestamp
  def getDerivedTstamp[F[_]: Applicative]: EStateT[F, Unit] =
    EStateT.fromEither {
      case (event, _) =>
        EE.getDerivedTimestamp(
          Option(event.dvce_sent_tstamp),
          Option(event.dvce_created_tstamp),
          Option(event.collector_tstamp),
          Option(event.true_tstamp)
        ).map { dt =>
          dt.foreach(event.derived_tstamp = _)
          Nil
        }.leftMap(NonEmptyList.one(_))
    }

  // Fetch IAB enrichment context (before anonymizing the IP address).
  // IAB enrichment is called only if the IP is v4 (and after removing the port if any)
  // and if the user agent is defined.
  def getIabContext[F[_]: Applicative](iabEnrichment: Option[IabEnrichment]): EStateT[F, Unit] =
    EStateT.fromEither {
      case (event, _) =>
        val result = for {
          iab <- iabEnrichment
          useragent <- Option(event.useragent).filter(_.trim.nonEmpty)
          ipString <- Option(event.user_ipaddress)
          ip <- CU.extractInetAddress(ipString)
          tstamp <- Option(event.derived_tstamp).map(EventEnrichments.fromTimestamp)
        } yield iab.getIabContext(useragent, ip, tstamp)

        result.sequence.bimap(NonEmptyList.one(_), _.toList)
    }

  def anonIp[F[_]: Applicative](anonIp: Option[AnonIpEnrichment]): EStateT[F, Unit] =
    EStateT.fromEither {
      case (event, _) =>
        anonIp match {
          case Some(anon) =>
            Option(event.user_ipaddress) match {
              case Some(ip) =>
                Option(anon.anonymizeIp(ip)).foreach(event.user_ipaddress = _)
                Nil.asRight
              case None =>
                Nil.asRight
            }
          case None =>
            Nil.asRight
        }
    }

  def getUaUtils[F[_]: Applicative](userAgentUtils: Option[UserAgentUtilsEnrichment]): EStateT[F, Unit] =
    EStateT.fromEither {
      case (event, _) =>
        userAgentUtils match {
          case Some(uap) =>
            Option(event.useragent) match {
              case Some(ua) =>
                val ca = uap.extractClientAttributes(ua)
                ca.map { c =>
                  event.br_name = c.browserName
                  event.br_family = c.browserFamily
                  c.browserVersion.foreach(bv => event.br_version = bv)
                  event.br_type = c.browserType
                  event.br_renderengine = c.browserRenderEngine
                  event.os_name = c.osName
                  event.os_family = c.osFamily
                  event.os_manufacturer = c.osManufacturer
                  event.dvce_type = c.deviceType
                  event.dvce_ismobile = CU.booleanToJByte(c.deviceIsMobile)
                  Nil
                }.leftMap(NonEmptyList.one(_))
              case None => Nil.asRight // No fields updated
            }
          case None => Nil.asRight
        }
    }

  def getUaParser[F[_]: Monad](uaParser: Option[UaParserEnrichment[F]]): EStateT[F, Unit] =
    EStateT.fromEitherF {
      case (event, _) =>
        uaParser match {
          case Some(uap) =>
            Option(event.useragent) match {
              case Some(ua) =>
                uap.extractUserAgent(ua).map {
                  case Left(failure) => Left(NonEmptyList.one(failure))
                  case Right(context) => Right(List(context))
                }
              case None => Monad[F].pure(Nil.asRight) // No fields updated
            }
          case None => Monad[F].pure(Nil.asRight)
        }
    }

  def getCurrency[F[_]: Monad](
    timestamp: DateTime,
    currencyConversion: Option[CurrencyConversionEnrichment[F]]
  ): EStateT[F, Unit] =
    EStateT.fromEitherF {
      case (event, _) =>
        currencyConversion match {
          case Some(currency) =>
            event.base_currency = currency.baseCurrency.getCode
            // Note that jBigDecimalToDouble is applied to either-valid-or-null event POJO
            // properties, so we don't expect any of these four vals to be a Failure
            val enrichmentInfo = FailureDetails.EnrichmentInformation(
              SchemaKey("com.snowplowanalytics.snowplow", "currency_conversion_config", "jsonschema", SchemaVer.Full(1, 0, 0)),
              "currency_conversion"
            )
            val trTax = CU.jBigDecimalToDouble("tr_tx", event.tr_tax, enrichmentInfo).toValidatedNel
            val tiPrice = CU.jBigDecimalToDouble("ti_pr", event.ti_price, enrichmentInfo).toValidatedNel
            val trTotal = CU.jBigDecimalToDouble("tr_tt", event.tr_total, enrichmentInfo).toValidatedNel
            val trShipping = CU.jBigDecimalToDouble("tr_sh", event.tr_shipping, enrichmentInfo).toValidatedNel
            EitherT(
              (trTotal, trTax, trShipping, tiPrice)
                .mapN {
                  currency.convertCurrencies(
                    Option(event.tr_currency),
                    _,
                    _,
                    _,
                    Option(event.ti_currency),
                    _,
                    timestamp
                  )
                }
                .toEither
                .sequence
                .map(_.flatMap(_.toEither))
            ).map {
              case (trTotalBase, trTaxBase, trShippingBase, tiPriceBase) =>
                trTotalBase.foreach(v => event.tr_total_base = v)
                trTaxBase.foreach(v => event.tr_tax_base = v)
                trShippingBase.foreach(v => event.tr_shipping_base = v)
                tiPriceBase.foreach(v => event.ti_price_base = v)
                List.empty[SelfDescribingData[Json]]
            }.value
          case None => Monad[F].pure(Nil.asRight)
        }
    }

  def getRefererUri[F[_]: Applicative](
    refererParser: Option[RefererParserEnrichment]
  ): EStateT[F, Unit] =
    EStateT.fromEither {
      case (event, _) =>
        CU.stringToUri(event.page_referrer)
          .map { uri =>
            for {
              u <- uri
            } {
              // Set the URL components
              val components = CU.explodeUri(u)
              event.refr_urlscheme = components.scheme
              event.refr_urlhost = components.host
              event.refr_urlport = components.port
              event.refr_urlpath = components.path.orNull
              event.refr_urlquery = components.query.orNull
              event.refr_urlfragment = components.fragment.orNull

              // Set the referrer details
              refererParser match {
                case Some(rp) =>
                  for (refr <- rp.extractRefererDetails(u, event.page_urlhost))
                    refr match {
                      case UnknownReferer(medium) => event.refr_medium = CU.makeTsvSafe(medium.value)
                      case SearchReferer(medium, source, term) =>
                        event.refr_medium = CU.makeTsvSafe(medium.value)
                        event.refr_source = CU.makeTsvSafe(source)
                        event.refr_term = CU.makeTsvSafe(term.orNull)
                      case InternalReferer(medium) => event.refr_medium = CU.makeTsvSafe(medium.value)
                      case SocialReferer(medium, source) =>
                        event.refr_medium = CU.makeTsvSafe(medium.value)
                        event.refr_source = CU.makeTsvSafe(source)
                      case EmailReferer(medium, source) =>
                        event.refr_medium = CU.makeTsvSafe(medium.value)
                        event.refr_source = CU.makeTsvSafe(source)
                      case PaidReferer(medium, source) =>
                        event.refr_medium = CU.makeTsvSafe(medium.value)
                        event.refr_source = CU.makeTsvSafe(source)
                      case ChatbotReferer(medium, source) =>
                        event.refr_medium = CU.makeTsvSafe(medium.value)
                        event.refr_source = CU.makeTsvSafe(source)
                    }
                case None => ()
              }
            }
            Nil
          }
          .leftMap(f =>
            NonEmptyList.one(
              FailureDetails.EnrichmentFailure(
                None,
                FailureDetails.EnrichmentFailureMessage.Simple(f)
              )
            )
          )
    }

  // Parse the page URI's querystring
  def extractQueryString[F[_]: Applicative](
    pageUri: Option[URI],
    encoding: String
  ): EStateT[F, Option[QueryStringParameters]] =
    EStateT.fromEitherOpt { _ =>
      pageUri match {
        case Some(u) =>
          CU.extractQuerystring(u, Charset.forName(encoding)).bimap(NonEmptyList.one(_), _.some)
        case None => None.asRight
      }
    }

  def setCampaign[F[_]: Applicative](
    pageQsMap: Option[QueryStringParameters],
    campaignAttribution: Option[CampaignAttributionEnrichment]
  ): EStateT[F, Unit] =
    EStateT.fromEither {
      case (event, _) =>
        pageQsMap match {
          case Some(qsList) =>
            campaignAttribution match {
              case Some(ce) =>
                val cmp = ce.extractMarketingFields(qsList)
                event.mkt_medium = CU.makeTsvSafe(cmp.medium.orNull)
                event.mkt_source = CU.makeTsvSafe(cmp.source.orNull)
                event.mkt_term = CU.makeTsvSafe(cmp.term.orNull)
                event.mkt_content = CU.makeTsvSafe(cmp.content.orNull)
                event.mkt_campaign = CU.makeTsvSafe(cmp.campaign.orNull)
                event.mkt_clickid = CU.makeTsvSafe(cmp.clickId.orNull)
                event.mkt_network = CU.makeTsvSafe(cmp.network.orNull)
                Nil.asRight
              case None => Nil.asRight
            }
          case None => Nil.asRight
        }
    }

  def getCrossDomain[F[_]: Applicative](
    pageQsMap: Option[QueryStringParameters],
    crossNavEnrichment: Option[CNE]
  ): EStateT[F, Unit] =
    EStateT.fromEither {
      case (event, _) =>
        pageQsMap match {
          case Some(qsMap) =>
            CNE
              .parseCrossDomain(qsMap)
              .bimap(
                err =>
                  crossNavEnrichment match {
                    case Some(cn) => NonEmptyList.one(cn.addEnrichmentInfo(err))
                    case None => NonEmptyList.one(err)
                  },
                crossNavMap => {
                  crossNavMap.duid.foreach(event.refr_domain_userid = _)
                  crossNavMap.tstamp.foreach(event.refr_dvce_tstamp = _)
                  crossNavEnrichment match {
                    case Some(_) => crossNavMap.getCrossNavigationContext
                    case None => Nil
                  }
                }
              )
          case None => Nil.asRight
        }
    }

  def setEventFingerprint[F[_]: Applicative](
    parameters: RawEventParameters,
    eventFingerprint: Option[EventFingerprintEnrichment]
  ): EStateT[F, Unit] =
    EStateT.fromEither {
      case (event, _) =>
        eventFingerprint match {
          case Some(efe) =>
            event.event_fingerprint = efe.getEventFingerprint(parameters)
            Nil.asRight
          case _ => Nil.asRight
        }
    }

  // Extract the event vendor/name/format/version
  def extractSchemaFields[F[_]: Applicative](
    unstructEvent: Option[SelfDescribingData[Json]]
  ): EStateT[F, Unit] =
    EStateT.fromEither {
      case (event, _) =>
        SchemaEnrichment
          .extractSchema(event, unstructEvent)
          .map {
            case Some(schemaKey) =>
              event.event_vendor = schemaKey.vendor
              event.event_name = schemaKey.name
              event.event_format = schemaKey.format
              event.event_version = schemaKey.version.asString
              Nil
            case None => Nil
          }
          .leftMap(NonEmptyList.one(_))
    }

  // Execute the JavaScript scripting enrichment
  def getJsScript[F[_]: Applicative](
    javascriptScript: JavascriptScriptEnrichment,
    headers: List[String],
    maxJsonDepth: Int
  ): EStateT[F, Unit] =
    EStateT.fromJsEnrichmentResult {
      case (event, derivedContexts) =>
        ME.formatContexts(derivedContexts).foreach(c => event.derived_contexts = c)
        javascriptScript.process(event, headers, maxJsonDepth)
    }

  def headerContexts[F[_]: Applicative, A](
    headers: List[String],
    enrichment: Option[A],
    f: (A, List[String]) => List[SelfDescribingData[Json]]
  ): EStateT[F, Unit] =
    EStateT.fromEither[F] {
      case (_, _) =>
        enrichment match {
          case Some(e) => f(e, headers).asRight
          case None => Nil.asRight
        }
    }

  // Fetch weather context
  def getWeatherContext[F[_]: Monad](weather: Option[WeatherEnrichment[F]]): EStateT[F, Unit] =
    EStateT.fromEitherF {
      case (event, _) =>
        weather match {
          case Some(we) =>
            we.getWeatherContext(
              Option(event.geo_latitude),
              Option(event.geo_longitude),
              Option(event.derived_tstamp).map(EventEnrichments.fromTimestamp)
            ).map(_.map(List(_)))
          case None => Monad[F].pure(Nil.asRight)
        }
    }

  def getYauaaContext[F[_]: Applicative](yauaa: Option[YauaaEnrichment], headers: List[String]): EStateT[F, Unit] =
    EStateT.fromEither {
      case (event, _) =>
        yauaa.map(_.getYauaaContext(event.useragent, headers)).toList.asRight
    }

  // Derive some contexts with custom SQL Query enrichment
  def getSqlQueryContexts[F[_]: Monad](
    inputContexts: List[SelfDescribingData[Json]],
    unstructEvent: Option[SelfDescribingData[Json]],
    sqlQuery: Option[SqlQueryEnrichment[F]]
  ): EStateT[F, Unit] =
    EStateT.fromEitherF {
      case (event, derivedContexts) =>
        sqlQuery match {
          case Some(enrichment) =>
            enrichment.lookup(event, derivedContexts, inputContexts, unstructEvent).map(_.toEither)
          case None =>
            List.empty[SelfDescribingData[Json]].asRight.pure[F]
        }
    }

  // Derive some contexts with custom API Request enrichment
  def getApiRequestContexts[F[_]: Monad](
    inputContexts: List[SelfDescribingData[Json]],
    unstructEvent: Option[SelfDescribingData[Json]],
    apiRequest: Option[ApiRequestEnrichment[F]]
  ): EStateT[F, Unit] =
    EStateT.fromEitherF {
      case (event, derivedContexts) =>
        apiRequest match {
          case Some(enrichment) =>
            enrichment.lookup(event, derivedContexts, inputContexts, unstructEvent).map(_.toEither)
          case None =>
            List.empty[SelfDescribingData[Json]].asRight.pure[F]
        }
    }

  def piiTransform[F[_]: Applicative](piiPseudonymizer: Option[PiiPseudonymizerEnrichment], headers: List[String]): EStateT[F, Unit] =
    EStateT.fromEither {
      case (event, _) =>
        piiPseudonymizer match {
          case Some(pseudonymizer) =>
            pseudonymizer.transformer(event, headers).foreach(p => event.pii = p.asString)
            Nil.asRight
          case None =>
            Nil.asRight
        }
    }
}
