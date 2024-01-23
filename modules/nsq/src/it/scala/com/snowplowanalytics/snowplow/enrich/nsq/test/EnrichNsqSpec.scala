/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.nsq.test

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.kernel.Ref

import cats.effect.testing.specs2.CatsEffect

import org.specs2.mutable.Specification

import Utils._

class EnrichNsqSpec extends Specification with CatsEffect {

  override protected val Timeout = 10.minutes

  "enrich-nsq" should {
    "emit the correct number of enriched events and bad rows" in {
      val nbGood = 100l
      val nbBad = 10l
      mkResources[IO].use {
        case (topology, sink) =>
          for {
            refGood <- Ref.of[IO, AggregateGood](Nil)
            refBad <- Ref.of[IO, AggregateBad](Nil)
            _ <-
              generateEvents(sink, nbGood, nbBad, topology)
                .merge(consume(refGood, refBad, topology))
                .interruptAfter(30.seconds)
                .attempt
                .compile
                .drain
            aggregateGood <- refGood.get
            aggregateBad <- refBad.get
          } yield {
            aggregateGood.size === nbGood
            aggregateBad.size === nbBad
          }
      }
    }
  }
}
