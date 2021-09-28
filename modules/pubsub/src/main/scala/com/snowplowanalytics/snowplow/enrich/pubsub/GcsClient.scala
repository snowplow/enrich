/*
 * Copyright (c) 2021-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.pubsub

import java.net.URI

import cats.effect.{Blocker, ConcurrentEffect, ContextShift}

import fs2.Stream

import blobstore.gcs.GcsStore
import blobstore.Path

import com.google.cloud.storage.StorageOptions

import com.snowplowanalytics.snowplow.enrich.common.fs2.io.Client

object GcsClient {

  def mk[F[_]: ConcurrentEffect: ContextShift](blocker: Blocker): Client[F] =
    new Client[F] {
      val prefixes = List("gs")

      val service = StorageOptions.getDefaultInstance.getService
      val store = GcsStore(service, blocker, List.empty)

      def download(uri: URI): Stream[F, Byte] =
        store.get(Path(uri.toString), 16 * 1024)
    }
}
