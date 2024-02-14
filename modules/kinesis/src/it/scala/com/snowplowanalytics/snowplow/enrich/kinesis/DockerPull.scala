/*
 * Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.kinesis

import com.github.dockerjava.core.DockerClientBuilder
import com.github.dockerjava.api.command.PullImageResultCallback
import com.github.dockerjava.api.model.PullResponseItem

object DockerPull {

  /**
   * A blocking operation that runs on main thread to pull container image before `CatsResource` is
   * created. This operation is then not counted towards test timeout.
   */
  def pull(image: String, tag: String): Unit =
    DockerClientBuilder
      .getInstance()
      .build()
      .pullImageCmd(image)
      .withTag(tag)
      .withPlatform("linux/amd64")
      .exec(new PullImageResultCallback() {
        override def onNext(item: PullResponseItem) = {
          println(s"$image: ${item.getStatus()}")
          super.onNext(item)
        }
      })
      .awaitCompletion()
      .onComplete()
}
