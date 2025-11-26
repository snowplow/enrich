/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.core

import com.github.dockerjava.core.DockerClientBuilder
import com.github.dockerjava.api.command.PullImageResultCallback
import com.github.dockerjava.api.model.PullResponseItem

object DockerPull {

  private lazy val client = DockerClientBuilder.getInstance().build()

  /**
   * A blocking operation called from `beforeAll()` on the main thread to pull container images
   * before test execution starts. This ensures pull time is not counted towards test timeout.
   * Reuses a single Docker client across all pull operations.
   */
  def pull(image: String, tag: String): Unit =
    client
      .pullImageCmd(image)
      .withTag(tag)
      .withPlatform("linux/amd64")
      .exec(new PullImageResultCallback() {
        override def onNext(item: PullResponseItem): Unit = {
          println(s"$image: ${item.getStatus}")
          super.onNext(item)
        }
      })
      .awaitCompletion()
      .onComplete()
}
