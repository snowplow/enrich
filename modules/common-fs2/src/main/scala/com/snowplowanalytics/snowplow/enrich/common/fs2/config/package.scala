/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2

import java.nio.file.Path

import _root_.io.circe.generic.extras.Configuration

package object config {

  type EncodedHoconOrPath = Either[Base64Hocon, Path]

  private[config] implicit def customCodecConfig: Configuration =
    Configuration.default.withDiscriminator("type")

}
