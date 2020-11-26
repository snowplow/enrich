/*
 * Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.fs2.io

import java.util.Base64

import cats.effect.IO

import com.snowplowanalytics.snowplow.enrich.fs2.{Payload}

import org.specs2.mutable.Specification

class TracingSpec extends Specification {
  "enrich" should {
    "do something" in {
      val input = "CwBkAAAACTEyNy4wLjAuMQoAyAAAAXWeB+ZeCwDSAAAABVVURi04CwDcAAAAEXNzYy0yLjAuMC1zdGRvdXQkCwEsAAAAC2N1cmwvNy42OC4wCwFAAAAAAi9pCwFKAAAAa2U9cHYmcGFnZT1Sb290JTIwUkVBRE1FJnVybD1odHRwJTNBJTJGJTJGZ2l0aHViLmNvbSUyRnNub3dwbG93JTJGc25vd3Bsb3cmYWlkPXNub3dwbG93JnA9d2ViJnR2PW5vLWpzLTAuMS4wDwFeCwAAAAcAAAAbVGltZW91dC1BY2Nlc3M6IDxmdW5jdGlvbjE+AAAAFEhvc3Q6IGxvY2FsaG9zdDo5MDkwAAAAF1VzZXItQWdlbnQ6IGN1cmwvNy42OC4wAAAAC0FjY2VwdDogKi8qAAAAD1gtQjMtU2FtcGxlZDogMQAAAB5YLUIzLVRyYWNlSWQ6IDVhOGFhZmMzZjVjMDg2YTgAAAAdWC1CMy1TcGFuSWQ6IDVhOGFhZmMzZjVjMDg2YTgLAZAAAAAJbG9jYWxob3N0CwGaAAAAJGViNGRlMGUzLWY3MTktNDFkYy1hMDQwLWM0NTIwYWJhNjRkNgt6aQAAAEFpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9Db2xsZWN0b3JQYXlsb2FkL3RocmlmdC8xLTAtMAA="
      val _ = Payload(Base64.getDecoder.decode(input), IO.unit)
      ok
    }
  }
}
