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
package com.snowplowanalytics.snowplow.enrich.common.loaders

import java.util.UUID

import cats.data.NonEmptyList
import cats.syntax.option._

import org.apache.commons.codec.binary.Base64
import org.joda.time.DateTime

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import org.specs2.matcher.ValidatedMatchers

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, FailureDetails, Payload, Processor}

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers.toNameValuePairs
import com.snowplowanalytics.snowplow.enrich.common.loaders.ThriftLoaderSpec._

class ThriftLoaderSpec extends Specification with ValidatedMatchers with ScalaCheck {
  "toCollectorPayload" should {
    "tolerate fake tracker protocol GET parameters" >> {
      val raw =
        "CgABAAABQ5iGqAYLABQAAAAQc3NjLTAuMC4xLVN0ZG91dAsAHgAAAAVVVEYtOAsAKAAAAAkxMjcuMC4wLjEMACkIAAEAAAABCAACAAAAAQsAAwAAABh0ZXN0UGFyYW09MyZ0ZXN0UGFyYW0yPTQACwAtAAAACTEyNy4wLjAuMQsAMgAAAGhNb3ppbGxhLzUuMCAoWDExOyBMaW51eCB4ODZfNjQpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hUTUwsIGxpa2UgR2Vja28pIENocm9tZS8zMS4wLjE2NTAuNjMgU2FmYXJpLzUzNy4zNg8ARgsAAAAIAAAAL0Nvb2tpZTogc3A9YzVmM2EwOWYtNzVmOC00MzA5LWJlYzUtZmVhNTYwZjc4NDU1AAAAGkFjY2VwdC1MYW5ndWFnZTogZW4tVVMsIGVuAAAAJEFjY2VwdC1FbmNvZGluZzogZ3ppcCwgZGVmbGF0ZSwgc2RjaAAAAHRVc2VyLUFnZW50OiBNb3ppbGxhLzUuMCAoWDExOyBMaW51eCB4ODZfNjQpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hUTUwsIGxpa2UgR2Vja28pIENocm9tZS8zMS4wLjE2NTAuNjMgU2FmYXJpLzUzNy4zNgAAAFZBY2NlcHQ6IHRleHQvaHRtbCwgYXBwbGljYXRpb24veGh0bWwreG1sLCBhcHBsaWNhdGlvbi94bWw7cT0wLjksIGltYWdlL3dlYnAsICovKjtxPTAuOAAAABhDYWNoZS1Db250cm9sOiBtYXgtYWdlPTAAAAAWQ29ubmVjdGlvbjoga2VlcC1hbGl2ZQAAABRIb3N0OiAxMjcuMC4wLjE6ODA4MAsAUAAAACRjNWYzYTA5Zi03NWY4LTQzMDktYmVjNS1mZWE1NjBmNzg0NTUA"
      val result = ThriftLoader.toCollectorPayload(Base64.decodeBase64(raw), ThriftLoaderSpec.Process)

      val context = CollectorPayload.Context(
        timestamp = DateTime.parse("2014-01-16T00:49:58.278+00:00").some,
        ipAddress = "127.0.0.1".some,
        useragent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36".some,
        refererUri = None,
        headers = List(
          "Cookie: sp=c5f3a09f-75f8-4309-bec5-fea560f78455",
          "Accept-Language: en-US, en",
          "Accept-Encoding: gzip, deflate, sdch",
          "User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36",
          "Accept: text/html, application/xhtml+xml, application/xml;q=0.9, image/webp, */*;q=0.8",
          "Cache-Control: max-age=0",
          "Connection: keep-alive",
          "Host: 127.0.0.1:8080"
        ),
        userId = UUID.fromString("c5f3a09f-75f8-4309-bec5-fea560f78455").some
      )
      val expected = CollectorPayload(
        api = ThriftLoaderSpec.Api,
        querystring = toNameValuePairs("testParam" -> "3", "testParam2" -> "4"),
        body = None,
        contentType = None,
        source = CollectorPayload.Source(ThriftLoaderSpec.Collector, ThriftLoaderSpec.Encoding, "127.0.0.1".some),
        context = context
      )

      result must beValid(expected.some)
    }

    "parse valid page ping GET payload" >> {
      val raw =
        "CgABAAABQ9pNXggLABQAAAAQc3NjLTAuMC4xLVN0ZG91dAsAHgAAAAVVVEYtOAsAKAAAAAgxMC4wLjIuMgwAKQgAAQAAAAEIAAIAAAABCwADAAACZmU9cHAmcGFnZT1Bc3luY2hyb25vdXMrd2Vic2l0ZS93ZWJhcHArZXhhbXBsZXMrZm9yK3Nub3dwbG93LmpzJnBwX21peD0wJnBwX21heD0wJnBwX21peT0wJnBwX21heT0wJmNvPSU3QiUyMnBhZ2UlMjI6JTdCJTIycGFnZV90eXBlJTIyOiUyMnRlc3QlMjIsJTIybGFzdF91cGRhdGVkJHRtcyUyMjoxMzkzMzcyODAwMDAwJTdELCUyMnVzZXIlMjI6JTdCJTIydXNlcl90eXBlJTIyOiUyMnRlc3RlciUyMiU3RCU3RCZkdG09MTM5MDkzNjkzODg1NSZ0aWQ9Nzk3NzQzJnZwPTI1NjB4OTYxJmRzPTI1NjB4OTYxJnZpZD03JmR1aWQ9M2MxNzU3NTQ0ZTM5YmNhNCZwPW1vYiZ0dj1qcy0wLjEzLjEmZnA9MjY5NTkzMDgwMyZhaWQ9Q0ZlMjNhJmxhbmc9ZW4tVVMmY3M9VVRGLTgmdHo9RXVyb3BlL0xvbmRvbiZ1aWQ9YWxleCsxMjMmZl9wZGY9MCZmX3F0PTEmZl9yZWFscD0wJmZfd21hPTAmZl9kaXI9MCZmX2ZsYT0xJmZfamF2YT0wJmZfZ2VhcnM9MCZmX2FnPTAmcmVzPTI1NjB4MTQ0MCZjZD0yNCZjb29raWU9MSZ1cmw9ZmlsZTovL2ZpbGU6Ly8vVXNlcnMvYWxleC9EZXZlbG9wbWVudC9kZXYtZW52aXJvbm1lbnQvZGVtby8xLXRyYWNrZXIvZXZlbnRzLmh0bWwvb3ZlcnJpZGRlbi11cmwvAAsALQAAAAlsb2NhbGhvc3QLADIAAABRTW96aWxsYS81LjAgKE1hY2ludG9zaDsgSW50ZWwgTWFjIE9TIFggMTAuOTsgcnY6MjYuMCkgR2Vja28vMjAxMDAxMDEgRmlyZWZveC8yNi4wDwBGCwAAAAcAAAAWQ29ubmVjdGlvbjoga2VlcC1hbGl2ZQAAAnBDb29raWU6IF9fdXRtYT0xMTE4NzIyODEuODc4MDg0NDg3LjEzOTAyMzcxMDcuMTM5MDg0ODQ4Ny4xMzkwOTMxNTIxLjY7IF9fdXRtej0xMTE4NzIyODEuMTM5MDIzNzEwNy4xLjEudXRtY3NyPShkaXJlY3QpfHV0bWNjbj0oZGlyZWN0KXx1dG1jbWQ9KG5vbmUpOyBfc3BfaWQuMWZmZj1iODlhNmZhNjMxZWVmYWMyLjEzOTAyMzcxMDcuNi4xMzkwOTMxNTQ1LjEzOTA4NDg2NDE7IGhibGlkPUNQamp1aHZGMDV6a3RQN0o3TTVWbzNOSUdQTEp5MVNGOyBvbGZzaz1vbGZzazU2MjkyMzYzNTYxNzU1NDsgX191dG1jPTExMTg3MjI4MTsgd2NzaWQ9dU1sb2cxUUpWRDdqdWhGWjdNNVZvQkN5UFB5aUJ5U1M7IF9va2x2PTEzOTA5MzE1ODU0NDUlMkN1TWxvZzFRSlZEN2p1aEZaN001Vm9CQ3lQUHlpQnlTUzsgX29rPTk3NTItNTAzLTEwLTUyMjc7IF9va2JrPWNkNCUzRHRydWUlMkN2aTUlM0QwJTJDdmk0JTNEMTM5MDkzMTUyMTEyMyUyQ3ZpMyUzRGFjdGl2ZSUyQ3ZpMiUzRGZhbHNlJTJDdmkxJTNEZmFsc2UlMkNjZDglM0RjaGF0JTJDY2Q2JTNEMCUyQ2NkNSUzRGF3YXklMkNjZDMlM0RmYWxzZSUyQ2NkMiUzRDAlMkNjZDElM0QwJTJDOyBzcD03NWExMzU4My01Yzk5LTQwZTMtODFmYy01NDEwODRkZmM3ODQAAAAeQWNjZXB0LUVuY29kaW5nOiBnemlwLCBkZWZsYXRlAAAAGkFjY2VwdC1MYW5ndWFnZTogZW4tVVMsIGVuAAAAK0FjY2VwdDogaW1hZ2UvcG5nLCBpbWFnZS8qO3E9MC44LCAqLyo7cT0wLjUAAABdVXNlci1BZ2VudDogTW96aWxsYS81LjAgKE1hY2ludG9zaDsgSW50ZWwgTWFjIE9TIFggMTAuOTsgcnY6MjYuMCkgR2Vja28vMjAxMDAxMDEgRmlyZWZveC8yNi4wAAAAFEhvc3Q6IGxvY2FsaG9zdDo0MDAxCwBQAAAAJDc1YTEzNTgzLTVjOTktNDBlMy04MWZjLTU0MTA4NGRmYzc4NAA="
      val result = ThriftLoader.toCollectorPayload(Base64.decodeBase64(raw), ThriftLoaderSpec.Process)

      val context = CollectorPayload.Context(
        timestamp = DateTime.parse("2014-01-28T19:22:20.040+00:00").some,
        ipAddress = "10.0.2.2".some,
        useragent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:26.0) Gecko/20100101 Firefox/26.0".some,
        refererUri = None,
        headers = List(
          "Connection: keep-alive",
          "Cookie: __utma=111872281.878084487.1390237107.1390848487.1390931521.6; __utmz=111872281.1390237107.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _sp_id.1fff=b89a6fa631eefac2.1390237107.6.1390931545.1390848641; hblid=CPjjuhvF05zktP7J7M5Vo3NIGPLJy1SF; olfsk=olfsk562923635617554; __utmc=111872281; wcsid=uMlog1QJVD7juhFZ7M5VoBCyPPyiBySS; _oklv=1390931585445%2CuMlog1QJVD7juhFZ7M5VoBCyPPyiBySS; _ok=9752-503-10-5227; _okbk=cd4%3Dtrue%2Cvi5%3D0%2Cvi4%3D1390931521123%2Cvi3%3Dactive%2Cvi2%3Dfalse%2Cvi1%3Dfalse%2Ccd8%3Dchat%2Ccd6%3D0%2Ccd5%3Daway%2Ccd3%3Dfalse%2Ccd2%3D0%2Ccd1%3D0%2C; sp=75a13583-5c99-40e3-81fc-541084dfc784",
          "Accept-Encoding: gzip, deflate",
          "Accept-Language: en-US, en",
          "Accept: image/png, image/*;q=0.8, */*;q=0.5",
          "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:26.0) Gecko/20100101 Firefox/26.0",
          "Host: localhost:4001"
        ),
        userId = UUID.fromString("75a13583-5c99-40e3-81fc-541084dfc784").some
      )

      val expected = CollectorPayload(
        api = ThriftLoaderSpec.Api,
        querystring = toNameValuePairs(
          "e" -> "pp",
          "page" -> "Asynchronous website/webapp examples for snowplow.js",
          "pp_mix" -> "0",
          "pp_max" -> "0",
          "pp_miy" -> "0",
          "pp_may" -> "0",
          "co" -> """{"page":{"page_type":"test","last_updated$tms":1393372800000},"user":{"user_type":"tester"}}""",
          "dtm" -> "1390936938855",
          "tid" -> "797743",
          "vp" -> "2560x961",
          "ds" -> "2560x961",
          "vid" -> "7",
          "duid" -> "3c1757544e39bca4",
          "p" -> "mob",
          "tv" -> "js-0.13.1",
          "fp" -> "2695930803",
          "aid" -> "CFe23a",
          "lang" -> "en-US",
          "cs" -> "UTF-8",
          "tz" -> "Europe/London",
          "uid" -> "alex 123",
          "f_pdf" -> "0",
          "f_qt" -> "1",
          "f_realp" -> "0",
          "f_wma" -> "0",
          "f_dir" -> "0",
          "f_fla" -> "1",
          "f_java" -> "0",
          "f_gears" -> "0",
          "f_ag" -> "0",
          "res" -> "2560x1440",
          "cd" -> "24",
          "cookie" -> "1",
          "url" -> "file://file:///Users/alex/Development/dev-environment/demo/1-tracker/events.html/overridden-url/"
        ),
        body = None,
        contentType = None,
        source = CollectorPayload.Source(ThriftLoaderSpec.Collector, ThriftLoaderSpec.Encoding, "localhost".some),
        context = context
      )

      result must beValid(expected.some)
    }

    "parse valid unstructured event GET payload" >> {
      val raw =
        "CgABAAABQ9qNGa4LABQAAAAQc3NjLTAuMC4xLVN0ZG91dAsAHgAAAAVVVEYtOAsAKAAAAAgxMC4wLjIuMgwAKQgAAQAAAAEIAAIAAAABCwADAAACeWU9dWUmdWVfbmE9Vmlld2VkK1Byb2R1Y3QmdWVfcHI9JTdCJTIycHJvZHVjdF9pZCUyMjolMjJBU08wMTA0MyUyMiwlMjJjYXRlZ29yeSUyMjolMjJEcmVzc2VzJTIyLCUyMmJyYW5kJTIyOiUyMkFDTUUlMjIsJTIycmV0dXJuaW5nJTIyOnRydWUsJTIycHJpY2UlMjI6NDkuOTUsJTIyc2l6ZXMlMjI6JTVCJTIyeHMlMjIsJTIycyUyMiwlMjJsJTIyLCUyMnhsJTIyLCUyMnh4bCUyMiU1RCwlMjJhdmFpbGFibGVfc2luY2UkZHQlMjI6MTU4MDElN0QmZHRtPTEzOTA5NDExMTUyNjMmdGlkPTY0NzYxNSZ2cD0yNTYweDk2MSZkcz0yNTYweDk2MSZ2aWQ9OCZkdWlkPTNjMTc1NzU0NGUzOWJjYTQmcD1tb2ImdHY9anMtMC4xMy4xJmZwPTI2OTU5MzA4MDMmYWlkPUNGZTIzYSZsYW5nPWVuLVVTJmNzPVVURi04JnR6PUV1cm9wZS9Mb25kb24mdWlkPWFsZXgrMTIzJmZfcGRmPTAmZl9xdD0xJmZfcmVhbHA9MCZmX3dtYT0wJmZfZGlyPTAmZl9mbGE9MSZmX2phdmE9MCZmX2dlYXJzPTAmZl9hZz0wJnJlcz0yNTYweDE0NDAmY2Q9MjQmY29va2llPTEmdXJsPWZpbGU6Ly9maWxlOi8vL1VzZXJzL2FsZXgvRGV2ZWxvcG1lbnQvZGV2LWVudmlyb25tZW50L2RlbW8vMS10cmFja2VyL2V2ZW50cy5odG1sL292ZXJyaWRkZW4tdXJsLwALAC0AAAAJbG9jYWxob3N0CwAyAAAAUU1vemlsbGEvNS4wIChNYWNpbnRvc2g7IEludGVsIE1hYyBPUyBYIDEwLjk7IHJ2OjI2LjApIEdlY2tvLzIwMTAwMTAxIEZpcmVmb3gvMjYuMA8ARgsAAAAHAAAAFkNvbm5lY3Rpb246IGtlZXAtYWxpdmUAAAJwQ29va2llOiBfX3V0bWE9MTExODcyMjgxLjg3ODA4NDQ4Ny4xMzkwMjM3MTA3LjEzOTA4NDg0ODcuMTM5MDkzMTUyMS42OyBfX3V0bXo9MTExODcyMjgxLjEzOTAyMzcxMDcuMS4xLnV0bWNzcj0oZGlyZWN0KXx1dG1jY249KGRpcmVjdCl8dXRtY21kPShub25lKTsgX3NwX2lkLjFmZmY9Yjg5YTZmYTYzMWVlZmFjMi4xMzkwMjM3MTA3LjYuMTM5MDkzMTU0NS4xMzkwODQ4NjQxOyBoYmxpZD1DUGpqdWh2RjA1emt0UDdKN001Vm8zTklHUExKeTFTRjsgb2xmc2s9b2xmc2s1NjI5MjM2MzU2MTc1NTQ7IF9fdXRtYz0xMTE4NzIyODE7IHdjc2lkPXVNbG9nMVFKVkQ3anVoRlo3TTVWb0JDeVBQeWlCeVNTOyBfb2tsdj0xMzkwOTMxNTg1NDQ1JTJDdU1sb2cxUUpWRDdqdWhGWjdNNVZvQkN5UFB5aUJ5U1M7IF9vaz05NzUyLTUwMy0xMC01MjI3OyBfb2tiaz1jZDQlM0R0cnVlJTJDdmk1JTNEMCUyQ3ZpNCUzRDEzOTA5MzE1MjExMjMlMkN2aTMlM0RhY3RpdmUlMkN2aTIlM0RmYWxzZSUyQ3ZpMSUzRGZhbHNlJTJDY2Q4JTNEY2hhdCUyQ2NkNiUzRDAlMkNjZDUlM0Rhd2F5JTJDY2QzJTNEZmFsc2UlMkNjZDIlM0QwJTJDY2QxJTNEMCUyQzsgc3A9NzVhMTM1ODMtNWM5OS00MGUzLTgxZmMtNTQxMDg0ZGZjNzg0AAAAHkFjY2VwdC1FbmNvZGluZzogZ3ppcCwgZGVmbGF0ZQAAABpBY2NlcHQtTGFuZ3VhZ2U6IGVuLVVTLCBlbgAAACtBY2NlcHQ6IGltYWdlL3BuZywgaW1hZ2UvKjtxPTAuOCwgKi8qO3E9MC41AAAAXVVzZXItQWdlbnQ6IE1vemlsbGEvNS4wIChNYWNpbnRvc2g7IEludGVsIE1hYyBPUyBYIDEwLjk7IHJ2OjI2LjApIEdlY2tvLzIwMTAwMTAxIEZpcmVmb3gvMjYuMAAAABRIb3N0OiBsb2NhbGhvc3Q6NDAwMQsAUAAAACQ3NWExMzU4My01Yzk5LTQwZTMtODFmYy01NDEwODRkZmM3ODQA"
      val result = ThriftLoader.toCollectorPayload(Base64.decodeBase64(raw), ThriftLoaderSpec.Process)

      val context = CollectorPayload.Context(
        timestamp = DateTime.parse("2014-01-28T20:31:56.846+00:00").some,
        ipAddress = "10.0.2.2".some,
        useragent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:26.0) Gecko/20100101 Firefox/26.0".some,
        refererUri = None,
        headers = List(
          "Connection: keep-alive",
          "Cookie: __utma=111872281.878084487.1390237107.1390848487.1390931521.6; __utmz=111872281.1390237107.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _sp_id.1fff=b89a6fa631eefac2.1390237107.6.1390931545.1390848641; hblid=CPjjuhvF05zktP7J7M5Vo3NIGPLJy1SF; olfsk=olfsk562923635617554; __utmc=111872281; wcsid=uMlog1QJVD7juhFZ7M5VoBCyPPyiBySS; _oklv=1390931585445%2CuMlog1QJVD7juhFZ7M5VoBCyPPyiBySS; _ok=9752-503-10-5227; _okbk=cd4%3Dtrue%2Cvi5%3D0%2Cvi4%3D1390931521123%2Cvi3%3Dactive%2Cvi2%3Dfalse%2Cvi1%3Dfalse%2Ccd8%3Dchat%2Ccd6%3D0%2Ccd5%3Daway%2Ccd3%3Dfalse%2Ccd2%3D0%2Ccd1%3D0%2C; sp=75a13583-5c99-40e3-81fc-541084dfc784",
          "Accept-Encoding: gzip, deflate",
          "Accept-Language: en-US, en",
          "Accept: image/png, image/*;q=0.8, */*;q=0.5",
          "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:26.0) Gecko/20100101 Firefox/26.0",
          "Host: localhost:4001"
        ),
        userId = UUID.fromString("75a13583-5c99-40e3-81fc-541084dfc784").some
      )
      val expected = CollectorPayload(
        api = ThriftLoaderSpec.Api,
        querystring = toNameValuePairs(
          "e" -> "ue",
          "ue_na" -> "Viewed Product",
          "ue_pr" -> """{"product_id":"ASO01043","category":"Dresses","brand":"ACME","returning":true,"price":49.95,"sizes":["xs","s","l","xl","xxl"],"available_since$dt":15801}""",
          "dtm" -> "1390941115263",
          "tid" -> "647615",
          "vp" -> "2560x961",
          "ds" -> "2560x961",
          "vid" -> "8",
          "duid" -> "3c1757544e39bca4",
          "p" -> "mob",
          "tv" -> "js-0.13.1",
          "fp" -> "2695930803",
          "aid" -> "CFe23a",
          "lang" -> "en-US",
          "cs" -> "UTF-8",
          "tz" -> "Europe/London",
          "uid" -> "alex 123",
          "f_pdf" -> "0",
          "f_qt" -> "1",
          "f_realp" -> "0",
          "f_wma" -> "0",
          "f_dir" -> "0",
          "f_fla" -> "1",
          "f_java" -> "0",
          "f_gears" -> "0",
          "f_ag" -> "0",
          "res" -> "2560x1440",
          "cd" -> "24",
          "cookie" -> "1",
          "url" -> "file://file:///Users/alex/Development/dev-environment/demo/1-tracker/events.html/overridden-url/"
        ),
        body = None,
        contentType = None,
        source = CollectorPayload.Source(ThriftLoaderSpec.Collector, ThriftLoaderSpec.Encoding, "localhost".some),
        context = context
      )

      result must beValid(expected.some)
    }

    "parse valid parameterless payload" >> {
      val raw =
        "CgABAAABQ9o8zYULABQAAAAQc3NjLTAuMC4xLVN0ZG91dAsAHgAAAAVVVEYtOAsAKAAAAAgxMC4wLjIuMgwAKQgAAQAAAAEIAAIAAAABAAsALQAAAAlsb2NhbGhvc3QLADIAAABRTW96aWxsYS81LjAgKE1hY2ludG9zaDsgSW50ZWwgTWFjIE9TIFggMTAuOTsgcnY6MjYuMCkgR2Vja28vMjAxMDAxMDEgRmlyZWZveC8yNi4wDwBGCwAAAAgAAAAYQ2FjaGUtQ29udHJvbDogbWF4LWFnZT0wAAAAFkNvbm5lY3Rpb246IGtlZXAtYWxpdmUAAAJwQ29va2llOiBfX3V0bWE9MTExODcyMjgxLjg3ODA4NDQ4Ny4xMzkwMjM3MTA3LjEzOTA4NDg0ODcuMTM5MDkzMTUyMS42OyBfX3V0bXo9MTExODcyMjgxLjEzOTAyMzcxMDcuMS4xLnV0bWNzcj0oZGlyZWN0KXx1dG1jY249KGRpcmVjdCl8dXRtY21kPShub25lKTsgX3NwX2lkLjFmZmY9Yjg5YTZmYTYzMWVlZmFjMi4xMzkwMjM3MTA3LjYuMTM5MDkzMTU0NS4xMzkwODQ4NjQxOyBoYmxpZD1DUGpqdWh2RjA1emt0UDdKN001Vm8zTklHUExKeTFTRjsgb2xmc2s9b2xmc2s1NjI5MjM2MzU2MTc1NTQ7IF9fdXRtYz0xMTE4NzIyODE7IHdjc2lkPXVNbG9nMVFKVkQ3anVoRlo3TTVWb0JDeVBQeWlCeVNTOyBfb2tsdj0xMzkwOTMxNTg1NDQ1JTJDdU1sb2cxUUpWRDdqdWhGWjdNNVZvQkN5UFB5aUJ5U1M7IF9vaz05NzUyLTUwMy0xMC01MjI3OyBfb2tiaz1jZDQlM0R0cnVlJTJDdmk1JTNEMCUyQ3ZpNCUzRDEzOTA5MzE1MjExMjMlMkN2aTMlM0RhY3RpdmUlMkN2aTIlM0RmYWxzZSUyQ3ZpMSUzRGZhbHNlJTJDY2Q4JTNEY2hhdCUyQ2NkNiUzRDAlMkNjZDUlM0Rhd2F5JTJDY2QzJTNEZmFsc2UlMkNjZDIlM0QwJTJDY2QxJTNEMCUyQzsgc3A9NzVhMTM1ODMtNWM5OS00MGUzLTgxZmMtNTQxMDg0ZGZjNzg0AAAAHkFjY2VwdC1FbmNvZGluZzogZ3ppcCwgZGVmbGF0ZQAAABpBY2NlcHQtTGFuZ3VhZ2U6IGVuLVVTLCBlbgAAAEpBY2NlcHQ6IHRleHQvaHRtbCwgYXBwbGljYXRpb24veGh0bWwreG1sLCBhcHBsaWNhdGlvbi94bWw7cT0wLjksICovKjtxPTAuOAAAAF1Vc2VyLUFnZW50OiBNb3ppbGxhLzUuMCAoTWFjaW50b3NoOyBJbnRlbCBNYWMgT1MgWCAxMC45OyBydjoyNi4wKSBHZWNrby8yMDEwMDEwMSBGaXJlZm94LzI2LjAAAAAUSG9zdDogbG9jYWxob3N0OjQwMDELAFAAAAAkNzVhMTM1ODMtNWM5OS00MGUzLTgxZmMtNTQxMDg0ZGZjNzg0AA=="
      val result = ThriftLoader.toCollectorPayload(Base64.decodeBase64(raw), ThriftLoaderSpec.Process)

      val context = CollectorPayload.Context(
        timestamp = DateTime.parse("2014-01-28T19:04:14.469+00:00").some,
        ipAddress = "10.0.2.2".some,
        useragent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:26.0) Gecko/20100101 Firefox/26.0".some,
        refererUri = None,
        headers = List(
          "Cache-Control: max-age=0",
          "Connection: keep-alive",
          "Cookie: __utma=111872281.878084487.1390237107.1390848487.1390931521.6; __utmz=111872281.1390237107.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _sp_id.1fff=b89a6fa631eefac2.1390237107.6.1390931545.1390848641; hblid=CPjjuhvF05zktP7J7M5Vo3NIGPLJy1SF; olfsk=olfsk562923635617554; __utmc=111872281; wcsid=uMlog1QJVD7juhFZ7M5VoBCyPPyiBySS; _oklv=1390931585445%2CuMlog1QJVD7juhFZ7M5VoBCyPPyiBySS; _ok=9752-503-10-5227; _okbk=cd4%3Dtrue%2Cvi5%3D0%2Cvi4%3D1390931521123%2Cvi3%3Dactive%2Cvi2%3Dfalse%2Cvi1%3Dfalse%2Ccd8%3Dchat%2Ccd6%3D0%2Ccd5%3Daway%2Ccd3%3Dfalse%2Ccd2%3D0%2Ccd1%3D0%2C; sp=75a13583-5c99-40e3-81fc-541084dfc784",
          "Accept-Encoding: gzip, deflate",
          "Accept-Language: en-US, en",
          "Accept: text/html, application/xhtml+xml, application/xml;q=0.9, */*;q=0.8",
          "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:26.0) Gecko/20100101 Firefox/26.0",
          "Host: localhost:4001"
        ),
        userId = UUID.fromString("75a13583-5c99-40e3-81fc-541084dfc784").some
      )

      val expected = CollectorPayload(
        api = ThriftLoaderSpec.Api,
        querystring = toNameValuePairs(),
        contentType = None,
        body = None,
        source = CollectorPayload.Source(ThriftLoaderSpec.Collector, ThriftLoaderSpec.Encoding, "localhost".some),
        context = context
      )

      result must beValid(expected.some)
    }

    "parse base64 encoded event" >> {
      val raw =
        "CgABAAABQ9qNGa4LABQAAAAQc3NjLTAuMC4xLVN0ZG91dAsAHgAAAAVVVEYtOAsAKAAAAAgxMC4wLjIuMgwAKQgAAQAAAAEIAAIAAAABCwADAAACeWU9dWUmdWVfbmE9Vmlld2VkK1Byb2R1Y3QmdWVfcHI9JTdCJTIycHJvZHVjdF9pZCUyMjolMjJBU08wMTA0MyUyMiwlMjJjYXRlZ29yeSUyMjolMjJEcmVzc2VzJTIyLCUyMmJyYW5kJTIyOiUyMkFDTUUlMjIsJTIycmV0dXJuaW5nJTIyOnRydWUsJTIycHJpY2UlMjI6NDkuOTUsJTIyc2l6ZXMlMjI6JTVCJTIyeHMlMjIsJTIycyUyMiwlMjJsJTIyLCUyMnhsJTIyLCUyMnh4bCUyMiU1RCwlMjJhdmFpbGFibGVfc2luY2UkZHQlMjI6MTU4MDElN0QmZHRtPTEzOTA5NDExMTUyNjMmdGlkPTY0NzYxNSZ2cD0yNTYweDk2MSZkcz0yNTYweDk2MSZ2aWQ9OCZkdWlkPTNjMTc1NzU0NGUzOWJjYTQmcD1tb2ImdHY9anMtMC4xMy4xJmZwPTI2OTU5MzA4MDMmYWlkPUNGZTIzYSZsYW5nPWVuLVVTJmNzPVVURi04JnR6PUV1cm9wZS9Mb25kb24mdWlkPWFsZXgrMTIzJmZfcGRmPTAmZl9xdD0xJmZfcmVhbHA9MCZmX3dtYT0wJmZfZGlyPTAmZl9mbGE9MSZmX2phdmE9MCZmX2dlYXJzPTAmZl9hZz0wJnJlcz0yNTYweDE0NDAmY2Q9MjQmY29va2llPTEmdXJsPWZpbGU6Ly9maWxlOi8vL1VzZXJzL2FsZXgvRGV2ZWxvcG1lbnQvZGV2LWVudmlyb25tZW50L2RlbW8vMS10cmFja2VyL2V2ZW50cy5odG1sL292ZXJyaWRkZW4tdXJsLwALAC0AAAAJbG9jYWxob3N0CwAyAAAAUU1vemlsbGEvNS4wIChNYWNpbnRvc2g7IEludGVsIE1hYyBPUyBYIDEwLjk7IHJ2OjI2LjApIEdlY2tvLzIwMTAwMTAxIEZpcmVmb3gvMjYuMA8ARgsAAAAHAAAAFkNvbm5lY3Rpb246IGtlZXAtYWxpdmUAAAJwQ29va2llOiBfX3V0bWE9MTExODcyMjgxLjg3ODA4NDQ4Ny4xMzkwMjM3MTA3LjEzOTA4NDg0ODcuMTM5MDkzMTUyMS42OyBfX3V0bXo9MTExODcyMjgxLjEzOTAyMzcxMDcuMS4xLnV0bWNzcj0oZGlyZWN0KXx1dG1jY249KGRpcmVjdCl8dXRtY21kPShub25lKTsgX3NwX2lkLjFmZmY9Yjg5YTZmYTYzMWVlZmFjMi4xMzkwMjM3MTA3LjYuMTM5MDkzMTU0NS4xMzkwODQ4NjQxOyBoYmxpZD1DUGpqdWh2RjA1emt0UDdKN001Vm8zTklHUExKeTFTRjsgb2xmc2s9b2xmc2s1NjI5MjM2MzU2MTc1NTQ7IF9fdXRtYz0xMTE4NzIyODE7IHdjc2lkPXVNbG9nMVFKVkQ3anVoRlo3TTVWb0JDeVBQeWlCeVNTOyBfb2tsdj0xMzkwOTMxNTg1NDQ1JTJDdU1sb2cxUUpWRDdqdWhGWjdNNVZvQkN5UFB5aUJ5U1M7IF9vaz05NzUyLTUwMy0xMC01MjI3OyBfb2tiaz1jZDQlM0R0cnVlJTJDdmk1JTNEMCUyQ3ZpNCUzRDEzOTA5MzE1MjExMjMlMkN2aTMlM0RhY3RpdmUlMkN2aTIlM0RmYWxzZSUyQ3ZpMSUzRGZhbHNlJTJDY2Q4JTNEY2hhdCUyQ2NkNiUzRDAlMkNjZDUlM0Rhd2F5JTJDY2QzJTNEZmFsc2UlMkNjZDIlM0QwJTJDY2QxJTNEMCUyQzsgc3A9NzVhMTM1ODMtNWM5OS00MGUzLTgxZmMtNTQxMDg0ZGZjNzg0AAAAHkFjY2VwdC1FbmNvZGluZzogZ3ppcCwgZGVmbGF0ZQAAABpBY2NlcHQtTGFuZ3VhZ2U6IGVuLVVTLCBlbgAAACtBY2NlcHQ6IGltYWdlL3BuZywgaW1hZ2UvKjtxPTAuOCwgKi8qO3E9MC41AAAAXVVzZXItQWdlbnQ6IE1vemlsbGEvNS4wIChNYWNpbnRvc2g7IEludGVsIE1hYyBPUyBYIDEwLjk7IHJ2OjI2LjApIEdlY2tvLzIwMTAwMTAxIEZpcmVmb3gvMjYuMAAAABRIb3N0OiBsb2NhbGhvc3Q6NDAwMQsAUAAAACQ3NWExMzU4My01Yzk5LTQwZTMtODFmYy01NDEwODRkZmM3ODQA"
      val result = ThriftLoader.toCollectorPayload(raw.getBytes("UTF-8"), ThriftLoaderSpec.Process, tryBase64Decoding = true)

      val context = CollectorPayload.Context(
        timestamp = DateTime.parse("2014-01-28T20:31:56.846+00:00").some,
        ipAddress = "10.0.2.2".some,
        useragent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:26.0) Gecko/20100101 Firefox/26.0".some,
        refererUri = None,
        headers = List(
          "Connection: keep-alive",
          "Cookie: __utma=111872281.878084487.1390237107.1390848487.1390931521.6; __utmz=111872281.1390237107.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _sp_id.1fff=b89a6fa631eefac2.1390237107.6.1390931545.1390848641; hblid=CPjjuhvF05zktP7J7M5Vo3NIGPLJy1SF; olfsk=olfsk562923635617554; __utmc=111872281; wcsid=uMlog1QJVD7juhFZ7M5VoBCyPPyiBySS; _oklv=1390931585445%2CuMlog1QJVD7juhFZ7M5VoBCyPPyiBySS; _ok=9752-503-10-5227; _okbk=cd4%3Dtrue%2Cvi5%3D0%2Cvi4%3D1390931521123%2Cvi3%3Dactive%2Cvi2%3Dfalse%2Cvi1%3Dfalse%2Ccd8%3Dchat%2Ccd6%3D0%2Ccd5%3Daway%2Ccd3%3Dfalse%2Ccd2%3D0%2Ccd1%3D0%2C; sp=75a13583-5c99-40e3-81fc-541084dfc784",
          "Accept-Encoding: gzip, deflate",
          "Accept-Language: en-US, en",
          "Accept: image/png, image/*;q=0.8, */*;q=0.5",
          "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:26.0) Gecko/20100101 Firefox/26.0",
          "Host: localhost:4001"
        ),
        userId = UUID.fromString("75a13583-5c99-40e3-81fc-541084dfc784").some
      )
      val expected = CollectorPayload(
        api = ThriftLoaderSpec.Api,
        querystring = toNameValuePairs(
          "e" -> "ue",
          "ue_na" -> "Viewed Product",
          "ue_pr" -> """{"product_id":"ASO01043","category":"Dresses","brand":"ACME","returning":true,"price":49.95,"sizes":["xs","s","l","xl","xxl"],"available_since$dt":15801}""",
          "dtm" -> "1390941115263",
          "tid" -> "647615",
          "vp" -> "2560x961",
          "ds" -> "2560x961",
          "vid" -> "8",
          "duid" -> "3c1757544e39bca4",
          "p" -> "mob",
          "tv" -> "js-0.13.1",
          "fp" -> "2695930803",
          "aid" -> "CFe23a",
          "lang" -> "en-US",
          "cs" -> "UTF-8",
          "tz" -> "Europe/London",
          "uid" -> "alex 123",
          "f_pdf" -> "0",
          "f_qt" -> "1",
          "f_realp" -> "0",
          "f_wma" -> "0",
          "f_dir" -> "0",
          "f_fla" -> "1",
          "f_java" -> "0",
          "f_gears" -> "0",
          "f_ag" -> "0",
          "res" -> "2560x1440",
          "cd" -> "24",
          "cookie" -> "1",
          "url" -> "file://file:///Users/alex/Development/dev-environment/demo/1-tracker/events.html/overridden-url/"
        ),
        body = None,
        contentType = None,
        source = CollectorPayload.Source(ThriftLoaderSpec.Collector, ThriftLoaderSpec.Encoding, "localhost".some),
        context = context
      )

      result must beValid(expected.some)
    }

    "fail to parse base64 encoded event when 'tryBase64Decoding' is false" >> {
      val raw =
        "CgABAAABQ9qNGa4LABQAAAAQc3NjLTAuMC4xLVN0ZG91dAsAHgAAAAVVVEYtOAsAKAAAAAgxMC4wLjIuMgwAKQgAAQAAAAEIAAIAAAABCwADAAACeWU9dWUmdWVfbmE9Vmlld2VkK1Byb2R1Y3QmdWVfcHI9JTdCJTIycHJvZHVjdF9pZCUyMjolMjJBU08wMTA0MyUyMiwlMjJjYXRlZ29yeSUyMjolMjJEcmVzc2VzJTIyLCUyMmJyYW5kJTIyOiUyMkFDTUUlMjIsJTIycmV0dXJuaW5nJTIyOnRydWUsJTIycHJpY2UlMjI6NDkuOTUsJTIyc2l6ZXMlMjI6JTVCJTIyeHMlMjIsJTIycyUyMiwlMjJsJTIyLCUyMnhsJTIyLCUyMnh4bCUyMiU1RCwlMjJhdmFpbGFibGVfc2luY2UkZHQlMjI6MTU4MDElN0QmZHRtPTEzOTA5NDExMTUyNjMmdGlkPTY0NzYxNSZ2cD0yNTYweDk2MSZkcz0yNTYweDk2MSZ2aWQ9OCZkdWlkPTNjMTc1NzU0NGUzOWJjYTQmcD1tb2ImdHY9anMtMC4xMy4xJmZwPTI2OTU5MzA4MDMmYWlkPUNGZTIzYSZsYW5nPWVuLVVTJmNzPVVURi04JnR6PUV1cm9wZS9Mb25kb24mdWlkPWFsZXgrMTIzJmZfcGRmPTAmZl9xdD0xJmZfcmVhbHA9MCZmX3dtYT0wJmZfZGlyPTAmZl9mbGE9MSZmX2phdmE9MCZmX2dlYXJzPTAmZl9hZz0wJnJlcz0yNTYweDE0NDAmY2Q9MjQmY29va2llPTEmdXJsPWZpbGU6Ly9maWxlOi8vL1VzZXJzL2FsZXgvRGV2ZWxvcG1lbnQvZGV2LWVudmlyb25tZW50L2RlbW8vMS10cmFja2VyL2V2ZW50cy5odG1sL292ZXJyaWRkZW4tdXJsLwALAC0AAAAJbG9jYWxob3N0CwAyAAAAUU1vemlsbGEvNS4wIChNYWNpbnRvc2g7IEludGVsIE1hYyBPUyBYIDEwLjk7IHJ2OjI2LjApIEdlY2tvLzIwMTAwMTAxIEZpcmVmb3gvMjYuMA8ARgsAAAAHAAAAFkNvbm5lY3Rpb246IGtlZXAtYWxpdmUAAAJwQ29va2llOiBfX3V0bWE9MTExODcyMjgxLjg3ODA4NDQ4Ny4xMzkwMjM3MTA3LjEzOTA4NDg0ODcuMTM5MDkzMTUyMS42OyBfX3V0bXo9MTExODcyMjgxLjEzOTAyMzcxMDcuMS4xLnV0bWNzcj0oZGlyZWN0KXx1dG1jY249KGRpcmVjdCl8dXRtY21kPShub25lKTsgX3NwX2lkLjFmZmY9Yjg5YTZmYTYzMWVlZmFjMi4xMzkwMjM3MTA3LjYuMTM5MDkzMTU0NS4xMzkwODQ4NjQxOyBoYmxpZD1DUGpqdWh2RjA1emt0UDdKN001Vm8zTklHUExKeTFTRjsgb2xmc2s9b2xmc2s1NjI5MjM2MzU2MTc1NTQ7IF9fdXRtYz0xMTE4NzIyODE7IHdjc2lkPXVNbG9nMVFKVkQ3anVoRlo3TTVWb0JDeVBQeWlCeVNTOyBfb2tsdj0xMzkwOTMxNTg1NDQ1JTJDdU1sb2cxUUpWRDdqdWhGWjdNNVZvQkN5UFB5aUJ5U1M7IF9vaz05NzUyLTUwMy0xMC01MjI3OyBfb2tiaz1jZDQlM0R0cnVlJTJDdmk1JTNEMCUyQ3ZpNCUzRDEzOTA5MzE1MjExMjMlMkN2aTMlM0RhY3RpdmUlMkN2aTIlM0RmYWxzZSUyQ3ZpMSUzRGZhbHNlJTJDY2Q4JTNEY2hhdCUyQ2NkNiUzRDAlMkNjZDUlM0Rhd2F5JTJDY2QzJTNEZmFsc2UlMkNjZDIlM0QwJTJDY2QxJTNEMCUyQzsgc3A9NzVhMTM1ODMtNWM5OS00MGUzLTgxZmMtNTQxMDg0ZGZjNzg0AAAAHkFjY2VwdC1FbmNvZGluZzogZ3ppcCwgZGVmbGF0ZQAAABpBY2NlcHQtTGFuZ3VhZ2U6IGVuLVVTLCBlbgAAACtBY2NlcHQ6IGltYWdlL3BuZywgaW1hZ2UvKjtxPTAuOCwgKi8qO3E9MC41AAAAXVVzZXItQWdlbnQ6IE1vemlsbGEvNS4wIChNYWNpbnRvc2g7IEludGVsIE1hYyBPUyBYIDEwLjk7IHJ2OjI2LjApIEdlY2tvLzIwMTAwMTAxIEZpcmVmb3gvMjYuMAAAABRIb3N0OiBsb2NhbGhvc3Q6NDAwMQsAUAAAACQ3NWExMzU4My01Yzk5LTQwZTMtODFmYy01NDEwODRkZmM3ODQA"
      val result = ThriftLoader.toCollectorPayload(raw.getBytes("UTF-8"), ThriftLoaderSpec.Process, tryBase64Decoding = false)

      result must beInvalid.like {
        case NonEmptyList(
              BadRow.CPFormatViolation(
                Process,
                Failure.CPFormatViolation(_, "thrift", _),
                Payload.RawPayload(_)
              ),
              List()
            ) =>
          ok
      }
    }

    "fail to parse random bytes" >> {
      prop { (raw: String) =>
        ThriftLoader.toCollectorPayload(Base64.decodeBase64(raw), Process) must beInvalid.like {
          case NonEmptyList(
                BadRow.CPFormatViolation(
                  Process,
                  Failure.CPFormatViolation(_, "thrift", f),
                  Payload.RawPayload(_)
                ),
                List()
              ) =>
            (f must beEqualTo(violation1byte)) or (f must beEqualTo(violation2bytes))
        }
      }
    }
  }
}

object ThriftLoaderSpec {
  val Encoding = "UTF-8"
  val Collector = "ssc-0.0.1-Stdout" // Note we have since fixed -stdout to be lowercase
  val Api = CollectorPayload.Api("com.snowplowanalytics.snowplow", "tp1")
  val Process = Processor("ThriftLoaderSpec", "v1")
  val DeserializeMessage =
    "error deserializing raw event: Cannot read. Remote side has closed. Tried to read 1 bytes, but only got 0 bytes. (This is often indicative of an internal error on the server side. Please check your server logs.)"

  val violation1byte: FailureDetails.CPFormatViolationMessage =
    FailureDetails.CPFormatViolationMessage.Fallback(
      "error deserializing raw event: Cannot read. Remote side has closed. Tried to read 1 bytes, but only got 0 bytes. (This is often indicative of an internal error on the server side. Please check your server logs.)"
    )
  val violation2bytes: FailureDetails.CPFormatViolationMessage =
    FailureDetails.CPFormatViolationMessage.Fallback(
      "error deserializing raw event: Cannot read. Remote side has closed. Tried to read 2 bytes, but only got 0 bytes. (This is often indicative of an internal error on the server side. Please check your server logs.)"
    )
}
