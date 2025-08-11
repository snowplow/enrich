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
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers

class ThriftLoaderSpec extends Specification with ValidatedMatchers with ScalaCheck {
  "toCollectorPayload" should {
    "tolerate fake tracker protocol GET parameters" >> {
      val raw =
        "CwBkAAAACTEyNy4wLjAuMQoAyAAAAUOYhqgGCwDSAAAABVVURi04CwDcAAAAEHNzYy0wLjAuMS1TdGRvdXQLASwAAABoTW96aWxsYS81LjAgKFgxMTsgTGludXggeDg2XzY0KSBBcHBsZVdlYktpdC81MzcuMzYgKEtIVE1MLCBsaWtlIEdlY2tvKSBDaHJvbWUvMzEuMC4xNjUwLjYzIFNhZmFyaS81MzcuMzYLAUAAAAACL2kLAUoAAAAYdGVzdFBhcmFtPTMmdGVzdFBhcmFtMj00DwFeCwAAAAgAAAAvQ29va2llOiBzcD1jNWYzYTA5Zi03NWY4LTQzMDktYmVjNS1mZWE1NjBmNzg0NTUAAAAaQWNjZXB0LUxhbmd1YWdlOiBlbi1VUywgZW4AAAAkQWNjZXB0LUVuY29kaW5nOiBnemlwLCBkZWZsYXRlLCBzZGNoAAAAdFVzZXItQWdlbnQ6IE1vemlsbGEvNS4wIChYMTE7IExpbnV4IHg4Nl82NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzMxLjAuMTY1MC42MyBTYWZhcmkvNTM3LjM2AAAAVkFjY2VwdDogdGV4dC9odG1sLCBhcHBsaWNhdGlvbi94aHRtbCt4bWwsIGFwcGxpY2F0aW9uL3htbDtxPTAuOSwgaW1hZ2Uvd2VicCwgKi8qO3E9MC44AAAAGENhY2hlLUNvbnRyb2w6IG1heC1hZ2U9MAAAABZDb25uZWN0aW9uOiBrZWVwLWFsaXZlAAAAFEhvc3Q6IDEyNy4wLjAuMTo4MDgwCwGQAAAACTEyNy4wLjAuMQsBmgAAACRjNWYzYTA5Zi03NWY4LTQzMDktYmVjNS1mZWE1NjBmNzg0NTULemkAAABBaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvQ29sbGVjdG9yUGF5bG9hZC90aHJpZnQvMS0wLTAA"
      val result = ThriftLoader.toCollectorPayload(Base64.decodeBase64(raw), ThriftLoaderSpec.Process, SpecHelpers.etlTstamp)

      val context = CollectorPayload.Context(
        timestamp = DateTime.parse("2014-01-16T00:49:58.278+00:00"),
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

      result must beValid(expected)
    }

    "parse valid page ping GET payload" >> {
      val raw =
        "CwBkAAAACDEwLjAuMi4yCgDIAAABQ9pNXggLANIAAAAFVVRGLTgLANwAAAAQc3NjLTAuMC4xLVN0ZG91dAsBLAAAAFFNb3ppbGxhLzUuMCAoTWFjaW50b3NoOyBJbnRlbCBNYWMgT1MgWCAxMC45OyBydjoyNi4wKSBHZWNrby8yMDEwMDEwMSBGaXJlZm94LzI2LjALAUAAAAACL2kLAUoAAAKYZT1wcCZwYWdlPUFzeW5jaHJvbm91cyt3ZWJzaXRlJTJGd2ViYXBwK2V4YW1wbGVzK2Zvcitzbm93cGxvdy5qcyZwcF9taXg9MCZwcF9tYXg9MCZwcF9taXk9MCZwcF9tYXk9MCZjbz0lN0IlMjJwYWdlJTIyJTNBJTdCJTIycGFnZV90eXBlJTIyJTNBJTIydGVzdCUyMiUyQyUyMmxhc3RfdXBkYXRlZCUyNHRtcyUyMiUzQTEzOTMzNzI4MDAwMDAlN0QlMkMlMjJ1c2VyJTIyJTNBJTdCJTIydXNlcl90eXBlJTIyJTNBJTIydGVzdGVyJTIyJTdEJTdEJmR0bT0xMzkwOTM2OTM4ODU1JnRpZD03OTc3NDMmdnA9MjU2MHg5NjEmZHM9MjU2MHg5NjEmdmlkPTcmZHVpZD0zYzE3NTc1NDRlMzliY2E0JnA9bW9iJnR2PWpzLTAuMTMuMSZmcD0yNjk1OTMwODAzJmFpZD1DRmUyM2EmbGFuZz1lbi1VUyZjcz1VVEYtOCZ0ej1FdXJvcGUlMkZMb25kb24mdWlkPWFsZXgrMTIzJmZfcGRmPTAmZl9xdD0xJmZfcmVhbHA9MCZmX3dtYT0wJmZfZGlyPTAmZl9mbGE9MSZmX2phdmE9MCZmX2dlYXJzPTAmZl9hZz0wJnJlcz0yNTYweDE0NDAmY2Q9MjQmY29va2llPTEmdXJsPWZpbGUlM0ElMkYlMkZmaWxlJTNBJTJGJTJGJTJGVXNlcnMlMkZhbGV4JTJGRGV2ZWxvcG1lbnQlMkZkZXYtZW52aXJvbm1lbnQlMkZkZW1vJTJGMS10cmFja2VyJTJGZXZlbnRzLmh0bWwlMkZvdmVycmlkZGVuLXVybCUyRg8BXgsAAAAHAAAAFkNvbm5lY3Rpb246IGtlZXAtYWxpdmUAAAJwQ29va2llOiBfX3V0bWE9MTExODcyMjgxLjg3ODA4NDQ4Ny4xMzkwMjM3MTA3LjEzOTA4NDg0ODcuMTM5MDkzMTUyMS42OyBfX3V0bXo9MTExODcyMjgxLjEzOTAyMzcxMDcuMS4xLnV0bWNzcj0oZGlyZWN0KXx1dG1jY249KGRpcmVjdCl8dXRtY21kPShub25lKTsgX3NwX2lkLjFmZmY9Yjg5YTZmYTYzMWVlZmFjMi4xMzkwMjM3MTA3LjYuMTM5MDkzMTU0NS4xMzkwODQ4NjQxOyBoYmxpZD1DUGpqdWh2RjA1emt0UDdKN001Vm8zTklHUExKeTFTRjsgb2xmc2s9b2xmc2s1NjI5MjM2MzU2MTc1NTQ7IF9fdXRtYz0xMTE4NzIyODE7IHdjc2lkPXVNbG9nMVFKVkQ3anVoRlo3TTVWb0JDeVBQeWlCeVNTOyBfb2tsdj0xMzkwOTMxNTg1NDQ1JTJDdU1sb2cxUUpWRDdqdWhGWjdNNVZvQkN5UFB5aUJ5U1M7IF9vaz05NzUyLTUwMy0xMC01MjI3OyBfb2tiaz1jZDQlM0R0cnVlJTJDdmk1JTNEMCUyQ3ZpNCUzRDEzOTA5MzE1MjExMjMlMkN2aTMlM0RhY3RpdmUlMkN2aTIlM0RmYWxzZSUyQ3ZpMSUzRGZhbHNlJTJDY2Q4JTNEY2hhdCUyQ2NkNiUzRDAlMkNjZDUlM0Rhd2F5JTJDY2QzJTNEZmFsc2UlMkNjZDIlM0QwJTJDY2QxJTNEMCUyQzsgc3A9NzVhMTM1ODMtNWM5OS00MGUzLTgxZmMtNTQxMDg0ZGZjNzg0AAAAHkFjY2VwdC1FbmNvZGluZzogZ3ppcCwgZGVmbGF0ZQAAABpBY2NlcHQtTGFuZ3VhZ2U6IGVuLVVTLCBlbgAAACtBY2NlcHQ6IGltYWdlL3BuZywgaW1hZ2UvKjtxPTAuOCwgKi8qO3E9MC41AAAAXVVzZXItQWdlbnQ6IE1vemlsbGEvNS4wIChNYWNpbnRvc2g7IEludGVsIE1hYyBPUyBYIDEwLjk7IHJ2OjI2LjApIEdlY2tvLzIwMTAwMTAxIEZpcmVmb3gvMjYuMAAAABRIb3N0OiBsb2NhbGhvc3Q6NDAwMQsBkAAAAAlsb2NhbGhvc3QLAZoAAAAkNzVhMTM1ODMtNWM5OS00MGUzLTgxZmMtNTQxMDg0ZGZjNzg0C3ppAAAAQWlnbHU6Y29tLnNub3dwbG93YW5hbHl0aWNzLnNub3dwbG93L0NvbGxlY3RvclBheWxvYWQvdGhyaWZ0LzEtMC0wAA=="
      val result = ThriftLoader.toCollectorPayload(Base64.decodeBase64(raw), ThriftLoaderSpec.Process, SpecHelpers.etlTstamp)

      val context = CollectorPayload.Context(
        timestamp = DateTime.parse("2014-01-28T19:22:20.040+00:00"),
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

      result must beValid(expected)
    }

    "parse valid unstructured event GET payload" >> {
      val raw =
        "CwBkAAAACDEwLjAuMi4yCgDIAAABQ9qNGa4LANIAAAAFVVRGLTgLANwAAAAQc3NjLTAuMC4xLVN0ZG91dAsBLAAAAFFNb3ppbGxhLzUuMCAoTWFjaW50b3NoOyBJbnRlbCBNYWMgT1MgWCAxMC45OyBydjoyNi4wKSBHZWNrby8yMDEwMDEwMSBGaXJlZm94LzI2LjALAUAAAAACL2kLAUoAAAK9ZT11ZSZ1ZV9uYT1WaWV3ZWQrUHJvZHVjdCZ1ZV9wcj0lN0IlMjJwcm9kdWN0X2lkJTIyJTNBJTIyQVNPMDEwNDMlMjIlMkMlMjJjYXRlZ29yeSUyMiUzQSUyMkRyZXNzZXMlMjIlMkMlMjJicmFuZCUyMiUzQSUyMkFDTUUlMjIlMkMlMjJyZXR1cm5pbmclMjIlM0F0cnVlJTJDJTIycHJpY2UlMjIlM0E0OS45NSUyQyUyMnNpemVzJTIyJTNBJTVCJTIyeHMlMjIlMkMlMjJzJTIyJTJDJTIybCUyMiUyQyUyMnhsJTIyJTJDJTIyeHhsJTIyJTVEJTJDJTIyYXZhaWxhYmxlX3NpbmNlJTI0ZHQlMjIlM0ExNTgwMSU3RCZkdG09MTM5MDk0MTExNTI2MyZ0aWQ9NjQ3NjE1JnZwPTI1NjB4OTYxJmRzPTI1NjB4OTYxJnZpZD04JmR1aWQ9M2MxNzU3NTQ0ZTM5YmNhNCZwPW1vYiZ0dj1qcy0wLjEzLjEmZnA9MjY5NTkzMDgwMyZhaWQ9Q0ZlMjNhJmxhbmc9ZW4tVVMmY3M9VVRGLTgmdHo9RXVyb3BlJTJGTG9uZG9uJnVpZD1hbGV4KzEyMyZmX3BkZj0wJmZfcXQ9MSZmX3JlYWxwPTAmZl93bWE9MCZmX2Rpcj0wJmZfZmxhPTEmZl9qYXZhPTAmZl9nZWFycz0wJmZfYWc9MCZyZXM9MjU2MHgxNDQwJmNkPTI0JmNvb2tpZT0xJnVybD1maWxlJTNBJTJGJTJGZmlsZSUzQSUyRiUyRiUyRlVzZXJzJTJGYWxleCUyRkRldmVsb3BtZW50JTJGZGV2LWVudmlyb25tZW50JTJGZGVtbyUyRjEtdHJhY2tlciUyRmV2ZW50cy5odG1sJTJGb3ZlcnJpZGRlbi11cmwlMkYPAV4LAAAABwAAABZDb25uZWN0aW9uOiBrZWVwLWFsaXZlAAACcENvb2tpZTogX191dG1hPTExMTg3MjI4MS44NzgwODQ0ODcuMTM5MDIzNzEwNy4xMzkwODQ4NDg3LjEzOTA5MzE1MjEuNjsgX191dG16PTExMTg3MjI4MS4xMzkwMjM3MTA3LjEuMS51dG1jc3I9KGRpcmVjdCl8dXRtY2NuPShkaXJlY3QpfHV0bWNtZD0obm9uZSk7IF9zcF9pZC4xZmZmPWI4OWE2ZmE2MzFlZWZhYzIuMTM5MDIzNzEwNy42LjEzOTA5MzE1NDUuMTM5MDg0ODY0MTsgaGJsaWQ9Q1BqanVodkYwNXprdFA3SjdNNVZvM05JR1BMSnkxU0Y7IG9sZnNrPW9sZnNrNTYyOTIzNjM1NjE3NTU0OyBfX3V0bWM9MTExODcyMjgxOyB3Y3NpZD11TWxvZzFRSlZEN2p1aEZaN001Vm9CQ3lQUHlpQnlTUzsgX29rbHY9MTM5MDkzMTU4NTQ0NSUyQ3VNbG9nMVFKVkQ3anVoRlo3TTVWb0JDeVBQeWlCeVNTOyBfb2s9OTc1Mi01MDMtMTAtNTIyNzsgX29rYms9Y2Q0JTNEdHJ1ZSUyQ3ZpNSUzRDAlMkN2aTQlM0QxMzkwOTMxNTIxMTIzJTJDdmkzJTNEYWN0aXZlJTJDdmkyJTNEZmFsc2UlMkN2aTElM0RmYWxzZSUyQ2NkOCUzRGNoYXQlMkNjZDYlM0QwJTJDY2Q1JTNEYXdheSUyQ2NkMyUzRGZhbHNlJTJDY2QyJTNEMCUyQ2NkMSUzRDAlMkM7IHNwPTc1YTEzNTgzLTVjOTktNDBlMy04MWZjLTU0MTA4NGRmYzc4NAAAAB5BY2NlcHQtRW5jb2Rpbmc6IGd6aXAsIGRlZmxhdGUAAAAaQWNjZXB0LUxhbmd1YWdlOiBlbi1VUywgZW4AAAArQWNjZXB0OiBpbWFnZS9wbmcsIGltYWdlLyo7cT0wLjgsICovKjtxPTAuNQAAAF1Vc2VyLUFnZW50OiBNb3ppbGxhLzUuMCAoTWFjaW50b3NoOyBJbnRlbCBNYWMgT1MgWCAxMC45OyBydjoyNi4wKSBHZWNrby8yMDEwMDEwMSBGaXJlZm94LzI2LjAAAAAUSG9zdDogbG9jYWxob3N0OjQwMDELAZAAAAAJbG9jYWxob3N0CwGaAAAAJDc1YTEzNTgzLTVjOTktNDBlMy04MWZjLTU0MTA4NGRmYzc4NAt6aQAAAEFpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9Db2xsZWN0b3JQYXlsb2FkL3RocmlmdC8xLTAtMAA="
      val result = ThriftLoader.toCollectorPayload(Base64.decodeBase64(raw), ThriftLoaderSpec.Process, SpecHelpers.etlTstamp)

      val context = CollectorPayload.Context(
        timestamp = DateTime.parse("2014-01-28T20:31:56.846+00:00"),
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

      result must beValid(expected)
    }

    "parse valid parameterless payload" >> {
      val raw =
        "CwBkAAAACDEwLjAuMi4yCgDIAAABQ9o8zYULANIAAAAFVVRGLTgLANwAAAAQc3NjLTAuMC4xLVN0ZG91dAsBLAAAAFFNb3ppbGxhLzUuMCAoTWFjaW50b3NoOyBJbnRlbCBNYWMgT1MgWCAxMC45OyBydjoyNi4wKSBHZWNrby8yMDEwMDEwMSBGaXJlZm94LzI2LjALAUAAAAACL2kPAV4LAAAACAAAABhDYWNoZS1Db250cm9sOiBtYXgtYWdlPTAAAAAWQ29ubmVjdGlvbjoga2VlcC1hbGl2ZQAAAnBDb29raWU6IF9fdXRtYT0xMTE4NzIyODEuODc4MDg0NDg3LjEzOTAyMzcxMDcuMTM5MDg0ODQ4Ny4xMzkwOTMxNTIxLjY7IF9fdXRtej0xMTE4NzIyODEuMTM5MDIzNzEwNy4xLjEudXRtY3NyPShkaXJlY3QpfHV0bWNjbj0oZGlyZWN0KXx1dG1jbWQ9KG5vbmUpOyBfc3BfaWQuMWZmZj1iODlhNmZhNjMxZWVmYWMyLjEzOTAyMzcxMDcuNi4xMzkwOTMxNTQ1LjEzOTA4NDg2NDE7IGhibGlkPUNQamp1aHZGMDV6a3RQN0o3TTVWbzNOSUdQTEp5MVNGOyBvbGZzaz1vbGZzazU2MjkyMzYzNTYxNzU1NDsgX191dG1jPTExMTg3MjI4MTsgd2NzaWQ9dU1sb2cxUUpWRDdqdWhGWjdNNVZvQkN5UFB5aUJ5U1M7IF9va2x2PTEzOTA5MzE1ODU0NDUlMkN1TWxvZzFRSlZEN2p1aEZaN001Vm9CQ3lQUHlpQnlTUzsgX29rPTk3NTItNTAzLTEwLTUyMjc7IF9va2JrPWNkNCUzRHRydWUlMkN2aTUlM0QwJTJDdmk0JTNEMTM5MDkzMTUyMTEyMyUyQ3ZpMyUzRGFjdGl2ZSUyQ3ZpMiUzRGZhbHNlJTJDdmkxJTNEZmFsc2UlMkNjZDglM0RjaGF0JTJDY2Q2JTNEMCUyQ2NkNSUzRGF3YXklMkNjZDMlM0RmYWxzZSUyQ2NkMiUzRDAlMkNjZDElM0QwJTJDOyBzcD03NWExMzU4My01Yzk5LTQwZTMtODFmYy01NDEwODRkZmM3ODQAAAAeQWNjZXB0LUVuY29kaW5nOiBnemlwLCBkZWZsYXRlAAAAGkFjY2VwdC1MYW5ndWFnZTogZW4tVVMsIGVuAAAASkFjY2VwdDogdGV4dC9odG1sLCBhcHBsaWNhdGlvbi94aHRtbCt4bWwsIGFwcGxpY2F0aW9uL3htbDtxPTAuOSwgKi8qO3E9MC44AAAAXVVzZXItQWdlbnQ6IE1vemlsbGEvNS4wIChNYWNpbnRvc2g7IEludGVsIE1hYyBPUyBYIDEwLjk7IHJ2OjI2LjApIEdlY2tvLzIwMTAwMTAxIEZpcmVmb3gvMjYuMAAAABRIb3N0OiBsb2NhbGhvc3Q6NDAwMQsBkAAAAAlsb2NhbGhvc3QLAZoAAAAkNzVhMTM1ODMtNWM5OS00MGUzLTgxZmMtNTQxMDg0ZGZjNzg0C3ppAAAAQWlnbHU6Y29tLnNub3dwbG93YW5hbHl0aWNzLnNub3dwbG93L0NvbGxlY3RvclBheWxvYWQvdGhyaWZ0LzEtMC0wAA=="
      val result = ThriftLoader.toCollectorPayload(Base64.decodeBase64(raw), ThriftLoaderSpec.Process, SpecHelpers.etlTstamp)

      val context = CollectorPayload.Context(
        timestamp = DateTime.parse("2014-01-28T19:04:14.469+00:00"),
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

      result must beValid(expected)
    }

    "fail to parse random bytes" >> {
      prop { (raw: String) =>
        ThriftLoader.toCollectorPayload(Base64.decodeBase64(raw), Process, SpecHelpers.etlTstamp) must beInvalid.like {
          case NonEmptyList(
                BadRow.CPFormatViolation(
                  Process,
                  Failure.CPFormatViolation(_, "thrift", f),
                  Payload.RawPayload(_)
                ),
                List()
              ) =>
            f must beLike {
              case Violation1Byte => ok
              case Violation2Bytes => ok
              case ViolationEndOfBytes => ok
            }
        }
      }
    }

    "fail to parse a payload if the schema key does not match our expected schema key" >> {
      val raw =
        "CwBkAAAACTEyNy4wLjAuMQoAyAAAAUOYhqgGCwDSAAAABVVURi04CwDcAAAAEHNzYy0wLjAuMS1TdGRvdXQLASwAAABoTW96aWxsYS81LjAgKFgxMTsgTGludXggeDg2XzY0KSBBcHBsZVdlYktpdC81MzcuMzYgKEtIVE1MLCBsaWtlIEdlY2tvKSBDaHJvbWUvMzEuMC4xNjUwLjYzIFNhZmFyaS81MzcuMzYLAUAAAAACL2kPAV4LAAAACAAAAC9Db29raWU6IHNwPWM1ZjNhMDlmLTc1ZjgtNDMwOS1iZWM1LWZlYTU2MGY3ODQ1NQAAABpBY2NlcHQtTGFuZ3VhZ2U6IGVuLVVTLCBlbgAAACRBY2NlcHQtRW5jb2Rpbmc6IGd6aXAsIGRlZmxhdGUsIHNkY2gAAAB0VXNlci1BZ2VudDogTW96aWxsYS81LjAgKFgxMTsgTGludXggeDg2XzY0KSBBcHBsZVdlYktpdC81MzcuMzYgKEtIVE1MLCBsaWtlIEdlY2tvKSBDaHJvbWUvMzEuMC4xNjUwLjYzIFNhZmFyaS81MzcuMzYAAABWQWNjZXB0OiB0ZXh0L2h0bWwsIGFwcGxpY2F0aW9uL3hodG1sK3htbCwgYXBwbGljYXRpb24veG1sO3E9MC45LCBpbWFnZS93ZWJwLCAqLyo7cT0wLjgAAAAYQ2FjaGUtQ29udHJvbDogbWF4LWFnZT0wAAAAFkNvbm5lY3Rpb246IGtlZXAtYWxpdmUAAAAUSG9zdDogMTI3LjAuMC4xOjgwODALAZAAAAAJbG9jYWxob3N0CwGaAAAAJGM1ZjNhMDlmLTc1ZjgtNDMwOS1iZWM1LWZlYTU2MGY3ODQ1NQt6aQAAAD1pZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy93cm9uZy1zY2hlbWEvdGhyaWZ0LzEtMC0wAA=="
      val result = ThriftLoader.toCollectorPayload(Base64.decodeBase64(raw), ThriftLoaderSpec.Process, SpecHelpers.etlTstamp)

      result must beInvalid.like {
        case NonEmptyList(
              BadRow.CPFormatViolation(
                Process,
                Failure.CPFormatViolation(_, "thrift", FailureDetails.CPFormatViolationMessage.Fallback(msg)),
                Payload.RawPayload(_)
              ),
              List()
            ) =>
          msg must beEqualTo(
            "verifying record as iglu:com.snowplowanalytics.snowplow/CollectorPayload/thrift/1-0-* failed: found iglu:com.snowplowanalytics.snowplow/wrong-schema/thrift/1-0-0"
          )
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

  val Violation1Byte: FailureDetails.CPFormatViolationMessage =
    FailureDetails.CPFormatViolationMessage.Fallback(
      "error deserializing raw event: Cannot read. Remote side has closed. Tried to read 1 bytes, but only got 0 bytes. (This is often indicative of an internal error on the server side. Please check your server logs.)"
    )
  val Violation2Bytes: FailureDetails.CPFormatViolationMessage =
    FailureDetails.CPFormatViolationMessage.Fallback(
      "error deserializing raw event: Cannot read. Remote side has closed. Tried to read 2 bytes, but only got 0 bytes. (This is often indicative of an internal error on the server side. Please check your server logs.)"
    )
  val ViolationEndOfBytes: FailureDetails.CPFormatViolationMessage =
    FailureDetails.CPFormatViolationMessage.Fallback(
      "error deserializing raw event: Reached end of bytes when parsing as thrift format"
    )
}
