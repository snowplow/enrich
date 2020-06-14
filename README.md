[![License][license-image]][license]
[![Coverage Status][coveralls-image]][coveralls]
[![Test][test-image]][test]

# Snowplow Enrich

Snowplow Enrich is a set of applications and libraries for processing raw Snowplow events into validated and enriched Snowplow events, ready for loading into [Storage][storage].
It consists of following modules:

* Snowplow Common Enrich - a core library, containing all validation and transformation logic. Published on Maven Central
* Snowplow Stream Enrich - a set of applications working with Kinesis, Kafka and NSQ. Each asset published as Docker image on DockerHub
* Snowplow Beam Enrich - a Google Dataflow job. Published as Docker image on DockerHub

Snowplow Enrich provides record-level enrichment only: feeding in 1 raw Snowplow event will yield 0 or 1 records out, where a record may be an enriched Snowplow event or a reported bad record.

## Find out more

| Technical Docs              | Setup Guide           | Roadmap               | Contributing                  |
|-----------------------------|-----------------------|-----------------------|-------------------------------|
| ![i1][techdocs-image]      | ![i2][setup-image]   | ![i3][roadmap-image] | ![i4][contributing-image]    |
| [Technical Docs][techdocs] | [Setup Guide][setup] | [Roadmap][roadmap]   | [Contributing][contributing] |

## Copyright and license

Scala Common Enrich is copyright 2012-2020 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[storage]: https://docs.snowplowanalytics.com/docs/setup-snowplow-on-aws/setup-destinations/

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png

[techdocs]: https://docs.snowplowanalytics.com/open-source-docs/
[setup]: https://docs.snowplowanalytics.com/docs/setup-snowplow-on-aws/setup-validation-and-enrich/
[roadmap]: https://github.com/snowplow/enrich/issues
[contributing]: https://github.com/snowplow/snowplow/wiki/Contributing

[test]: https://github.com/snowplow/enrich/actions?query=workflow%3ATest
[test-image]: https://github.com/snowplow/enrich/workflows/Test/badge.svg

[license]: http://www.apache.org/licenses/LICENSE-2.0
[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat

[coveralls]: https://coveralls.io/github/snowplow/enrich?branch=master
[coveralls-image]: https://coveralls.io/repos/github/snowplow/enrich/badge.svg?branch=master
