[![Release][release-image]][releases]
[![License][license-image]][license]
[![Coverage Status][coveralls-image]][coveralls]
[![CI][ci-image]][ci]

# Snowplow Enrich

Snowplow Enrich is a set of applications and libraries for processing raw Snowplow events into validated and enriched Snowplow events.
It consists of following modules:

* Snowplow Common Enrich - a core library, containing all validation and transformation logic. Published on Maven Central
* Snowplow Stream Enrich - a set of applications working with Kinesis, Kafka and NSQ. Each asset published as Docker image on DockerHub
* Snowplow Enrich PubSub - an application for a GCP pipeline that does not require a distributed computing framework. Published as Docker image on DockerHub

Snowplow Enrich provides record-level enrichment only: feeding in 1 raw Snowplow event will yield exactly 1 record out, where a record may be an enriched Snowplow event or a reported bad record.

## Find out more

| Technical Docs              | Setup Guide           | Roadmap               | Contributing                  |
|:---------------------------:|:---------------------:|:---------------------:|:-----------------------------:|
| ![i1][techdocs-image]       | ![i2][setup-image]    | ![i3][roadmap-image]  | ![i4][contributing-image]     |
| [Technical Docs][techdocs]  | [Setup Guide][setup]  | [Roadmap][roadmap]    | _coming soon_                 |

## Copyright and license

Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.

Licensed under the [Snowplow Limited Use License Agreement][license]. _(If you are uncertain how it applies to your use case, check our answers to [frequently asked questions][faq].)_

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png

[techdocs]: https://docs.snowplowanalytics.com/docs/pipeline-components-and-applications/enrichment-components/
[setup]: https://docs.snowplowanalytics.com/docs/getting-started-on-snowplow-open-source/
[roadmap]: https://github.com/snowplow/enrich/issues
[contributing]: https://docs.snowplowanalytics.com/docs/contributing/

[ci]: https://github.com/snowplow/enrich/actions?query=workflow%3ACI
[ci-image]: https://github.com/snowplow/enrich/workflows/CI/badge.svg

[coveralls]: https://coveralls.io/github/snowplow/enrich?branch=master
[coveralls-image]: https://coveralls.io/repos/github/snowplow/enrich/badge.svg?branch=master

[release-image]: https://img.shields.io/badge/release-4.0.0-blue.svg?style=flat
[releases]: https://github.com/snowplow/enrich/releases

[license]: https://docs.snowplow.io/limited-use-license-1.0
[license-image]: https://img.shields.io/badge/license-Snowplow--Limited-Use-blue.svg?style=flat

[faq]: https://docs.snowplow.io/docs/contributing/limited-use-license-faq/
