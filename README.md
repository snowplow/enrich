# Stream Enrich

## Introduction

Stream Enrich reads raw [Snowplow][snowplow] events from

-  [Amazon Kinesis][kinesis]
-  [NSQ][nsq]
-  [Kafka][kafka]
-  stdin

enriches them using [Snowplow Common Enrich][common-enrich] library and emits enriched events to an output stream.

## Building

Assuming you already have [SBT][sbt] installed:

    $ git clone git@github.com:snowplow/stream-enrich.git
    $ cd stream-enrich
    $ sbt compile

## Usage

Stream Enrich has the following command line interface:

```
snowplow-stream-enrich 1.1.1
Usage: snowplow-stream-enrich [options]

  --config <filename>
  --resolver <resolver uri>
                           Iglu resolver file, 'file:[filename]' or 'dynamodb:[region/table/key]'
  --enrichments <enrichments directory uri>
                           Directory of enrichment configuration JSONs, 'file:[filename]' or 'dynamodb:[region/table/key]'
  --force-cached-files-download
                           Invalidate the cached IP lookup / IAB database files and download them anew
```

## Copyright and license

Copyright 2013-2020 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[kinesis]: https://aws.amazon.com/kinesis/
[snowplow]: https://snowplowanalytics.com/
[common-enrich]: https://github.com/snowplow/common-enrich
[sbt]: https://www.scala-sbt.org
[nsq]: https://nsq.io/
[kafka]: https://kafka.apache.org/

[license]: http://www.apache.org/licenses/LICENSE-2.0
