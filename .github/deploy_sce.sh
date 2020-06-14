#!/bin/sh

tag=$1

mkdir ~/.bintray/
FILE=$HOME/.bintray/.credentials
cat <<EOF >$FILE
realm = Bintray API Realm
host = api.bintray.com
user = $BINTRAY_SNOWPLOW_MAVEN_USER
password = $BINTRAY_SNOWPLOW_MAVEN_API_KEY
EOF

sbt "project common" +publish
echo "Snowplow Common Enrich: published to Bintray Maven"
sbt "project common" +bintraySyncMavenCentral
echo "Snowplow Common Enrich: synced to Maven Central"
