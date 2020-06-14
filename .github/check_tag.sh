
#!/bin/bash

set -e

tag=$1

project_version=$(sbt version -Dsbt.log.noformat=true | perl -ne 'print "$1\n" if /info.*(\d+\.\d+\.\d+[^\r\n]*)/' | tail -n 1 | tr -d '\n')

if [[ "${tag}" = "${project_version}" ]]; then
  echo "Tag version (${tag}) matches project version (${project_version}). Deploying!"
else
  echo "Tag version (${tag}) doesn't match version in scala project (${project_version}). Aborting!"
  exit 1
fi