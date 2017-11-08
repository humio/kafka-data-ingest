#!/bin/bash

set -e
set -x

DIR=`dirname $0`
cd $DIR

sbt assembly

BUILD_DIR=`mktemp -d`

cp humio.properties $BUILD_DIR
cp kafka-consumer.properties $BUILD_DIR
cp topics.txt $BUILD_DIR

cp target/scala-2.11/humio-ingest-assembly-0.1.jar $BUILD_DIR

tar -vczf  humio-ingest.tgz -C $BUILD_DIR .
