#!/bin/sh

docker run \
-e GRADLE_OPTS="-Dorg.gradle.daemon=true" \
-v $(pwd)/src/test/resources/log4j.properties:/usr/local/spark/conf/log4j.properties \
-v $(pwd):/opt/pipeline \
-it \
--entrypoint="/startup.sh" \
--workdir="/opt/pipeline" \
uncharted/sparklet:1.5.1 bash
