#
# Uncharted Spark Pipeline Test Container
# Apache Spark 1.5.1
#
# Runs the Spark Pipeline test suite in a container
#
# One-time usage (such as on travis):
# $ docker build -t uncharted/spark-pipeline-test .
# $ docker run --rm uncharted/spark-pipeline-test
#
# Dev environment usage:
# $ docker run -v $(pwd):/opt/pipeline -it --entrypoint=/bin/bash uncharted/spark-pipeline-test
# container$ ./gradlew
#
# If you need to install the jars to your local m2 repository, be sure to clean
# the build directory from inside the docker container, since the container
# happens to assign root permissions to all the files in the /build directory

FROM uncharted/sparklet:1.5.1
MAINTAINER Sean McIntyre <smcintyre@uncharted.software>

ADD . /opt/pipeline

WORKDIR /opt/pipeline
RUN mkdir /opt/libs

# silence log4j garbage from spark
ADD src/test/resources/log4j.properties /usr/local/spark/conf/log4j.properties

# for dev environment
ENV GRADLE_OPTS -Dorg.gradle.daemon=true

CMD ["./gradlew", "coverage"]
