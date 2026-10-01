#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# RECOMMENDED submission (per ODP Flink docs — application mode, not the
# deprecated per-job / yarn-cluster path):
#
#     start-seatunnel-flink-19-connector-v2.sh \
#       -e run-application -t yarn-application \
#       --config <job.conf>
#
# Legacy per-job / yarn-cluster (`-m yarn-cluster`) still works but is
# deprecated in Flink 1.15+ and will be removed upstream.

set -eu
# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ] ; do
  # shellcheck disable=SC2006
  ls=`ls -ld "$PRG"`
  # shellcheck disable=SC2006
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    # shellcheck disable=SC2006
    PRG=`dirname "$PRG"`/"$link"
  fi
done

PRG_DIR=`dirname "$PRG"`
APP_DIR=`cd "$PRG_DIR/.." >/dev/null; pwd`
CONF_DIR=${APP_DIR}/config
APP_JAR=${APP_DIR}/starter/seatunnel-flink-19-starter.jar
APP_MAIN="org.apache.seatunnel.core.starter.flink.FlinkStarter"

if [ -f "${CONF_DIR}/seatunnel-env.sh" ]; then
    . "${CONF_DIR}/seatunnel-env.sh"
fi

# --- classpath ordering ----------------------------------------------------
# Hadoop distros (HDP / CDP / ODP / vanilla Bigtop) ship commons-cli-1.2 in
# /usr/…/hadoop/lib and place that directory at the front of
# `yarn.application.classpath`. Flink 1.15+ requires commons-cli 1.5 for
# `Option.builder(String)`; the older jar wins the classloader race and the
# YARN AM crashes with
#     java.lang.NoSuchMethodError:
#       org.apache.commons.cli.Option$Builder Option.builder(java.lang.String)
#
# Prepend Flink's own lib to HADOOP_CLASSPATH so the correct commons-cli
# reaches the AM container. Also works for session / per-job modes.
if [ -n "${FLINK_HOME:-}" ] && [ -d "${FLINK_HOME}/lib" ]; then
  _FLINK_LIB_CP="${FLINK_HOME}/lib/*"
  if [ -n "${HADOOP_CLASSPATH:-}" ]; then
    export HADOOP_CLASSPATH="${_FLINK_LIB_CP}:${HADOOP_CLASSPATH}"
  else
    export HADOOP_CLASSPATH="${_FLINK_LIB_CP}"
  fi
fi

# AbstractFlinkStarter emits `-Dyarn.ship-archives=runtime.tar.gz` as a bare
# relative path; Flink resolves it against submit CWD. Anchor both the build
# and the subsequent `eval` to $APP_DIR so inputs, the archive, and Flink's
# reference all resolve against the installed tree.
cd "${APP_DIR}"

if [ ! -f "${APP_DIR}/runtime.tar.gz" ]; then
  directories=("connectors" "lib" "plugins")
  existing_dirs=()
  for dir in "${directories[@]}"; do
      if [ -d "$dir" ]; then
          existing_dirs+=("$dir")
      fi
  done

  if [ ${#existing_dirs[@]} -eq 0 ]; then
      echo "[connectors,lib,plugins] not existed in ${APP_DIR}, skip generate runtime.tar.gz"
  else
      tar -zcf runtime.tar.gz "${existing_dirs[@]}"
  fi
fi

if [ $# == 0 ]
then
    args="-h"
else
    args=$@
fi

set +u
# Log4j2 Config
if [ -e "${CONF_DIR}/log4j2.properties" ]; then
  JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.configurationFile=${CONF_DIR}/log4j2.properties"
  JAVA_OPTS="${JAVA_OPTS} -Dseatunnel.logs.path=${APP_DIR}/logs"
  JAVA_OPTS="${JAVA_OPTS} -Dseatunnel.logs.file_name=seatunnel-flink-starter"
fi

CLASS_PATH=${APP_DIR}/starter/logging/*:${APP_JAR}

CMD=$(java ${JAVA_OPTS} -cp ${CLASS_PATH} ${APP_MAIN} ${args}) && EXIT_CODE=$? || EXIT_CODE=$?
if [ ${EXIT_CODE} -eq 234 ]; then
    # print usage
    echo "${CMD}"
    exit 0
elif [ ${EXIT_CODE} -eq 0 ]; then
    echo "Execute SeaTunnel Flink Job: $(echo "${CMD}" | tail -n 1)"
    eval $(echo "${CMD}" | tail -n 1)
else
    echo "${CMD}"
    exit ${EXIT_CODE}
fi
