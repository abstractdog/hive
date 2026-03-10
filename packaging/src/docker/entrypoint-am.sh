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

set -euo pipefail
set -x

# Mocking DAGAppMaster#main() env variables #
: "${CONTAINER_ID:="container_1700000000000_0001_01_000001"}"
: "${USER:="tez"}"
: "${HADOOP_USER_NAME:="tez"}"
: "${NM_HOST:="localhost"}"
: "${NM_PORT:="12345"}"
: "${NM_HTTP_PORT:="8042"}"
: "${LOCAL_DIRS:="/tmp"}"
: "${LOG_DIRS:="/opt/tez/logs"}"
: "${APP_SUBMIT_TIME_ENV:=$(($(date +%s) * 1000))}"
: "${TEZ_AM_EXTERNAL_ID:="tez-session-$(hostname)"}"

export CONTAINER_ID USER HADOOP_USER_NAME NM_HOST NM_PORT NM_HTTP_PORT \
    LOCAL_DIRS LOG_DIRS APP_SUBMIT_TIME_ENV TEZ_AM_EXTERNAL_ID

export HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
export TEZ_HOME=${TEZ_HOME:-/opt/tez}

# Allow external configuration directories to be mounted in.
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export TEZ_CONF_DIR="${TEZ_CONF_DIR:-$HADOOP_CONF_DIR}"

# Make Tez jars visible to the Hadoop launcher.
export HADOOP_CLASSPATH="$TEZ_HOME/*:$TEZ_HOME/lib/*:${HADOOP_CLASSPATH:-}"

# Extra JVM options for the Tez AM can be supplied via TEZ_AM_OPTS.
export HADOOP_OPTS="${HADOOP_OPTS:-} ${TEZ_AM_OPTS:-}"

cd "${HADOOP_HOME}"

exec "${HADOOP_HOME}/bin/hadoop" org.apache.tez.dag.app.DAGAppMaster "$@"

