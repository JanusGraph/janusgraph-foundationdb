#!/bin/bash
#
# Copyright 2018 Expero Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

if [ -z "$*" ]; then
    echo "Please provide the path to your JanusGraph install directory"
    exit 0
fi

JANUS_INSTALL_PATH=$1
JANUS_CONF_PATH=${JANUS_INSTALL_PATH}/conf
JANUS_GREMLIN_SERVER_CONF=${JANUS_CONF_PATH}/gremlin-server

if [ ! -d "$JANUS_INSTALL_PATH" ]; then
  echo "Directory ${JANUS_INSTALL_PATH} does not exist"
  exit 0
fi

cp lib/* ${JANUS_INSTALL_PATH}/ext
cp conf/janusgraph-foundationdb.properties ${JANUS_CONF_PATH}
cp conf/janusgraph-foundationdb-es-server.properties ${JANUS_GREMLIN_SERVER_CONF}

# backup Gremlin server config
mv ${JANUS_GREMLIN_SERVER_CONF}/gremlin-server.yaml ${JANUS_GREMLIN_SERVER_CONF}/gremlin-server.yaml.orig
cp conf/gremlin-server.yaml ${JANUS_GREMLIN_SERVER_CONF}/gremlin-server.yaml
