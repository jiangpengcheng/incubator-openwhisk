#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

# Build script for Travis-CI.
SECONDS=0
SCRIPTDIR=$(cd $(dirname "$0") && pwd)
ROOTDIR="$SCRIPTDIR/../.."

cd $ROOTDIR/ansible

$ANSIBLE_CMD setup.yml -e mode=HA
$ANSIBLE_CMD prereq.yml
$ANSIBLE_CMD couchdb.yml
$ANSIBLE_CMD initdb.yml
$ANSIBLE_CMD wipe.yml

# deploy mongodb for unit tests
$ANSIBLE_CMD mongodb.yml -e db_port=27017 # use a different port other than 5984
$ANSIBLE_CMD initMongoDB.yml -e db_port=27017
$ANSIBLE_CMD wipeMongoDB.yml -e db_port=27017

$ANSIBLE_CMD properties.yml -e enable_mongodb=true -e mongodb_port=27017
echo "Time taken for ${0##*/} is $SECONDS secs"
