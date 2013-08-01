#!/bin/sh
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script explicitly sets the CLASSPATH for embedded JVMs (e.g. in
# Impalad or in runquery) Because embedded JVMs do not honour
# CLASSPATH wildcard expansion, we have to add every dependency jar
# explicitly to the CLASSPATH.

CLASSPATH=\
$HADOOP_HOME/etc/hadoop:\
$HBASE_HOME/conf:\
$HIVE_HOME/conf:\
$HBASE_HOME/hbase-0.94.6-cdh4.3.0.jar:\
/home/mpp/ht5install/hugetable/lib/hugetable-core-5.0-130609.jar:\
/home/mpp/ht5install/hugetable/lib/guava-14.0.jar:\
$IMPALA_HOME/fe/target/impala-frontend-0.1-SNAPSHOT.jar
$IMPALA_HOME/fe/src/test/resources:\
$IMPALA_HOME/fe/target/classes:\
$IMPALA_HOME/fe/target/dependency:\
$IMPALA_HOME/fe/target/test-classes:\
${HIVE_HOME}/lib/datanucleus-core-2.0.3.jar:\
${HIVE_HOME}/lib/datanucleus-enhancer-2.0.3.jar:\
${HIVE_HOME}/lib/datanucleus-rdbms-2.0.3.jar:\
${HIVE_HOME}/lib/datanucleus-connectionpool-2.0.3.jar:

for jar in `ls ${IMPALA_HOME}/fe/target/dependency/*.jar`; do
  CLASSPATH=${CLASSPATH}:$jar
done

export CLASSPATH
