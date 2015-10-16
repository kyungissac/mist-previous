#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# EXAMPLE USAGE 
# ./run.sh org.apache.reef.examples.hello.HelloREEF

# RUNTIME
SELF_JAR=./target/wordcounter-1.0-shaded.jar

LOGGING_CONFIG='-Djava.util.logging.config.class=org.apache.reef.util.logging.Config'

CLASSPATH=$YARN_HOME/share/hadoop/common/*:$YARN_HOME/share/hadoop/common/lib/*:$YARN_HOME/share/hadoop/yarn/*:$YARN_HOME/share/hadoop/hdfs/*:$YARN_HOME/share/hadoop/mapreduce/lib/*:$YARN_HOME/share/hadoop/mapreduce/*

YARN_CONF_DIR=$YARN_HOME/etc/hadoop

CMD="java -cp $YARN_CONF_DIR:$SELF_JAR:$CLASSPATH $LOCAL_RUNTIME_TMP $LOGGING_CONFIG edu.snu.cms.reef.mist.wordcounter.WordCounterClient"

mvn clean package
rm -rf ./REEF_LOCAL_RUNTIME
echo $CMD
$CMD # 2> /dev/null