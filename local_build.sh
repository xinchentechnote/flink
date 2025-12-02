#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# https://nightlies.apache.org/flink/flink-docs-master/zh/docs/flinkdev/building/
export JAVA_HOME=/usr/java/jdk-11.0.9
export PATH=$JAVA_HOME/bin:$PATH
export MAVEN_HOME=$HOME/software/apache-maven-3.8.6
export PATH=$MAVEN_HOME/bin:$PATH
export MAVEN_OPTS="-Xmx8g -XX:+TieredCompilation -XX:TieredStopAtLevel=1"
mvn --version
mvn clean
mvn generate-sources -pl flink-table/flink-sql-parser -DskipTests
mvn generate-sources -pl flink-table/flink-table-planner -DskipTests
mvn generate-sources -pl flink-table/flink-sql-gateway -DskipTests
mvn install -DskipTests -pl flink-formats -DskipTests
mvn install -DskipTests -pl flink-table/flink-sql-gateway -DskipTests
mvn install -DskipTests -Dfast -Pskip-webui-build