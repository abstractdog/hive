/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.dbinstall.rules;

import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;

/**
 * JUnit TestRule for Derby.
 */
public class Derby extends DatabaseRule {

  @Override
  public String getHivePassword() {
    return HIVE_PASSWORD;
  }

  @Override
  public String getDockerImageName() {
    return null;
  }

  @Override
  public String[] getDockerAdditionalArgs() {
    return null;
  }

  @Override
  public String getDbType() {
    return "derby";
  }

  @Override
  public String getDbRootUser() {
    return "APP";
  }

  @Override
  public String getDbRootPassword() {
    return "mine";
  }

  @Override
  public String getJdbcDriver() {
    return "org.apache.derby.jdbc.EmbeddedDriver";
  }

  @Override
  public String getJdbcUrl() {
    return "jdbc:derby:memory:${test.tmp.dir}/" + getDb() + ";create=true";
  }

  @Override
  public String getInitialJdbcUrl() {
    return "jdbc:derby:memory:${test.tmp.dir}/" + getDb() + ";create=true";
  }

  public String getDb() {
    return MetaStoreServerUtils.JUNIT_DATABASE_PREFIX;
  };

  @Override
  public boolean isContainerReady(String logOutput) {
    return true;
  }

  @Override
  public void before() throws Exception {
    // no-op, no need for docker container for derby
  }

  @Override
  public void after() {
    // no-op, no need for docker container for derby
  }
}
