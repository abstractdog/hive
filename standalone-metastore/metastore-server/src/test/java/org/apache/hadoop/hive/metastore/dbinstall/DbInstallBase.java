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
package org.apache.hadoop.hive.metastore.dbinstall;

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.dbinstall.rules.DatabaseRule;
import org.apache.hadoop.hive.metastore.tools.schematool.MetastoreSchemaTool;
import org.junit.Assert;
import org.junit.Test;


public abstract class DbInstallBase {
  protected static final String HIVE_USER = "hiveuser";
  private static final String FIRST_VERSION = "1.2.0";

  @Test
  public void install() {
    Assert.assertEquals(0, createUser());
    Assert.assertEquals(0, installLatest());
  }

  @Test
  public void upgrade() throws HiveMetaException {
    Assert.assertEquals(0, createUser());
    Assert.assertEquals(0, installAVersion(FIRST_VERSION));
    Assert.assertEquals(0, upgradeToLatest());
  }
  

  protected abstract DatabaseRule getRule();

  private int createUser() {
    return new MetastoreSchemaTool().run(buildArray(
        "-createUser",
        "-dbType",
        getRule().getDbType(),
        "-userName",
        getRule().getDbRootUser(),
        "-passWord",
        getRule().getDbRootPassword(),
        "-hiveUser",
        HIVE_USER,
        "-hivePassword",
        getRule().getHivePassword(),
        "-hiveDb",
        getRule().getDb(),
        "-url",
        getRule().getInitialJdbcUrl(),
        "-driver",
        getRule().getJdbcDriver()
    ));
  }

  private int installLatest() {
    return new MetastoreSchemaTool().run(buildArray(
        "-initSchema",
        "-dbType",
        getRule().getDbType(),
        "-userName",
        HIVE_USER,
        "-passWord",
        getRule().getHivePassword(),
        "-url",
        getRule().getJdbcUrl(),
        "-driver",
        getRule().getJdbcDriver()
    ));
  }

  private int installAVersion(String version) {
    return new MetastoreSchemaTool().run(buildArray(
        "-initSchemaTo",
        version,
        "-dbType",
        getRule().getDbType(),
        "-userName",
        HIVE_USER,
        "-passWord",
        getRule().getHivePassword(),
        "-url",
        getRule().getJdbcUrl(),
        "-driver",
        getRule().getJdbcDriver()
    ));
  }

  private int upgradeToLatest() {
    return new MetastoreSchemaTool().run(buildArray(
        "-upgradeSchema",
        "-dbType",
        getRule().getDbType(),
        "-userName",
        HIVE_USER,
        "-passWord",
        getRule().getHivePassword(),
        "-url",
        getRule().getJdbcUrl(),
        "-driver",
        getRule().getJdbcDriver()
    ));
  }

  protected String[] buildArray(String... strs) {
    return strs;
  }
}
