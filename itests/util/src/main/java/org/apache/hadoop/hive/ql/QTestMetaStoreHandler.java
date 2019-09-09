/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.dbinstall.rules.DatabaseRule;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Mssql;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Mysql;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Oracle;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Postgres;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QTestMetaStoreHandler {
  private static final Logger LOG = LoggerFactory.getLogger(QTestMetaStoreHandler.class);

  private String metastoreType;
  private DatabaseRule rule;

  public QTestMetaStoreHandler() {
    init();
  }

  private void init() {
    this.metastoreType = QTestSystemProperties.getMetaStoreDbType();
    this.rule = getDatabaseRule(metastoreType);
    
    LOG.info(String.format("initialized metastore type '%s' for qtests", metastoreType));
  }

  public DatabaseRule getRule() {
    return rule;
  }

  public QTestMetaStoreHandler setMetaStoreConfiguration(HiveConf conf) {
    conf.setVar(ConfVars.METASTOREDBTYPE, getDbTypeConfString());

    conf.setVar(ConfVars.METASTORECONNECTURLKEY, rule.getJdbcUrl());
    conf.setVar(ConfVars.METASTORE_CONNECTION_DRIVER, rule.getJdbcDriver());
    conf.setVar(ConfVars.METASTORE_CONNECTION_USER_NAME, rule.getDbRootUser());
    conf.setVar(ConfVars.METASTOREPWD, rule.getDbRootPassword());

    return this;
  }

  public void cleanupMetaStore(HiveConf conf) throws Exception {
    TxnDbUtil.cleanDb(conf);
    TxnDbUtil.prepDb(conf);
  }

  private DatabaseRule getDatabaseRule(String metastoreType) {
    switch (metastoreType.toLowerCase()) {
    case "postgres":
      return new Postgres();
    case "oracle":
      return new Oracle();
    case "mysql":
      return new Mysql();
    case "mssql":
    case "sqlserver":
      return new Mssql();
    default:
      throw new RuntimeException("unknown metastore type: " + metastoreType);
    }
  }

  private String getDbTypeConfString() {// "ORACLE", "MYSQL", "MSSQL", "POSTGRES"
    return "sqlserver".equalsIgnoreCase(metastoreType) ? "MSSQL" : metastoreType.toUpperCase();
  }
}
