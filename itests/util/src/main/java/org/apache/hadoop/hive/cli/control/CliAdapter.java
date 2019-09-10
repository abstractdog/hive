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
package org.apache.hadoop.hive.cli.control;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.QTestMetaStoreHandler;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * This class adapts old vm test-executors to be executed in multiple instances
 */
public abstract class CliAdapter {

  protected final AbstractCliConfig cliConfig;
  protected QTestMetaStoreHandler metaStoreHandler;

  public CliAdapter(AbstractCliConfig cliConfig) {
    this.cliConfig = cliConfig;
    metaStoreHandler = new QTestMetaStoreHandler();
  }

  public final List<Object[]> getParameters() throws Exception {
    Set<File> f = cliConfig.getQueryFiles();
    List<Object[]> ret = new ArrayList<>();

    for (File file : f) {
      String label = file.getName().replaceAll("\\.[^\\.]+$", "");
      ret.add(new Object[] { label, file });
    }
    return ret;
  }

  public abstract void beforeClass() throws Exception;

  // HIVE-14444 pending rename: before
  public abstract void setUp();

  // HIVE-14444 pending rename: after
  public abstract void tearDown();

  // HIVE-14444 pending rename: afterClass
  public abstract void shutdown() throws Exception;

  public abstract void runTest(String name, String fileName, String absolutePath) throws Exception;

  public final TestRule buildClassRule() {
    return new TestRule() {
      @Override
      public Statement apply(final Statement base, Description description) {
        return new Statement() {
          @Override
          public void evaluate() throws Throwable {
            metaStoreHandler.getRule().before();
            metaStoreHandler.getRule().install();
            metaStoreHandler.setSystemProperties();
            CliAdapter.this.beforeClass(); // instantiating QTestUtil
            metaStoreHandler.setMetaStoreConfiguration(getQt().getConf());
            try {
              base.evaluate();
            } finally {
              CliAdapter.this.shutdown();
              metaStoreHandler.getRule().after();
            }
          }
        };
      }
    };
  }

  public final TestRule buildTestRule() {
    return new TestRule() {
      @Override
      public Statement apply(final Statement base, Description description) {
        return new Statement() {
          @Override
          public void evaluate() throws Throwable {
            metaStoreHandler.setMetaStoreConfiguration(getQt().getConf());
            CliAdapter.this.setUp();
            try {
              base.evaluate();
            } finally {
              CliAdapter.this.tearDown();
              metaStoreHandler.cleanupMetaStore(getQt().getConf());
            }
          }
        };
      }
    };
  }

  //FIXME: make this abstract
  protected QTestUtil getQt() {
    return null;
  }

  // HIVE-14444: pending refactor to push File forward
  public final void runTest(String name, File qfile) throws Exception {
    runTest(name, qfile.getName(), qfile.getAbsolutePath());
  }

}
