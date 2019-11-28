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

import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DriverUtils.class);

  public static void runOnDriver(HiveConf conf, String user, SessionState sessionState,
      String query) throws HiveException {
    runOnDriver(conf, user, sessionState, query, null, -1);
  }

  /**
   * For Query Based compaction to run the query to generate the compacted data.
   */
  public static void runOnDriver(HiveConf conf, String user,
      SessionState sessionState, String query, ValidWriteIdList writeIds, long compactorTxnId)
      throws HiveException {
    if(writeIds != null && compactorTxnId < 0) {
      throw new IllegalArgumentException(JavaUtils.txnIdToString(compactorTxnId) +
          " is not valid. Context: " + query);
    }
    SessionState.setCurrentSessionState(sessionState);
    boolean isOk = false;
    try {
      QueryState qs = new QueryState.Builder().withHiveConf(conf).withGenerateNewQueryId(true).nonIsolated().build();
      Driver driver = new Driver(qs, user, null, null);
      driver.setCompactionWriteIds(writeIds, compactorTxnId);
      try {
        try {
          driver.run(query);
        } catch (CommandProcessorException e) {
          LOG.error("Failed to run " + query, e);
          throw new HiveException("Failed to run " + query, e);
        }
      } finally {
        driver.close();
        driver.destroy();
      }
      isOk = true;
    } finally {
      if (!isOk) {
        try {
          sessionState.close(); // This also resets SessionState.get.
        } catch (Throwable th) {
          LOG.warn("Failed to close a bad session", th);
          SessionState.detachSession();
        }
      }
    }
  }

  public static SessionState setUpSessionState(HiveConf conf, String user, boolean doStart) {
    SessionState sessionState = SessionState.get();
    if (sessionState == null) {
      // Note: we assume that workers run on the same threads repeatedly, so we can set up
      //       the session here and it will be reused without explicitly storing in the worker.
      sessionState = new SessionState(conf, user);
      if (doStart) {
        // TODO: Required due to SessionState.getHDFSSessionPath. Why wasn't it required before?
        sessionState.setIsHiveServerQuery(true);
        SessionState.start(sessionState);
      }
      SessionState.setCurrentSessionState(sessionState);
    }
    return sessionState;
  }
}
