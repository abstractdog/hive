/**
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

/**
 * This class contains unit tests for QTestUtil
 */
public class TestQTestUtil {
  private static final String HDFS_MASK = "###";

  @Test
  public void testSelectiveHdfsPatternMaskOnlyHdfsPath() {
    Assert.assertEquals(maskHdfs("nothing to be masked"), "nothing to be masked");
    Assert.assertEquals(maskHdfs("hdfs://"), "hdfs://");
    Assert.assertEquals(maskHdfs("hdfs://a"), String.format("hdfs://%s", HDFS_MASK));
    Assert.assertEquals(maskHdfs("hdfs://tmp.dfs.com:50029/tmp other text"),
        String.format("hdfs://%s other text", HDFS_MASK));
    Assert.assertEquals(
        maskHdfs(
            "hdfs://localhost:51594/build/ql/test/data/warehouse/default/encrypted_table_dp/p=2014-09-23"),
        String.format("hdfs://%s", HDFS_MASK));

    String line = maskHdfs("hdfs://localhost:11111/tmp/ct_noperm_loc_foo1");
    Assert.assertEquals(line, String.format("hdfs://%s", HDFS_MASK));

    line = maskHdfs("hdfs://one hdfs://two");
    Assert.assertEquals(line, String.format("hdfs://%s hdfs://%s", HDFS_MASK, HDFS_MASK));

    line = maskHdfs(
        "some text before [name=hdfs://localhost:11111/tmp/ct_noperm_loc_foo1]] some text between hdfs://localhost:22222/tmp/ct_noperm_loc_foo2 some text after");
    Assert.assertEquals(line,
        String.format(
            "some text before [name=hdfs://%s]] some text between hdfs://%s some text after",
            HDFS_MASK, HDFS_MASK));
  }

  private String maskHdfs(String line) {
    Matcher matcher = Pattern.compile(QTestUtil.PATH_HDFS_REGEX).matcher(line);

    if (matcher.find()) {
      line = matcher.replaceAll(String.format("$1%s", HDFS_MASK));
    }

    return line;
  }
}