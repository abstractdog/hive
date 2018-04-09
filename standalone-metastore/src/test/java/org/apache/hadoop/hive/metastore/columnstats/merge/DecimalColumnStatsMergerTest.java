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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.columnstats.merge;

import java.nio.ByteBuffer;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class DecimalColumnStatsMergerTest {

  private static final Decimal DECIMAL_VALUE = getDecimal(1, 0);

  private DecimalColumnStatsMerger merger = new DecimalColumnStatsMerger();

  @Test
  public void testMergeNullMinMaxValues() {
    ColumnStatisticsObj objNulls = new ColumnStatisticsObj();
    createData(objNulls, null, null);

    merger.merge(objNulls, objNulls);

    Assert.assertNull(objNulls.getStatsData().getDecimalStats().getLowValue());
    Assert.assertNull(objNulls.getStatsData().getDecimalStats().getHighValue());
  }

  @Test
  public void testMergeNonNullAndNullLowerValuesOldIsNull() {
    ColumnStatisticsObj oldObj = new ColumnStatisticsObj();
    createData(oldObj, null, null);

    ColumnStatisticsObj newObj = new ColumnStatisticsObj();
    createData(newObj, DECIMAL_VALUE, null);

    merger.merge(oldObj, newObj);

    Assert.assertEquals(getInt(DECIMAL_VALUE),
        getInt(oldObj.getStatsData().getDecimalStats().getLowValue()));
  }

  @Test
  public void testMergeNonNullAndNullLowerValuesNewIsNull() {
    ColumnStatisticsObj oldObj = new ColumnStatisticsObj();
    createData(oldObj, DECIMAL_VALUE, null);

    ColumnStatisticsObj newObj = new ColumnStatisticsObj();
    createData(newObj, null, null);

    merger.merge(oldObj, newObj);

    Assert.assertEquals(getInt(DECIMAL_VALUE),
        getInt(oldObj.getStatsData().getDecimalStats().getLowValue()));
  }

  @Test
  public void testMergeNonNullAndNullHigherValuesOldIsNull() {
    ColumnStatisticsObj oldObj = new ColumnStatisticsObj();
    createData(oldObj, null, null);

    ColumnStatisticsObj newObj = new ColumnStatisticsObj();
    createData(newObj, null, DECIMAL_VALUE);

    merger.merge(oldObj, newObj);

    Assert.assertEquals(getInt(DECIMAL_VALUE),
        getInt(oldObj.getStatsData().getDecimalStats().getHighValue()));
  }

  @Test
  public void testMergeNonNullAndNullHigherValuesNewIsNull() {
    ColumnStatisticsObj oldObj = new ColumnStatisticsObj();
    createData(oldObj, null, DECIMAL_VALUE);

    ColumnStatisticsObj newObj = new ColumnStatisticsObj();
    createData(newObj, null, null);

    merger.merge(oldObj, newObj);

    Assert.assertEquals(getInt(DECIMAL_VALUE),
        getInt(oldObj.getStatsData().getDecimalStats().getHighValue()));
  }

  private int getInt(Decimal decimal) {
    return ByteBuffer.wrap(decimal.getUnscaled()).getInt();
  }

  private static Decimal getDecimal(int number, int scale) {
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.asIntBuffer().put(number);
    return new Decimal(bb, (short) scale);
  }

  private DecimalColumnStatsDataInspector createData(ColumnStatisticsObj objNulls, Decimal lowValue,
      Decimal highValue) {
    ColumnStatisticsData statisticsData = new ColumnStatisticsData();
    DecimalColumnStatsDataInspector data = new DecimalColumnStatsDataInspector();

    statisticsData.setDecimalStats(data);
    objNulls.setStatsData(statisticsData);

    data.setLowValue(lowValue);
    data.setHighValue(highValue);
    return data;
  }
}
