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

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFPercentileCont.PercentileCalculator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFPercentileCont.PercentileContCalculator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFPercentileCont.PercentileContLongEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFPercentileCont.PercentileContLongEvaluator.PercentileAgg;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;
import org.junit.Test;

public class TestGenericUDAFPercentileDisc {
  PercentileCalculator calc = new PercentileContCalculator();

  @Test
  public void testNoInterpolation() throws Exception {
    Long[] items = new Long[] { 1L, 2L, 3L, 4L, 5L };
    checkPercentile(items, 0.5, 3);
  }

  @Test
  public void testInterpolateLower() throws Exception {
    Long[] items = new Long[] { 1L, 2L, 3L, 4L, 5L };
    checkPercentile(items, 0.49, 2.0);
  }

  @Test
  public void testInterpolateHigher() throws Exception {
    Long[] items = new Long[] { 1L, 2L, 3L, 4L, 5L };
    checkPercentile(items, 0.51, 3.0);
  }

  @Test
  public void testSingleItem50() throws Exception {
    Long[] items = new Long[] { 1L };
    checkPercentile(items, 0.5, 1);
  }

  @Test
  public void testSingleItem100() throws Exception {
    Long[] items = new Long[] { 1L };
    checkPercentile(items, 1, 1);
  }

  private void checkPercentile(Long[] items, double percentile, double expected) throws Exception {
    PercentileContLongEvaluator eval = new GenericUDAFPercentileDisc.PercentileDiscLongEvaluator();

    PercentileAgg agg = new PercentileAgg();

    agg.percentiles = new ArrayList<DoubleWritable>();
    agg.percentiles.add(new DoubleWritable(percentile));

    for (int i = 0; i < items.length; i++) {
      eval.increment(agg, new LongWritable(items[i]), 1);
    }

    DoubleWritable result = (DoubleWritable) eval.terminate(agg);

    Assert.assertEquals(expected, result.get(), 0.01);
    eval.close();
  }
}
