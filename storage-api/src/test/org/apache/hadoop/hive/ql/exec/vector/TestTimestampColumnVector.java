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

package org.apache.hadoop.hive.ql.exec.vector;

import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.GregorianCalendar;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.type.RandomTypeUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for ListColumnVector
 */
public class TestTimestampColumnVector {
  private static final GregorianCalendar PROLEPTIC_GREGORIAN_CALENDAR_UTC =
      new GregorianCalendar(TimeZone.getTimeZone("UTC"));
  private static final GregorianCalendar GREGORIAN_CALENDAR_UTC =
      new GregorianCalendar(TimeZone.getTimeZone("UTC"));

  private static final SimpleDateFormat PROLEPTIC_GREGORIAN_TIMESTAMP_FORMATTER_UTC =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static final SimpleDateFormat GREGORIAN_TIMESTAMP_FORMATTER_UTC =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  static {
    PROLEPTIC_GREGORIAN_CALENDAR_UTC.setGregorianChange(new java.util.Date(Long.MIN_VALUE));

    PROLEPTIC_GREGORIAN_TIMESTAMP_FORMATTER_UTC.setCalendar(PROLEPTIC_GREGORIAN_CALENDAR_UTC);
    GREGORIAN_TIMESTAMP_FORMATTER_UTC.setCalendar(GREGORIAN_CALENDAR_UTC);
  }

  @Test
  public void testSaveAndRetrieve() throws Exception {

    Random r = new Random(1234);
    TimestampColumnVector timestampColVector = new TimestampColumnVector();
    Timestamp[] randTimestamps = new Timestamp[VectorizedRowBatch.DEFAULT_SIZE];

    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      Timestamp randTimestamp = RandomTypeUtil.getRandTimestamp(r);
      randTimestamps[i] = randTimestamp;
      timestampColVector.set(i, randTimestamp);
    }
    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      Timestamp retrievedTimestamp = timestampColVector.asScratchTimestamp(i);
      Timestamp randTimestamp = randTimestamps[i];
      if (!retrievedTimestamp.equals(randTimestamp)) {
        assertTrue(false);
      }
    }
  }

  @Test
  public void testTimestampCompare() throws Exception {
    Random r = new Random(1234);
    TimestampColumnVector timestampColVector = new TimestampColumnVector();
    Timestamp[] randTimestamps = new Timestamp[VectorizedRowBatch.DEFAULT_SIZE];
    Timestamp[] candTimestamps = new Timestamp[VectorizedRowBatch.DEFAULT_SIZE];
    int[] compareToLeftRights = new int[VectorizedRowBatch.DEFAULT_SIZE];
    int[] compareToRightLefts = new int[VectorizedRowBatch.DEFAULT_SIZE];

    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      Timestamp randTimestamp = RandomTypeUtil.getRandTimestamp(r);
      randTimestamps[i] = randTimestamp;
      timestampColVector.set(i, randTimestamp);
      Timestamp candTimestamp = RandomTypeUtil.getRandTimestamp(r);
      candTimestamps[i] = candTimestamp;
      compareToLeftRights[i] = candTimestamp.compareTo(randTimestamp);
      compareToRightLefts[i] = randTimestamp.compareTo(candTimestamp);
    }

    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      Timestamp retrievedTimestamp = timestampColVector.asScratchTimestamp(i);
      Timestamp randTimestamp = randTimestamps[i];
      if (!retrievedTimestamp.equals(randTimestamp)) {
        assertTrue(false);
      }
      Timestamp candTimestamp = candTimestamps[i];
      int compareToLeftRight = timestampColVector.compareTo(candTimestamp, i);
      if (compareToLeftRight != compareToLeftRights[i]) {
        assertTrue(false);
      }
      int compareToRightLeft = timestampColVector.compareTo(i, candTimestamp);
      if (compareToRightLeft != compareToRightLefts[i]) {
        assertTrue(false);
      }
    }
  }

  /**
   * Test case for TimestampColumnVector's changeCalendar
   *   16768: hybrid: 2015-11-29 proleptic: 2015-11-29
   * -141418: hybrid: 1582-10-24 proleptic: 1582-10-24
   * -141427: hybrid: 1582-10-15 proleptic: 1582-10-15
   * -141428: hybrid: 1582-10-04 proleptic: 1582-10-14
   * -141430: hybrid: 1582-10-02 proleptic: 1582-10-12
   * -141437: hybrid: 1582-09-25 proleptic: 1582-10-05
   * -141438: hybrid: 1582-09-24 proleptic: 1582-10-04
   * -499952: hybrid: 0601-03-04 proleptic: 0601-03-07
   * -499955: hybrid: 0601-03-01 proleptic: 0601-03-04
   */
  @Test
  public void testProlepticCalendar() {
    // from hybrid internal representation to proleptic
    setAndVerifyProlepticUpdate(16768, appendTime("2015-11-29"), false, true);
    setAndVerifyProlepticUpdate(-141418, appendTime("1582-10-24"), false, true);
    setAndVerifyProlepticUpdate(-141427, appendTime("1582-10-15"), false, true);
    setAndVerifyProlepticUpdate(-141428, appendTime("1582-10-04"), false, true);
    setAndVerifyProlepticUpdate(-141430, appendTime("1582-10-02"), false, true);
    setAndVerifyProlepticUpdate(-141437, appendTime("1582-09-25"), false, true);
    setAndVerifyProlepticUpdate(-499952, appendTime("0601-03-04"), false, true);
    setAndVerifyProlepticUpdate(-499955, appendTime("0601-03-01"), false, true);

    // from proleptic internal representation to hybrid
    setAndVerifyProlepticUpdate(16768, appendTime("2015-11-29"), true, false);
    setAndVerifyProlepticUpdate(-141418, appendTime("1582-10-24"), true, false);
    setAndVerifyProlepticUpdate(-141427, appendTime("1582-10-15"), true, false);
    setAndVerifyProlepticUpdate(-141428, appendTime("1582-10-24"), true, false);
    setAndVerifyProlepticUpdate(-141430, appendTime("1582-10-22"), true, false);
    setAndVerifyProlepticUpdate(-141437, appendTime("1582-10-15"), true, false);
    setAndVerifyProlepticUpdate(-499952, appendTime("0601-03-07"), true, false);
    setAndVerifyProlepticUpdate(-499955, appendTime("0601-03-04"), true, false);
  }

  private String appendTime(String string) {
    return string + " 00:00:00.000";
  }

  private void setAndVerifyProlepticUpdate(long epochDay, String expected,
      boolean originalUseProleptic, boolean newUseProleptic) {
    long epochMilli = TimeUnit.DAYS.toMillis(epochDay);

    DateFormat testFormatter = getTestFormatter(newUseProleptic);

    Instant instant = Instant.ofEpochMilli(epochMilli); // instant is always a moment in UTC

    int nanos = instant.getNano() + new Random().nextInt(999999) + 0;
    TimestampColumnVector timestampColVector =
        new TimestampColumnVector().setUsingProlepticCalendar(originalUseProleptic);

    timestampColVector.time[0] = instant.toEpochMilli();
    timestampColVector.nanos[0] = nanos;

    timestampColVector.changeCalendar(newUseProleptic, true);

    Assert.assertEquals(expected,
        testFormatter.format(Timestamp.from(Instant.ofEpochMilli(timestampColVector.time[0]))));
    Assert.assertEquals(nanos, timestampColVector.nanos[0]); // preserving nanos
  }

  private DateFormat getTestFormatter(boolean useProleptic) {
    DateFormat testFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    if (useProleptic) {
      testFormatter.setCalendar(PROLEPTIC_GREGORIAN_CALENDAR_UTC);
    } else {
      testFormatter.setCalendar(GREGORIAN_CALENDAR_UTC);
    }

    testFormatter.setLenient(false);

    return testFormatter;
  }
}
