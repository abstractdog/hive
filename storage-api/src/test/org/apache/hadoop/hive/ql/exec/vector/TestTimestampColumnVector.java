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
import java.util.Random;
import java.util.TimeZone;

import org.apache.hadoop.hive.common.type.RandomTypeUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for ListColumnVector
 */
public class TestTimestampColumnVector {

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
   */
  @Test
  public void testProlepticCalendar() {
    // proleptic
    // a random gregorian day as propleptic gregorian date
    setInstantAndVerifyProlepticUpdate("2015-11-29T12:00:00.123Z", "2015-11-29 12:00:00.123", true, true);
    setInstantAndVerifyProlepticUpdate("2015-11-29T12:00:00.123Z", "2015-11-29 12:00:00.123", true, false);

    // first gregorian day as propleptic gregorian date
    setInstantAndVerifyProlepticUpdate("1582-10-15T11:17:22.123Z", "1582-10-15 11:17:22.123", true, true);
    setInstantAndVerifyProlepticUpdate("1582-10-15T11:17:22.123Z", "1582-10-15 11:17:22.123", true, false);

    // a day before first gregorian day as propleptic gregorian date
    setInstantAndVerifyProlepticUpdate("1582-10-14T11:17:22.123Z", "1582-10-24 11:17:22.123", true, true);
    setInstantAndVerifyProlepticUpdate("1582-10-14T11:17:22.123Z", "1582-10-24 11:17:22.123", true, false);

    // a day after last julian day as propleptic gregorian date
    setInstantAndVerifyProlepticUpdate("1582-10-05T11:17:22.123Z", "1582-10-15 11:17:22.123", true, true);
    setInstantAndVerifyProlepticUpdate("1582-10-05T11:17:22.123Z", "1582-10-15 11:17:22.123", true, false);

    // last julian day as propleptic gregorian date
    setInstantAndVerifyProlepticUpdate("1582-10-04T11:17:22.123Z", "1582-10-14 11:17:22.123", true, true);
    setInstantAndVerifyProlepticUpdate("1582-10-04T11:17:22.123Z", "1582-10-14 11:17:22.123", true, false);

    // older julian day as propleptic gregorian date
    setInstantAndVerifyProlepticUpdate("0601-03-04T11:17:22.123Z", "0601-03-07 11:17:22.123", true, true);
    setInstantAndVerifyProlepticUpdate("0601-03-04T11:17:22.123Z", "0601-03-07 11:17:22.123", true, false);

    // non-proleptic
    // a random gregorian day as non-propleptic gregorian date
    setInstantAndVerifyProlepticUpdate("2015-11-29T12:00:00.123Z", "2015-11-29 12:00:00.123", false, true);
    setInstantAndVerifyProlepticUpdate("2015-11-29T12:00:00.123Z", "2015-11-29 12:00:00.123", false, false);

    // first gregorian day as non-propleptic gregorian date
    setInstantAndVerifyProlepticUpdate("1582-10-15T11:17:22.123Z", "1582-10-15 11:17:22.123", false, true);
    setInstantAndVerifyProlepticUpdate("1582-10-15T11:17:22.123Z", "1582-10-15 11:17:22.123", false, false);

    // a day before first gregorian day as non-propleptic gregorian date
    setInstantAndVerifyProlepticUpdate("1582-10-14T11:17:22.123Z", "1582-10-04 11:17:22.123", false, true);
    setInstantAndVerifyProlepticUpdate("1582-10-14T11:17:22.123Z", "1582-10-04 11:17:22.123", false, false);

    // a day after last julian day as non-propleptic gregorian date
    setInstantAndVerifyProlepticUpdate("1582-10-05T11:17:22.123Z", "1582-09-25 11:17:22.123", false, true);
    setInstantAndVerifyProlepticUpdate("1582-10-05T11:17:22.123Z", "1582-09-25 11:17:22.123", false, false);
 
    // last julian day as non-propleptic gregorian date
    setInstantAndVerifyProlepticUpdate("1582-10-04T11:17:22.123Z", "1582-09-24 11:17:22.123", false, true);
    setInstantAndVerifyProlepticUpdate("1582-10-04T11:17:22.123Z", "1582-09-24 11:17:22.123", false, false);

    // older julian day as non-propleptic gregorian date
    setInstantAndVerifyProlepticUpdate("0601-03-04T11:17:22.123Z", "0601-03-01 11:17:22.123", false, true);
    setInstantAndVerifyProlepticUpdate("0601-03-04T11:17:22.123Z", "0601-03-01 11:17:22.123", false, false);
  }

  private void setInstantAndVerifyProlepticUpdate(String momentInUtc, String expected,
      boolean useProleptic, boolean isUTC) {
    TimeZone timeZone = isUTC ? TimeZone.getTimeZone("UTC") : TimeZone.getDefault();
    DateFormat testFormatter = getTestFormatter(useProleptic, timeZone);

    Instant instant = Instant.parse(momentInUtc); // instant is always a moment in UTC
    long offsetFromUTC = timeZone.getOffset(instant.toEpochMilli());

    int nanos = instant.getNano() + new Random().nextInt(999999) + 0;
    TimestampColumnVector timestampColVector = new TimestampColumnVector();
    timestampColVector.setIsUTC(isUTC);
    timestampColVector.time[0] = instant.toEpochMilli();
    timestampColVector.nanos[0] = nanos;

    timestampColVector.changeCalendar(useProleptic, true);

    Assert.assertEquals(expected, testFormatter
        .format(Timestamp.from(Instant.ofEpochMilli(timestampColVector.time[0] - offsetFromUTC))));
    Assert.assertEquals(nanos, timestampColVector.nanos[0]); // preserving nanos
  }

  private DateFormat getTestFormatter(boolean useProleptic, TimeZone timeZone) {
    DateFormat testFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    if (useProleptic) {
      testFormatter.setCalendar(TimestampColumnVector.PROLEPTIC_GREGORIAN_CALENDAR_UTC);
    } else {
      testFormatter.setCalendar(TimestampColumnVector.GREGORIAN_CALENDAR_UTC);
    }
    testFormatter.setTimeZone(timeZone);
    testFormatter.setLenient(false);

    return testFormatter;
  }
}
