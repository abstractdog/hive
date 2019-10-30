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

import org.junit.Test;

import org.junit.Assert;

import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.hive.common.type.RandomTypeUtil;

import static org.junit.Assert.*;

/**
 * Test for ListColumnVector
 */
public class TestTimestampColumnVector {

  private static int TEST_COUNT = 5000;

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

  @Test
  public void testProlepticCalendar() {
    fillAndVerify("2015-11-29T12:00:00.00Z", "2015-11-29 12:00:00", true, true, true);
    fillAndVerify("2015-11-29T12:00:00.00Z", "2015-11-29 12:00:00", true, true, false);

    fillAndVerify("1582-10-15T11:17:22Z", "1582-10-15 11:17:22", true, true, true);
    fillAndVerify("1582-10-15T11:17:22Z", "1582-10-15 11:17:22", true, true, false);

    fillAndVerify("1582-10-14T11:17:22Z", "1582-10-24 11:17:22", true, true, true);
    fillAndVerify("1582-10-14T11:17:22Z", "1582-10-24 11:17:22", true, true, false);

    fillAndVerify("1582-10-04T11:17:22Z", "1582-10-14 11:17:22", true, true, true);
    fillAndVerify("1582-10-04T11:17:22Z", "1582-10-14 11:17:22", true, true, false);

    fillAndVerify("0601-03-04T11:17:22Z", "0601-03-07 11:17:22", true, true, true);
    fillAndVerify("0601-03-04T11:17:22Z", "0601-03-07 11:17:22", true, true, false);
  }

  private void fillAndVerify(String momentInUtc, String expected, boolean useProleptic, boolean changeData, boolean isUTC) {
    DateFormat testFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    TimeZone timeZone = isUTC ? TimeZone.getTimeZone("UTC") : TimeZone.getDefault();
    Instant instant = Instant.parse(momentInUtc); // instant is always a moment in UTC
    long offsetFromUTC = timeZone.getOffset(instant.toEpochMilli());

    if (useProleptic) {
      testFormatter.setCalendar(DateColumnVector.PROLEPTIC_GREGORIAN_CALENDAR);
    } else {
      testFormatter.setCalendar(DateColumnVector.GREGORIAN_CALENDAR);
    }
    testFormatter.setTimeZone(timeZone);
    testFormatter.setLenient(false);

    TimestampColumnVector timestampColVector = new TimestampColumnVector();
    timestampColVector.setIsUTC(isUTC);
    timestampColVector.time[0] = instant.toEpochMilli();

    System.out.println(timestampColVector.time[0]);
    timestampColVector.changeCalendar(useProleptic, changeData);
    System.out.println(timestampColVector.time[0]);

    System.out.println(Timestamp.from(Instant.ofEpochMilli(timestampColVector.time[0])));
    Assert.assertEquals(expected,
        testFormatter.format(Timestamp.from(Instant.ofEpochMilli(timestampColVector.time[0] - offsetFromUTC))));
  }
  /*
  @Test
  public void testGenerate() throws Exception {
    PrintWriter writer = new PrintWriter("/Users/you/timestamps.txt");
    Random r = new Random(18485);
    for (int i = 0; i < 25; i++) {
      Timestamp randTimestamp = RandomTypeUtil.getRandTimestamp(r);
      writer.println(randTimestamp.toString());
    }
    for (int i = 0; i < 25; i++) {
      Timestamp randTimestamp = RandomTypeUtil.getRandTimestamp(r, 1965, 2025);
      writer.println(randTimestamp.toString());
    }
    writer.close();
  }
  */
}
