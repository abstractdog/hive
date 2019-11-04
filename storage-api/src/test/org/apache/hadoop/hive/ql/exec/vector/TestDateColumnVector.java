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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;

import org.junit.Assert;
import org.junit.Test;

public class TestDateColumnVector {

  /**
   * Test case for DateColumnVector's changeCalendar
   * @throws Exception 
   */
  @Test
  public void testProlepticCalendar() throws Exception {
    // proleptic
    // gregorian day as proleptic gregorian date
    setDateAndVerifyProlepticUpdate("2015-11-29", "2015-11-29", true);

    // first gregorian day as proleptic gregorian date
    setDateAndVerifyProlepticUpdate("1582-10-15", "1582-10-15", true);

    // a day before first gregorian day as proleptic gregorian date
    setDateAndVerifyProlepticUpdate("1582-10-14", "1582-10-24", true);

    // a day after last julian day as proleptic gregorian date
    setDateAndVerifyProlepticUpdate("1582-10-05", "1582-10-15", true);

    // last julian day as proleptic gregorian date
    setDateAndVerifyProlepticUpdate("1582-10-04", "1582-10-14", true);

    // older julian day as propleptic gregorian date
    setDateAndVerifyProlepticUpdate("0601-03-04", "0601-03-07", true);

    // non-proleptic
    // gregorian day as non-proleptic gregorian date
    setDateAndVerifyProlepticUpdate("2015-11-29", "2015-11-29", false);

    // first gregorian day as non-proleptic gregorian date
    setDateAndVerifyProlepticUpdate("1582-10-15", "1582-10-15", false);

    // a day before first gregorian day as non-proleptic gregorian date
    setDateAndVerifyProlepticUpdate("1582-10-14", "1582-10-04", false);

    // a day after last julian day as non-proleptic gregorian date
    setDateAndVerifyProlepticUpdate("1582-10-05", "1582-09-25", false);

    // last julian day as non-propleptic gregorian date
    setDateAndVerifyProlepticUpdate("1582-10-04", "1582-09-24", false);

    // older julian day as non-propleptic gregorian date
    setDateAndVerifyProlepticUpdate("0601-03-04", "0601-03-01", false);
  }

  private void setDateAndVerifyProlepticUpdate(String dateString,
      String expectedGregorianDateString, boolean useProleptic) throws Exception {
    Instant instant = Instant.parse(dateString + "T00:00:00Z");
    long timestamp = instant.toEpochMilli();

    DateColumnVector dateColumnVector = new DateColumnVector();
    dateColumnVector.vector[0] = timestamp;

    dateColumnVector.changeCalendar(useProleptic, true);

    Assert.assertEquals(expectedGregorianDateString,
        getTestFormatter(useProleptic).format(dateColumnVector.vector[0]));
  }

  private DateFormat getTestFormatter(boolean useProleptic) {
    DateFormat testFormatter = new SimpleDateFormat("yyyy-MM-dd");
    if (useProleptic) {
      testFormatter.setCalendar(DateColumnVector.PROLEPTIC_GREGORIAN_CALENDAR);
    } else {
      testFormatter.setCalendar(DateColumnVector.GREGORIAN_CALENDAR);
    }
    testFormatter.setLenient(false);

    return testFormatter;
  }
}
