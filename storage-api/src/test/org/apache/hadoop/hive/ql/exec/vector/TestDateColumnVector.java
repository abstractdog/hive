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

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class TestDateColumnVector {
  private static final SimpleDateFormat PROLEPTIC_GREGORIAN_DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");
  private static final SimpleDateFormat GREGORIAN_DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");

  static {
    PROLEPTIC_GREGORIAN_DATE_FORMATTER.setCalendar(DateColumnVector.PROLEPTIC_GREGORIAN_CALENDAR);
    GREGORIAN_DATE_FORMATTER.setCalendar(DateColumnVector.GREGORIAN_CALENDAR);
  }

  /**
   * Test case for DateColumnVector's changeCalendar
   * REFERENCE for EPOCH DAYS:
   * epoch days, hybrid representation, proleptic representation
   *   16768: hybrid: 2015-11-29 proleptic: 2015-11-29
   * -141427: hybrid: 1582-10-15 proleptic: 1582-10-15
   * -141428: hybrid: 1582-10-04 proleptic: 1582-10-14
   * -141430: hybrid: 1582-10-02 proleptic: 1582-10-12
   * -141437: hybrid: 1582-09-25 proleptic: 1582-10-05
   * -141438: hybrid: 1582-09-24 proleptic: 1582-10-04
   * -499952: hybrid: 0601-03-04 proleptic: 0601-03-07
   * -499955: hybrid: 0601-03-01 proleptic: 0601-03-04
   * @throws Exception 
   */
  @Test
  public void testProlepticCalendar() throws Exception {
    // non-proleptic to proleptic
    setDateAndVerifyProlepticUpdate(16768, "2015-11-29", false, true);
    setDateAndVerifyProlepticUpdate(-141427, "1582-10-15", false, true);
    setDateAndVerifyProlepticUpdate(-141428, "1582-10-14", false, true);
    setDateAndVerifyProlepticUpdate(-141430, "1582-10-12", false, true);
    setDateAndVerifyProlepticUpdate(-141437, "1582-10-05", false, true);
    setDateAndVerifyProlepticUpdate(-141438, "1582-10-04", false, true);
    setDateAndVerifyProlepticUpdate(-499952, "0601-03-07", false, true);

    // non-proleptic to non-proleptic
    setDateAndVerifyProlepticUpdate(16768, "2015-11-29", false, false);
    setDateAndVerifyProlepticUpdate(-141427, "1582-10-15", false, false);
    setDateAndVerifyProlepticUpdate(-141428, "1582-10-04", false, false);
    setDateAndVerifyProlepticUpdate(-141430, "1582-10-02", false, false);
    setDateAndVerifyProlepticUpdate(-141437, "1582-09-25", false, false);
    setDateAndVerifyProlepticUpdate(-141438, "1582-09-24", false, false);
    setDateAndVerifyProlepticUpdate(-499952, "0601-03-04", false, false);

    // proleptic to non-proleptic
    setDateAndVerifyProlepticUpdate(16768, "2015-11-29", true, false);
    setDateAndVerifyProlepticUpdate(-141427, "1582-10-15", true, false);
    setDateAndVerifyProlepticUpdate(-141428, "1582-10-04", true, false);
    setDateAndVerifyProlepticUpdate(-141430, "1582-10-02", true, false);
    setDateAndVerifyProlepticUpdate(-141437, "1582-09-25", true, false);
    setDateAndVerifyProlepticUpdate(-141438, "1582-09-24", true, false);
    setDateAndVerifyProlepticUpdate(-499952, "0601-03-04", true, false);

    // proleptic to proleptic
    setDateAndVerifyProlepticUpdate(16768, "2015-11-29", true, true);
    setDateAndVerifyProlepticUpdate(-141427, "1582-10-15", true, true);
    setDateAndVerifyProlepticUpdate(-141428, "1582-10-14", true, true);
    setDateAndVerifyProlepticUpdate(-141430, "1582-10-12", true, true);
    setDateAndVerifyProlepticUpdate(-141437, "1582-10-05", true, true);
    setDateAndVerifyProlepticUpdate(-141438, "1582-10-04", true, true);
    setDateAndVerifyProlepticUpdate(-499952, "0601-03-07", true, true);
  }

  private void setDateAndVerifyProlepticUpdate(long epochDays, String expectedDateString,
      boolean originalUseProleptic, boolean newUseProleptic) throws Exception {
    long millis = TimeUnit.MILLISECONDS.toDays(epochDays);
    millis += TimeZone.getDefault().getOffset(millis);

    DateColumnVector dateColumnVector =
        new DateColumnVector().setUsingProlepticCalendar(originalUseProleptic);
    dateColumnVector.vector[0] = epochDays;

    //dateColumnVector.changeCalendar(newUseProleptic, true);
    dateColumnVector.setUsingProlepticCalendar(newUseProleptic);

    Assert.assertEquals(expectedDateString, dateColumnVector.stringifyValue(0));
    Assert.assertEquals(Date.valueOf(expectedDateString), dateColumnVector.getDateValue(0));
  }

  @Test
  public void testSqlDateParse() {
    printSqlDate("2015-11-29");
    printSqlDate("1582-10-24");
    printSqlDate("1582-10-15");
    printSqlDate("1582-10-14");
    printSqlDate("1582-10-05");
    printSqlDate("1582-10-04");
    printSqlDate("1582-10-02");
    printSqlDate("1582-09-25");
    printSqlDate("1582-09-24");
    printSqlDate("0601-03-04");
  }

  private void printSqlDate(String strDate) {
    System.out.println("original string: " + strDate + "-> Date.valueOf: " + java.sql.Date.valueOf(strDate) + " toDays(getTime()): "
        + TimeUnit.MILLISECONDS.toDays(java.sql.Date.valueOf(strDate).getTime()) + " proleptic: "
        + PROLEPTIC_GREGORIAN_DATE_FORMATTER.format(java.sql.Date.valueOf(strDate).getTime()));
  }
}
