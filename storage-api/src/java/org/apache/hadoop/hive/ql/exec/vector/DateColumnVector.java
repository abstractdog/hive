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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * This class extends LongColumnVector in order to introduce some date-specific semantics.
 */
public class DateColumnVector extends LongColumnVector implements ProlepticCalendarColumnVectorType {
  public static final GregorianCalendar PROLEPTIC_GREGORIAN_CALENDAR =
      new GregorianCalendar(TimeZone.getTimeZone("UTC".intern()));
  public static final GregorianCalendar GREGORIAN_CALENDAR =
      new GregorianCalendar(TimeZone.getTimeZone("UTC".intern()));

  private static final SimpleDateFormat PROLEPTIC_GREGORIAN_DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");
  private static final SimpleDateFormat GREGORIAN_DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");

  static {
    PROLEPTIC_GREGORIAN_CALENDAR.setGregorianChange(new java.util.Date(Long.MIN_VALUE));

    PROLEPTIC_GREGORIAN_CALENDAR.setLenient(false);
    GREGORIAN_CALENDAR.setLenient(false);

    PROLEPTIC_GREGORIAN_DATE_FORMATTER.setCalendar(PROLEPTIC_GREGORIAN_CALENDAR);
    GREGORIAN_DATE_FORMATTER.setCalendar(GREGORIAN_CALENDAR);
  }

  private boolean usingProlepticCalendar = false;

  public DateColumnVector() {
    this(VectorizedRowBatch.DEFAULT_SIZE);
  }

  @Override
  public void changeCalendar(boolean useProleptic, boolean updateData) {
    usingProlepticCalendar = useProleptic;
    if (updateData) {
      updateDataAccordingProlepticSetting();
    }
  }

  private void updateDataAccordingProlepticSetting() {
    for (int i = 0; i < vector.length; i++) {
      vector[i] =
          java.sql.Date.valueOf(usingProlepticCalendar ? PROLEPTIC_GREGORIAN_DATE_FORMATTER.format(new Date(vector[i]))
            : GREGORIAN_DATE_FORMATTER.format(new Date(vector[i]))).getTime();
    }
  }

  @Override
  public boolean usingProlepticCalendar() {
    return usingProlepticCalendar;
  }

  /**
   * Don't use this except for testing purposes.
   *
   * @param len the number of rows
   */
  public DateColumnVector(int len) {
    super(len);
  }

  @Override
  public void shallowCopyTo(ColumnVector otherCv) {
    DateColumnVector other = (DateColumnVector) otherCv;
    super.shallowCopyTo(other);
    other.vector = vector;
  }
}
