/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.google.common.utils;

import io.cdap.plugin.google.common.GoogleDriveFilteringClient;

import java.time.DayOfWeek;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.regex.Pattern;

/**
 * Builds data range.
 */
public class ModifiedDateRangeUtils {

  private static final DateTimeFormatter DATE_TIME_FORMATTER =
    DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSXXX");
  private static final Pattern DATE_PATTERN =
    // RFC 3339 regex : year-month-dayT part
    Pattern.compile("^([0-9]+)-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])" +
                      // hour:minute:second part
                      "([Tt]([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]|60)" +
                      // .microseconds or partial-time
                      "(\\.[0-9]+)?(([Zz])|([\\+|\\-]([01][0-9]|2[0-3]):[0-5][0-9]))?)?$");

  /**
   * Returns the instance of DateRange.
   * @param modifiedDateRangeType The modified date range type with
   * @param startDate The start date  with
   * @param endDate The end date
   * @return The instance of DateRange
   * @throws InterruptedException if there was an error getting the column information for the data
   */
  public static DateRange getDataRange(ModifiedDateRangeType modifiedDateRangeType, String startDate, String endDate)
    throws InterruptedException {
    ZoneId zoneId = ZoneId.systemDefault();
    ZonedDateTime now = ZonedDateTime.now();
    switch (modifiedDateRangeType) {
      case TODAY:
        ZonedDateTime startOfDay = now.toLocalDate().atStartOfDay(zoneId);
        return convertFromLocalDateTimes(startOfDay, now);
      case YESTERDAY:
        ZonedDateTime startOfPreviousDay = now.minusDays(1).toLocalDate().atStartOfDay(zoneId);
        ZonedDateTime endOfPreviousDay = now.minusDays(1).toLocalDate().atTime(LocalTime.MAX).atZone(zoneId);
        return convertFromLocalDateTimes(startOfPreviousDay, endOfPreviousDay);
      case THIS_WEEK_SUN_TODAY:
        ZonedDateTime startOfRecentSun = now.with(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY))
          .toLocalDate().atStartOfDay(zoneId);
        return convertFromLocalDateTimes(startOfRecentSun, now);
      case THIS_WEEK_MON_TODAY:
        ZonedDateTime startOfRecentMon = now.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY))
          .toLocalDate().atStartOfDay(zoneId);
        return convertFromLocalDateTimes(startOfRecentMon, now);
      case LAST_WEEK_SUN_SAT:
        ZonedDateTime endOfRecentSat = now.with(TemporalAdjusters.previous(DayOfWeek.SATURDAY))
          .toLocalDate().atTime(LocalTime.MAX).atZone(zoneId);
        ZonedDateTime startOfPreRecentSun = endOfRecentSat.with(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY))
          .toLocalDate().atStartOfDay(zoneId);
        return convertFromLocalDateTimes(startOfPreRecentSun, endOfRecentSat);
      case LAST_WEEK_MON_SUN:
        ZonedDateTime endOfRecentSun = now.with(TemporalAdjusters.previous(DayOfWeek.SUNDAY))
          .toLocalDate().atTime(LocalTime.MAX).atZone(zoneId);
        ZonedDateTime startOfPreRecentMon = endOfRecentSun.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY))
          .toLocalDate().atStartOfDay(zoneId);
        return convertFromLocalDateTimes(startOfPreRecentMon, endOfRecentSun);
      case THIS_MONTH:
        ZonedDateTime startOfMonth = now.with(TemporalAdjusters.firstDayOfMonth())
          .toLocalDate().atTime(LocalTime.MIDNIGHT).atZone(zoneId);
        return convertFromLocalDateTimes(startOfMonth, now);
      case LAST_MONTH:
        ZonedDateTime startOfPreviousMonth = now.minusMonths(1).with(TemporalAdjusters.firstDayOfMonth())
          .toLocalDate().atTime(LocalTime.MIDNIGHT).atZone(zoneId);
        ZonedDateTime endOfPreviousMonth = now.minusMonths(1).with(TemporalAdjusters.lastDayOfMonth())
          .toLocalDate().atTime(LocalTime.MAX).atZone(zoneId);
        return convertFromLocalDateTimes(startOfPreviousMonth, endOfPreviousMonth);
      case THIS_QUARTER:
        ZonedDateTime startOfQuarter = now.with(now.getMonth().firstMonthOfQuarter())
          .with(TemporalAdjusters.firstDayOfMonth())
          .toLocalDate().atTime(LocalTime.MIDNIGHT).atZone(zoneId);
        return convertFromLocalDateTimes(startOfQuarter, now);
      case LAST_3D:
        return convertFromLocalDateTimes(getStartOfDaysAgo(now, zoneId, 3), getEndOfYesterday(now, zoneId));
      case LAST_7D:
        return convertFromLocalDateTimes(getStartOfDaysAgo(now, zoneId, 7), getEndOfYesterday(now, zoneId));
      case LAST_14D:
        return convertFromLocalDateTimes(getStartOfDaysAgo(now, zoneId, 14), getEndOfYesterday(now, zoneId));
      case LAST_28D:
        return convertFromLocalDateTimes(getStartOfDaysAgo(now, zoneId, 28), getEndOfYesterday(now, zoneId));
      case LAST_30D:
        return convertFromLocalDateTimes(getStartOfDaysAgo(now, zoneId, 30), getEndOfYesterday(now, zoneId));
      case LAST_90D:
        return convertFromLocalDateTimes(getStartOfDaysAgo(now, zoneId, 90), getEndOfYesterday(now, zoneId));
      case THIS_YEAR:
        ZonedDateTime startOfCurrentYear = now.with(TemporalAdjusters.firstDayOfYear())
          .toLocalDate().atTime(LocalTime.MIDNIGHT).atZone(zoneId);
        return convertFromLocalDateTimes(startOfCurrentYear, now);
      case LAST_YEAR:
        ZonedDateTime startOfPreviousYear = now.minusYears(1).with(TemporalAdjusters.firstDayOfYear())
          .toLocalDate().atTime(LocalTime.MIDNIGHT).atZone(zoneId);
        ZonedDateTime endOfPreviousYear = now.minusYears(1).with(TemporalAdjusters.lastDayOfYear())
          .toLocalDate().atTime(LocalTime.MAX).atZone(zoneId);
        return convertFromLocalDateTimes(startOfPreviousYear, endOfPreviousYear);
      case LIFETIME:
        return null;
      case CUSTOM:
        return new DateRange(startDate, endDate);
    }
    throw new IllegalArgumentException("No valid modified date range was selected.");
  }

  private static ZonedDateTime getEndOfYesterday(ZonedDateTime now, ZoneId zoneId) {
    return now.minusDays(1).toLocalDate().atTime(LocalTime.MAX).atZone(zoneId);
  }

  private static ZonedDateTime getStartOfDaysAgo(ZonedDateTime now, ZoneId zoneId, int daysAgo) {
    return now.minusDays(daysAgo).toLocalDate().atTime(LocalTime.MIDNIGHT).atZone(zoneId);
  }

  private static DateRange convertFromLocalDateTimes(ZonedDateTime fromDateTime,
                                                     ZonedDateTime toDateTime) {
    return new DateRange(fromDateTime.format(DATE_TIME_FORMATTER),
                         toDateTime.format(DATE_TIME_FORMATTER));
  }

  public static boolean isValidDateString(String dateString) {
    return DATE_PATTERN.matcher(dateString).matches();
  }

  /**
   * Returns the String.
   * @param dateRange The data range is reference of DateRange
   * @return The String
   */
  public static String getFilterValue(DateRange dateRange) {
    if (dateRange == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    sb.append(GoogleDriveFilteringClient.MODIFIED_TIME_TERM);
    sb.append(">='");
    sb.append(dateRange.getStartDate());
    sb.append("' and ");
    sb.append(GoogleDriveFilteringClient.MODIFIED_TIME_TERM);
    sb.append("<='");
    sb.append(dateRange.getEndDate());
    sb.append("'");
    return sb.toString();
  }
}
