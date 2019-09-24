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

package io.cdap.plugin.google.drive.source.utils;

import io.cdap.plugin.google.common.exceptions.InvalidPropertyTypeException;
import io.cdap.plugin.google.drive.source.GoogleDriveSourceConfig;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An enum which represent a type of dare range of file modification.
 */
public enum ModifiedDateRangeType {
  TODAY("today"),
  YESTERDAY("yesterday"),
  THIS_WEEK_SUN_TODAY("this_week_sun_today"),
  THIS_WEEK_MON_TODAY("this_week_mon_today"),
  LAST_WEEK_SUN_SAT("last_week_sun_sat"),
  LAST_WEEK_MON_SUN("last_week_mon_sun"),
  THIS_MONTH("this_month"),
  LAST_MONTH("last_month"),
  THIS_QUARTER("this_quarter"),
  LAST_3D("last_3d"),
  LAST_7D("last_7d"),
  LAST_14D("last_14d"),
  LAST_28D("last_28d"),
  LAST_30D("last_30d"),
  LAST_90D("last_90d"),
  THIS_YEAR("this_year"),
  LAST_YEAR("last_year"),
  LIFETIME("lifetime"),
  CUSTOM("custom");

  private final String value;

  ModifiedDateRangeType(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  /**
   * Returns ModifiedDateRangeType.
   * @param value The value is String type.
   * @return The ModifiedDateRangeType
   */
  public static ModifiedDateRangeType fromValue(String value) {
    return Stream.of(ModifiedDateRangeType.values())
      .filter(keyType -> keyType.getValue().equalsIgnoreCase(value))
      .findAny()
      .orElseThrow(() -> new InvalidPropertyTypeException(GoogleDriveSourceConfig.MODIFICATION_DATE_RANGE_LABEL,
                                                          value, getAllowedValues()));
  }

  public static List<String> getAllowedValues() {
    return Arrays.stream(ModifiedDateRangeType.values()).map(v -> v.getValue())
        .collect(Collectors.toList());
  }
}
