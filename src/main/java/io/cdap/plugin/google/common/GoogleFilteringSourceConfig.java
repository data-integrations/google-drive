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

package io.cdap.plugin.google.common;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.google.common.exceptions.InvalidPropertyTypeException;
import io.cdap.plugin.google.common.utils.ModifiedDateRangeType;
import io.cdap.plugin.google.common.utils.ModifiedDateRangeUtils;

import javax.annotation.Nullable;

/**
 * Base Google filtering batch config. Contains common filtering configuration properties and methods.
 */
public class GoogleFilteringSourceConfig extends GoogleAuthBaseConfig {
  public static final String FILTER = "filter";
  public static final String MODIFICATION_DATE_RANGE = "modificationDateRange";
  public static final String START_DATE = "startDate";
  public static final String END_DATE = "endDate";

  public static final String MODIFICATION_DATE_RANGE_LABEL = "Modification date range";
  public static final String START_DATE_LABEL = "Start date";
  public static final String END_DATE_LABEL = "End date";
  public static final String FILE_TYPES_TO_PULL_LABEL = "File types to pull";

  private static final String IS_VALID_FAILURE_MESSAGE_PATTERN = "'%s' property has invalid value %s.";

  @Nullable
  @Name(FILTER)
  @Description("Filter that can be applied to the files in the selected directory. \n" +
    "Filters follow the [Google Drive filters syntax](https://developers.google.com/drive/api/v3/ref-search-terms).")
  protected String filter;

  @Name(MODIFICATION_DATE_RANGE)
  @Nullable
  @Description("Filter that narrows set of files by modified date range. \n" +
    "User can select either among predefined or custom entered ranges. \n" +
    "For _Custom_ selection the dates range can be specified via **Start date** and **End date**.")
  protected String modificationDateRange;

  @Nullable
  @Name(START_DATE)
  @Description("Start date for custom modification date range. \n" +
    "Is shown only when 'Custom' range is selected for 'Modification date range' field. \n" +
    "RFC3339 (https://tools.ietf.org/html/rfc3339) format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.")
  protected String startDate;

  @Nullable
  @Name(END_DATE)
  @Description("End date for custom modification date range. \n" +
    "Is shown only when 'Custom' range is selected for 'Modification date range' field.\n" +
    "RFC3339 (https://tools.ietf.org/html/rfc3339) format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.")
  protected String endDate;

  public GoogleFilteringSourceConfig(String referenceName) {
    super();
  }

  public GoogleFilteringSourceConfig() {

  }

  /**
   * Returns the ValidationResult.
   *
   * @param collector the failure collector is provided
   * @return The ValidationResult
   */
  public ValidationResult validate(FailureCollector collector) {
    ValidationResult validationResult = super.validate(collector);
    if (validateModificationDateRange(collector)
      && getModificationDateRangeType().equals(ModifiedDateRangeType.CUSTOM)) {
      if (checkPropertyIsSet(collector, startDate, START_DATE, START_DATE_LABEL)) {
        checkPropertyIsValid(collector, ModifiedDateRangeUtils.isValidDateString(startDate), startDate, START_DATE,
                             START_DATE_LABEL);
      }
      if (checkPropertyIsSet(collector, endDate, END_DATE, END_DATE_LABEL)) {
        checkPropertyIsValid(collector, ModifiedDateRangeUtils.isValidDateString(endDate), startDate, END_DATE,
                             END_DATE_LABEL);
      }
    }
    return validationResult;
  }

  private boolean validateModificationDateRange(FailureCollector collector) {
    if (!containsMacro(MODIFICATION_DATE_RANGE) && IdentifierType.DIRECTORY_IDENTIFIER.equals(getIdentifierType())) {
      try {
        getModificationDateRangeType();
        return true;
      } catch (InvalidPropertyTypeException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(MODIFICATION_DATE_RANGE);
      }
    }
    return false;
  }

  protected boolean checkPropertyIsValid(FailureCollector collector, boolean isPropertyValid, String propertyName,
                                      Object propertyValue, String propertyLabel) {
    if (isPropertyValid) {
      return true;
    }
    collector.addFailure(String.format(IS_VALID_FAILURE_MESSAGE_PATTERN, propertyLabel, propertyValue), null)
      .withConfigProperty(propertyName);
    return false;
  }

  public ModifiedDateRangeType getModificationDateRangeType() {
    return ModifiedDateRangeType.fromValue(modificationDateRange);
  }

  @Nullable
  public String getFilter() {
    return filter;
  }

  public String getModificationDateRange() {
    return modificationDateRange;
  }

  @Nullable
  public String getStartDate() {
    return startDate;
  }

  @Nullable
  public String getEndDate() {
    return endDate;
  }
}
