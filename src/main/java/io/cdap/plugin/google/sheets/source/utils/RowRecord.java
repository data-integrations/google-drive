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

package io.cdap.plugin.google.sheets.source.utils;

import java.util.Map;

/**
 * Representation for single row data. Row values are saved as recursive structures for nested columns support.
 */
public class RowRecord {
  private String spreadsheetName;
  private String sheetTitle;
  private Map<String, String> metadata;
  private Map<String, ComplexSingleValueColumn> headeredCells;
  private boolean isEmptyData;

  /**
   * Constructor for RowRecord object.
   * @param spreadsheetName The spread sheet name
   * @param sheetTitle The sheet title
   * @param metadata The metadata
   * @param headeredCells The headered cells
   * @param isEmptyData The isEmptyData
   */
  public RowRecord(String spreadsheetName, String sheetTitle, Map<String, String> metadata,
                   Map<String, ComplexSingleValueColumn> headeredCells, boolean isEmptyData) {
    this.spreadsheetName = spreadsheetName;
    this.sheetTitle = sheetTitle;
    this.metadata = metadata;
    this.headeredCells = headeredCells;
    this.isEmptyData = isEmptyData;
  }

  public String getSpreadsheetName() {
    return spreadsheetName;
  }

  public String getSheetTitle() {
    return sheetTitle;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public Map<String, ComplexSingleValueColumn> getHeaderedCells() {
    return headeredCells;
  }

  public boolean isEmptyData() {
    return isEmptyData;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }
}
