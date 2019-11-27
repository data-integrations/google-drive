/*
 * Copyright © 2019 Cask Data, Inc.
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
package io.cdap.plugin.google.sheets.sink.threading;

import io.cdap.plugin.google.sheets.sink.utils.FlatteredRowsRequest;

import java.util.List;

/**
 * Wrapper for batch of flattered requests that relate to the same spreadsheet.
 */
public class RecordsBatch implements Comparable<RecordsBatch> {
  private final List<FlatteredRowsRequest> group;
  private final String spreadsheetName;
  private final String spreadsheetId;

  public RecordsBatch(List<FlatteredRowsRequest> group, String spreadsheetName, String spreadsheetId) {
    this.group = group;
    this.spreadsheetName = spreadsheetName;
    this.spreadsheetId = spreadsheetId;
  }

  public void add(FlatteredRowsRequest record) {
    group.add(record);
  }

  public List<FlatteredRowsRequest> getGroup() {
    return group;
  }

  public String getSpreadsheetName() {
    return spreadsheetName;
  }

  public String getSpreadsheetId() {
    return spreadsheetId;
  }

  @Override
  public int compareTo(RecordsBatch o) {
    return o.group.size() - this.group.size();
  }
}
