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

package io.cdap.plugin.google.sheets.sink.utils;

import com.google.api.services.sheets.v4.model.Request;

import java.util.List;

/**
 * Wrapper for API requests related to one flattered record.
 */
public class FlatteredRowsRequest {
  private final Request contentRequest;
  private final List<Request> mergeRequests;
  private final int lastRowIndex;

  private String spreadsheetName;
  private String sheetTitle;

  public FlatteredRowsRequest(Request contentRequest, List<Request> mergeRequests,
                              int lastRowIndex) {

    this.contentRequest = contentRequest;
    this.mergeRequests = mergeRequests;
    this.lastRowIndex = lastRowIndex;
  }

  public Request getContentRequest() {
    return contentRequest;
  }

  public int getLastRowIndex() {
    return lastRowIndex;
  }

  public List<Request> getMergeRequests() {
    return mergeRequests;
  }

  public String getSpreadsheetName() {
    return spreadsheetName;
  }

  public String getSheetTitle() {
    return sheetTitle;
  }

  public void setSpreadsheetName(String spreadsheetName) {
    this.spreadsheetName = spreadsheetName;
  }

  public void setSheetTitle(String sheetTitle) {
    this.sheetTitle = sheetTitle;
  }
}
