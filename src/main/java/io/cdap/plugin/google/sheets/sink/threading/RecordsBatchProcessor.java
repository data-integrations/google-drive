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

import com.google.api.services.sheets.v4.model.Request;
import io.cdap.plugin.google.sheets.sink.GoogleSheetsSinkClient;
import io.cdap.plugin.google.sheets.sink.utils.FlatteredRowsRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

/**
 * Task that executes all requests from {@link RecordsBatch} by single Sheets API request.
 * Finally it releases threadsSemaphore semaphore as signal that another instance may be submitted.
 */
public class RecordsBatchProcessor implements Callable {

  private final GoogleSheetsSinkClient sheetsSinkClient;
  private final RecordsBatch recordsBatch;
  private final Semaphore threadsSemaphore;

  public RecordsBatchProcessor(GoogleSheetsSinkClient sheetsSinkClient, RecordsBatch recordsBatch,
                               Semaphore threadsSemaphore) {
    this.sheetsSinkClient = sheetsSinkClient;
    this.recordsBatch = recordsBatch;
    this.threadsSemaphore = threadsSemaphore;
  }

  @Override
  public Object call() throws Exception {
    try {
      List<FlatteredRowsRequest> recordsToProcess = recordsBatch.getGroup();
      if (recordsToProcess.isEmpty()) {
        return null;
      }
      String spreadsheetName = recordsBatch.getSpreadsheetName();

      List<Request> contentRequests = new ArrayList<>();
      List<Request> mergeRequests = new ArrayList<>();
      List<String> sheetTitles = new ArrayList<>();
      for (FlatteredRowsRequest flatteredRowsRequest : recordsToProcess) {
        contentRequests.add(flatteredRowsRequest.getContentRequest());
        mergeRequests.addAll(flatteredRowsRequest.getMergeRequests());
        sheetTitles.add(flatteredRowsRequest.getSheetTitle());
      }

      String spreadsheetId = recordsBatch.getSpreadsheetId();

      sheetsSinkClient.populateCells(spreadsheetId, spreadsheetName, sheetTitles,
        contentRequests, mergeRequests);
    } finally {
      threadsSemaphore.release();
      return null;
    }
  }
}
