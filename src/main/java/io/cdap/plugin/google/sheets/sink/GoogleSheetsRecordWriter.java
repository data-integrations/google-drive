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

package io.cdap.plugin.google.sheets.sink;

import com.github.rholder.retry.RetryException;
import com.google.api.services.sheets.v4.model.SheetProperties;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import io.cdap.plugin.google.sheets.sink.threading.RecordsBatch;
import io.cdap.plugin.google.sheets.sink.threading.RecordsBatchProcessor;
import io.cdap.plugin.google.sheets.sink.utils.DimensionType;
import io.cdap.plugin.google.sheets.sink.utils.FlatteredRowsRecord;
import io.cdap.plugin.google.sheets.sink.utils.FlatteredRowsRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Writes {@link FlatteredRowsRecord} records to Google drive via {@link GoogleSheetsSinkClient}.
 * So have a single reducer, all writes are doing sequentially.
 * Firstly each input record is written to records queue {@link GoogleSheetsRecordWriter#recordsQueue}.
 * On queue exceeding (the size is defined by {@link GoogleSheetsSinkConfig#getRecordsQueueLength()}) or after timeout
 * {@link GoogleSheetsSinkConfig#getMaxFlushInterval()} queue is blocked and a new instance of tasks former is started
 * {@link TasksFormer}.
 * Task former groups all retrieved records by spreadsheet file name, limits groups size with buffer size
 * ({@link GoogleSheetsSinkConfig#getMaxBufferSize()}) and spread out the biggest ones between records batch processors
 * {@link RecordsBatchProcessor} on free threads.
 * All remaining grouped records are placed back to records queue. This allows to wait till queue gathers full
 * records buffer for each spreadsheet file.
 */
public class GoogleSheetsRecordWriter extends RecordWriter<NullWritable, FlatteredRowsRecord> {

  // variable that saves sheet Ids for each sheet of each spreadsheet
  private final Map<String, Map<String, Integer>> availableSheets = new HashMap<>();

  // variable that saves spreadsheet Id for each spreadsheet
  private final Map<String, String> availableFiles = new HashMap<>();

  // variable that saves the number of row for next record for each sheet
  // in other words - number of rows that were  written
  private final Map<String, Map<String, Integer>> sheetsContentShift = new HashMap<>();

  // variable that saves the number of all rows for each sheet
  private final Map<String, Map<String, Integer>> sheetsRowCount = new HashMap<>();

  // variable that saves the number of all columns for each sheet
  private final Map<String, Map<String, Integer>> sheetsColumnCount = new HashMap<>();

  // queue for all received records
  private final Queue<FlatteredRowsRequest> recordsQueue = new ConcurrentLinkedQueue<>();

  // semaphore for controlling to number of run message processors
  private final Semaphore threadsSemaphore;

  // semaphore for blocking the records queue while tasks former works
  private final Semaphore queueSemaphore = new Semaphore(1);

  // executor for records batch processors
  private final ExecutorService messageProcessorService;

  // scheduled executor for periodical performing of tasks former
  private ScheduledExecutorService formerScheduledExecutorService;

  // flag for signalizing of stop method performing
  private boolean stopSignal;

  // properties from config
  private final int flushTimeout;
  private final int maxBufferSize;
  private final int maxFlushInterval;

  private GoogleSheetsSinkClient sheetsSinkClient;
  private GoogleSheetsSinkConfig googleSheetsSinkConfig;

  /**
   * Constructor for GoogleSheetsRecordWriter object.
   * @param taskAttemptContext the task attempt context is provided
   * @throws IOException on issues with file reading
   */
  public GoogleSheetsRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(GoogleSheetsOutputFormatProvider.PROPERTY_CONFIG_JSON);
    googleSheetsSinkConfig =
      GoogleSheetsOutputFormatProvider.GSON.fromJson(configJson, GoogleSheetsSinkConfig.class);

    sheetsSinkClient = new GoogleSheetsSinkClient(googleSheetsSinkConfig);

    messageProcessorService = Executors.newFixedThreadPool(googleSheetsSinkConfig.getThreadsNumber());
    threadsSemaphore = new Semaphore(googleSheetsSinkConfig.getThreadsNumber());

    flushTimeout = googleSheetsSinkConfig.getFlushExecutionTimeout();
    maxBufferSize = googleSheetsSinkConfig.getMaxBufferSize();
    maxFlushInterval = googleSheetsSinkConfig.getMaxFlushInterval();

    formerScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    formerScheduledExecutorService.schedule(new TasksFormer(true),
      flushTimeout, TimeUnit.SECONDS);
  }

  @Override
  public void write(NullWritable nullWritable, FlatteredRowsRecord record) throws IOException, InterruptedException {
    try {
      String spreadsheetName = record.getSpreadsheetName();
      String sheetTitle = record.getSheetTitle();

      // create new spreadsheet file with sheet if needed
      createSpreadsheetIfNeeded(spreadsheetName, sheetTitle);
      String spreadsheetId = availableFiles.get(spreadsheetName);

      // create new sheet if needed
      createSheetIfNeeded(spreadsheetId, spreadsheetName, sheetTitle);
      Integer sheetId = availableSheets.get(spreadsheetName).get(sheetTitle);
      int currentShift = sheetsContentShift.get(spreadsheetName).get(sheetTitle);

      // for each flattered record
      // 1. prepare all needed merge and content requests
      FlatteredRowsRequest flatteredRowsRequest =
        sheetsSinkClient.prepareFlatteredRequest(sheetId, record, currentShift);
      flatteredRowsRequest.setSheetTitle(sheetTitle);
      flatteredRowsRequest.setSpreadsheetName(spreadsheetName);

      // 2. update shift for appropriate sheet
      sheetsContentShift.get(spreadsheetName).put(sheetTitle, flatteredRowsRequest.getLastRowIndex());

      // 3. extend column dimension if needed
      if (record.getHeader().getWidth() > sheetsColumnCount.get(spreadsheetName).get(sheetTitle)) {
        int extensionSize = record.getHeader().getWidth() -
          sheetsRowCount.get(spreadsheetName).get(sheetTitle);
        sheetsSinkClient.extendDimension(spreadsheetId, spreadsheetName, sheetTitle, sheetId, extensionSize,
          DimensionType.COLUMNS);
        sheetsColumnCount.get(spreadsheetName).put(sheetTitle, record.getHeader().getWidth());
      }

      // 4. extend rows dimension if needed
      if (sheetsContentShift.get(spreadsheetName).get(sheetTitle) >
        sheetsRowCount.get(spreadsheetName).get(sheetTitle)) {
        int minimalRequiredExtension = sheetsContentShift.get(spreadsheetName).get(sheetTitle) -
          sheetsRowCount.get(spreadsheetName).get(sheetTitle);
        int extensionSize = Math.max(minimalRequiredExtension, googleSheetsSinkConfig.getMinPageExtensionSize());
        sheetsSinkClient.extendDimension(spreadsheetId, spreadsheetName, sheetTitle, sheetId, extensionSize,
          DimensionType.ROWS);
        sheetsRowCount.get(spreadsheetName).put(sheetTitle,
          sheetsRowCount.get(spreadsheetName).get(sheetTitle) + extensionSize);
      }

      // 5. offer flattered requests to queue
      queueSemaphore.acquire();
      try {
        recordsQueue.offer(flatteredRowsRequest);
      } finally {
        queueSemaphore.release();
      }

      // 6. forcibly send requests to execution if the queue is exceeded
      if (recordsQueue.size() >= googleSheetsSinkConfig.getRecordsQueueLength()) {
        int counter = 0;
        while (recordsQueue.size() >= googleSheetsSinkConfig.getRecordsQueueLength() && !stopSignal) {
          if (counter > 0) {
            Thread.sleep(1000);
          }
          formerScheduledExecutorService.submit(new TasksFormer()).get();
          counter++;
        }
      }

    } catch (ExecutionException | RetryException e) {
      throw new RuntimeException("Exception during writing of flattered rows record.", e);
    }
  }

  private void createSpreadsheetIfNeeded(String spreadsheetName, String sheetTitle)
    throws ExecutionException, RetryException {
    if (!availableFiles.keySet().contains(spreadsheetName)) {
      Spreadsheet spreadsheet = sheetsSinkClient.createEmptySpreadsheet(spreadsheetName, sheetTitle);
      String spreadsheetId = spreadsheet.getSpreadsheetId();
      Integer sheetId = spreadsheet.getSheets().get(0).getProperties().getSheetId();
      int sheetRowCount = spreadsheet.getSheets().get(0).getProperties().getGridProperties().getRowCount();
      int sheetColumnCount = spreadsheet.getSheets().get(0).getProperties().getGridProperties().getColumnCount();

      sheetsSinkClient.moveSpreadsheetToDestinationFolder(spreadsheetId, spreadsheetName);

      availableFiles.put(spreadsheetName, spreadsheetId);

      availableSheets.put(spreadsheetName, new HashMap<>());
      availableSheets.get(spreadsheetName).put(sheetTitle, sheetId);

      sheetsContentShift.put(spreadsheetName, new ConcurrentHashMap<>());
      sheetsContentShift.get(spreadsheetName).put(sheetTitle, 0);

      sheetsRowCount.put(spreadsheetName, new ConcurrentHashMap<>());
      sheetsRowCount.get(spreadsheetName).put(sheetTitle, sheetRowCount);

      sheetsColumnCount.put(spreadsheetName, new ConcurrentHashMap<>());
      sheetsColumnCount.get(spreadsheetName).put(sheetTitle, sheetColumnCount);
    }
  }

  private void createSheetIfNeeded(String spreadsheetId, String spreadsheetName, String sheetTitle)
    throws ExecutionException, RetryException {
    if (!availableSheets.get(spreadsheetName).keySet().contains(sheetTitle)) {
      SheetProperties sheetProperties = sheetsSinkClient.createEmptySheet(spreadsheetId, spreadsheetName, sheetTitle);
      Integer sheetId = sheetProperties.getSheetId();
      int sheetRowCount = sheetProperties.getGridProperties().getRowCount();
      int sheetColumnCount = sheetProperties.getGridProperties().getColumnCount();

      availableSheets.get(spreadsheetName).put(sheetTitle, sheetId);
      sheetsContentShift.get(spreadsheetName).put(sheetTitle, 0);
      sheetsRowCount.get(spreadsheetName).put(sheetTitle, sheetRowCount);
      sheetsColumnCount.get(spreadsheetName).put(sheetTitle, sheetColumnCount);
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    stopSignal = true;
    // wait for scheduled task formers will be completed
    formerScheduledExecutorService.shutdown();
    formerScheduledExecutorService.awaitTermination(googleSheetsSinkConfig.getMaxFlushInterval() * 2,
      TimeUnit.SECONDS);

    // we should guarantee that at least one task former was called finally
    try {
      formerScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
      formerScheduledExecutorService.submit(new TasksFormer()).get();
    } catch (ExecutionException e) {
      throw new RuntimeException("Exception during final writing of records from record queue.", e);
    }

    // wait for worker threads completion
    messageProcessorService.shutdown();
    messageProcessorService.awaitTermination(flushTimeout, TimeUnit.SECONDS);
  }

  /**
   * Task that forms batches of requests and schedules tasks for writing of these batches.
   * With {@link TasksFormer#rerun} set to true a new tasks former will be scheduled after execution.
   */
  private class TasksFormer implements Callable {
    private final boolean rerun;

    TasksFormer(boolean rerun) {
      this.rerun = rerun;
    }

    TasksFormer() {
      this.rerun = false;
    }

    @Override
    public Object call() throws Exception {
      queueSemaphore.acquire();
      try {
        Map<String, Queue<FlatteredRowsRequest>> groupedRequests = getGroupedRequests(recordsQueue);
        List<RecordsBatch> sortedBatches = formAndSortBatches(groupedRequests);

        // send biggest groups to threads, else back to queue
        for (RecordsBatch recordsBatch : sortedBatches) {
          if (stopSignal) {
            if (threadsSemaphore.tryAcquire(flushTimeout, TimeUnit.SECONDS)) {
              // create new thread
              messageProcessorService.submit(
                new RecordsBatchProcessor(sheetsSinkClient, recordsBatch, threadsSemaphore));
            } else {
              throw new RuntimeException(
                String.format("Timeout '%d' exceeded when trying to schedule batch records for execution.",
                  flushTimeout));
            }
          } else {
            if (threadsSemaphore.tryAcquire()) {
              // create new thread
              messageProcessorService.submit(
                new RecordsBatchProcessor(sheetsSinkClient, recordsBatch, threadsSemaphore));
            } else {
              recordsBatch.getGroup().stream().forEach(r -> {
                recordsQueue.offer(r);
              });
            }
          }
        }
      } finally {
        queueSemaphore.release();
      }
      if (rerun) {
        if (!stopSignal) {
          formerScheduledExecutorService.schedule(new TasksFormer(rerun), maxFlushInterval, TimeUnit.SECONDS);
        }
      }
      return null;
    }

    private Map<String, Queue<FlatteredRowsRequest>> getGroupedRequests(Queue<FlatteredRowsRequest> recordsQueue) {
      Map<String, Queue<FlatteredRowsRequest>> groupedRequests = new HashMap<>();

      // pick up groups of elements
      FlatteredRowsRequest element;
      while ((element = recordsQueue.poll()) != null) {
        String spreadsheetName = element.getSpreadsheetName();

        if (!groupedRequests.containsKey(spreadsheetName)) {
          groupedRequests.put(spreadsheetName, new ConcurrentLinkedQueue<>());
        }
        groupedRequests.get(spreadsheetName).add(element);
      }
      return groupedRequests;
    }

    private List<RecordsBatch> formAndSortBatches(Map<String, Queue<FlatteredRowsRequest>> groupedRequests) {
      // form batches of requests
      List<RecordsBatch> recordsToSort = new ArrayList<>();
      for (Map.Entry<String, Queue<FlatteredRowsRequest>> spreadsheetEntry : groupedRequests.entrySet()) {
        String spreadsheetName = spreadsheetEntry.getKey();
        String spreadsheetId = availableFiles.get(spreadsheetName);
        Queue<FlatteredRowsRequest> queue = spreadsheetEntry.getValue();
        List<FlatteredRowsRequest> buffer = new ArrayList<>();

        while (!queue.isEmpty()) {
          buffer.add(queue.poll());
          if (buffer.size() == maxBufferSize) {
            recordsToSort.add(new RecordsBatch(buffer, spreadsheetName, spreadsheetId));
            buffer = new ArrayList<>();
          }
        }
        if (!buffer.isEmpty()) {
          recordsToSort.add(new RecordsBatch(buffer, spreadsheetName, spreadsheetId));
        }
      }

      // sort groups by size of records
      Collections.sort(recordsToSort);

      return recordsToSort;
    }
  }
}
