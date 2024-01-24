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

package io.cdap.plugin.google.sheets.source;

import com.github.rholder.retry.RetryException;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.sheets.source.utils.MetadataKeyValueAddress;
import io.cdap.plugin.google.sheets.source.utils.MultipleRowRecord;
import io.cdap.plugin.google.sheets.source.utils.RowRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * RecordReader implementation, which reads {@link RowRecord} wrappers from Google Drive using
 * Google Drive API.
 * Reader supports buffered read. The size of the buffer is specified by
 * {@link GoogleSheetsSourceConfig#getReadBufferSize()}.
 */
public class GoogleSheetsRecordReader extends RecordReader<NullWritable, StructuredRecord> {

  private GoogleSheetsSourceClient googleSheetsSourceClient;
  private String fileId;
  private GoogleSheetsSourceConfig config;
  private Map<Integer, Map<String, List<String>>> resolvedHeaders;
  private List<MetadataKeyValueAddress> metadataCoordinates;

  private Queue<GroupedRowTask> rowTaskQueue = new ArrayDeque<>();
  private GroupedRowTask currentGroupedRowTask;
  private int currentRowIndex;
  private String currentSheetTitle;
  private Map<String, String> sheetMetadata = Collections.EMPTY_MAP;
  private int bufferSize;
  private MultipleRowRecord bufferedMultipleRowRecord = null;
  private int processedRowsCounter = 0;
  private int overallRowsNumber = 0;
  private Schema schema;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
    throws IOException {
    Configuration conf = taskAttemptContext.getConfiguration();
    String schemaJson = conf.get(GoogleSheetsInputFormatProvider.PROPERTY_CONFIG_SCHEMA);
    schema = Schema.parseJson(schemaJson);
    config =  GoogleSheetsInputFormatProvider.extractPropertiesFromConfig(conf);
    googleSheetsSourceClient = new GoogleSheetsSourceClient(config);

    GoogleSheetsSplit split = (GoogleSheetsSplit) inputSplit;
    this.fileId = split.getFileId();
    Type headersType = new TypeToken<Map<Integer, Map<String, List<String>>>>() {
    }.getType();
    this.resolvedHeaders = GoogleSheetsInputFormatProvider.GSON.fromJson(split.getHeaders(), headersType);
    Type metadataType = new TypeToken<List<MetadataKeyValueAddress>>() {
    }.getType();
    this.metadataCoordinates = GoogleSheetsInputFormatProvider.GSON.fromJson(split.getMetadates(), metadataType);

    bufferSize = config.getReadBufferSize();

    populateBufferedTasks();
  }

  private void populateBufferedTasks() {
    List<Sheet> sheetTitles;
    try {
      sheetTitles = getSheetTitles();
    } catch (ExecutionException | RetryException e) {
      throw new RuntimeException("Exception during sheet titles retrieving.", e);
    }
    sheetTitles.forEach(t -> {
      int firstDataRow = config.getActualFirstDataRow();
      // Each sheet can have different number of records so last row can be different sheet wise
      // and in case last data row is set to 0, It will fetch all records from the sheet
      int lastDataRow = config.getActualLastDataRow(t.getProperties().getGridProperties().getRowCount());
      int rowsNumber = lastDataRow - firstDataRow + 1;
      overallRowsNumber += rowsNumber;
      int counter = 0;
      String title = t.getProperties().getTitle();
      while (rowsNumber > bufferSize) {
        rowTaskQueue.add(
          new GroupedRowTask(title, counter * bufferSize + firstDataRow, bufferSize));
        counter++;
        rowsNumber -= bufferSize;
      }
      rowTaskQueue.add(new GroupedRowTask(title, counter * bufferSize + firstDataRow,
        rowsNumber));
    });
    currentRowIndex = -1;
    currentGroupedRowTask = null;
  }

  private List<Sheet> getSheetTitles() throws ExecutionException, RetryException {
    List<Sheet> sheetList = new ArrayList<>();
    switch (config.getSheetsToPull()) {
      case ALL:
        sheetList = googleSheetsSourceClient.getSheets(fileId);
        break;
      case NUMBERS:
        List<Integer> sheetIndexes = config.getSheetsIdentifiers().stream()
          .map(s -> Integer.parseInt(s)).collect(Collectors.toList());
        sheetList = googleSheetsSourceClient.getSheets(fileId).stream()
          .filter(s -> sheetIndexes.contains(s.getProperties().getIndex()))
          .collect(Collectors.toList());
        break;
      case TITLES:
        List<String> sheetTitles = config.getSheetsIdentifiers();
        sheetList = googleSheetsSourceClient.getSheets(fileId).stream()
          .filter(s -> sheetTitles.contains(s.getProperties().getTitle()))
          .collect(Collectors.toList());
        break;
    }
    return sheetList;
  }

  @Override
  public boolean nextKeyValue() {
    if (currentGroupedRowTask != null) {
      if (currentRowIndex >= 0 && currentRowIndex < currentGroupedRowTask.length - 1) {
        currentRowIndex++;
        return true;
      }
    }
    currentGroupedRowTask = rowTaskQueue.poll();
    currentRowIndex = 0;
    return currentGroupedRowTask != null;
  }

  @Override
  public NullWritable getCurrentKey() {
    return null;
  }

  @Override
  public StructuredRecord getCurrentValue() throws IOException {
    boolean isNewGroupTask = currentRowIndex == 0;
    boolean isNewSheet = !currentGroupedRowTask.getSheetTitle().equals(currentSheetTitle);
    currentSheetTitle = currentGroupedRowTask.getSheetTitle();
    try {
      if (isNewSheet || isNewGroupTask) {

        bufferedMultipleRowRecord = googleSheetsSourceClient.getContent(fileId, currentSheetTitle,
          currentGroupedRowTask.getRowNumber(), currentGroupedRowTask.getLength(), resolvedHeaders,
          isNewSheet ? metadataCoordinates : null);

        if (isNewSheet) {
          sheetMetadata = bufferedMultipleRowRecord.getMetadata();
        } else {
          bufferedMultipleRowRecord.setMetadata(sheetMetadata);
        }
      }
    } catch (ExecutionException | RetryException e) {
      throw new RuntimeException(
        String.format("Exception on retrieving sheet content, file id: '%s', sheet title: '%s', " +
            "start row number: '%d', length of the group: '%d'",
          fileId, currentSheetTitle, currentGroupedRowTask.getRowNumber(), currentGroupedRowTask.getLength()), e);
    }

    processedRowsCounter++;
    RowRecord rowRecord = bufferedMultipleRowRecord.getRowRecord(currentRowIndex);

    // skip empty rows if needed
    if (!config.isSkipEmptyData() || !rowRecord.isEmptyData()) {
      return SheetTransformer.transform(rowRecord, schema, config.isExtractMetadata(),
        config.getMetadataFieldName(), config.getAddNameFields(),
        config.getSpreadsheetFieldName(), config.getSheetFieldName());
    }

    return null;
  }

  @Override
  public float getProgress() {
    return (float) processedRowsCounter / (float) overallRowsNumber;
  }

  @Override
  public void close() {

  }

  /**
   * Wrapper for group of records.
   */
  private class GroupedRowTask {
    final String sheetTitle;
    final int rowNumber;
    final int length;

    GroupedRowTask(String sheetTitle, int rowNumber, int length) {
      this.sheetTitle = sheetTitle;
      this.rowNumber = rowNumber;
      this.length = length;
    }

    public String getSheetTitle() {
      return sheetTitle;
    }

    public int getRowNumber() {
      return rowNumber;
    }

    public int getLength() {
      return length;
    }
  }
}
