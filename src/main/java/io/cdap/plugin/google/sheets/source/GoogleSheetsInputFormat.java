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
import com.google.api.services.drive.model.File;
import com.google.gson.reflect.TypeToken;
import io.cdap.plugin.google.common.GoogleDriveFilteringClient;
import io.cdap.plugin.google.common.utils.ExportedType;
import io.cdap.plugin.google.sheets.source.utils.MetadataKeyValueAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Input format class which generates splits for each query.
 */
public class GoogleSheetsInputFormat extends InputFormat {

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    Configuration conf = jobContext.getConfiguration();

    String headersJson = conf.get(GoogleSheetsInputFormatProvider.PROPERTY_HEADERS_JSON);
    GoogleSheetsSourceConfig googleSheetsSourceConfig =
      GoogleSheetsInputFormatProvider.extractPropertiesFromConfig(conf);

    Type headersType = new TypeToken<Map<Integer, Map<String, List<String>>>>() {
    }.getType();
    Map<Integer, Map<String, List<String>>> resolvedHeaders =
        GoogleSheetsInputFormatProvider.GSON.fromJson(headersJson, headersType);

    // get all sheets files according to filter
    GoogleDriveFilteringClient driveFilteringClient = new GoogleSheetsFilteringClient(googleSheetsSourceConfig);
    List<File> spreadsheetsFiles;
    try {
      spreadsheetsFiles = driveFilteringClient.getFilesSummary(Collections.singletonList(ExportedType.SPREADSHEETS));
    } catch (ExecutionException | RetryException e) {
      throw new RuntimeException("Failure of getting info about source spreadsheets.", e);
    }
    return getSplitsFromFiles(googleSheetsSourceConfig, spreadsheetsFiles, resolvedHeaders);
  }

  private List<InputSplit> getSplitsFromFiles(GoogleSheetsSourceConfig googleSheetsSourceConfig,
                                              List<File> files,
                                              Map<Integer, Map<String, List<String>>> resolvedHeaders) {
    List<InputSplit> splits = new ArrayList<>();
    String resolvedHeadersJson =
        GoogleSheetsInputFormatProvider.GSON.toJson(resolvedHeaders);

    List<MetadataKeyValueAddress> metadataCoordinates = googleSheetsSourceConfig.getMetadataCoordinates();
    String metadataCoordinatesJson =
        GoogleSheetsInputFormatProvider.GSON.toJson(metadataCoordinates);

    splits.addAll(files.stream().map(f ->
        new GoogleSheetsSplit(f.getId(), resolvedHeadersJson, metadataCoordinatesJson))
        .collect(Collectors.toList()));
    return splits;
  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    return new GoogleSheetsRecordReader();
  }
}
