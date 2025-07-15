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

package io.cdap.plugin.google.drive.source;

import com.github.rholder.retry.RetryException;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.google.drive.common.FileFromFolder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * RecordReader implementation, which reads {@link StructuredRecord} wrappers from Google Drive using
 * Google Drive API.
 */
public class GoogleDriveRecordReader extends RecordReader<NullWritable, StructuredRecord> {

  private GoogleDriveSourceClient googleDriveSourceClient;
  private GoogleDriveSourceConfig googleDriveSourceConfig;
  private String fileId;
  private long bytesFrom;
  private long bytesTo;
  private boolean isPartitioned;
  private boolean isFileProcessed;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    Configuration conf = taskAttemptContext.getConfiguration();
    googleDriveSourceConfig = GoogleDriveInputFormatProvider.extractPropertiesFromConfig(conf);
    googleDriveSourceClient = new GoogleDriveSourceClient(googleDriveSourceConfig);

    GoogleDriveSplit split = (GoogleDriveSplit) inputSplit;
    this.fileId = split.getFileId();
    this.bytesFrom = split.getBytesFrom();
    this.bytesTo = split.getBytesTo();
    this.isPartitioned = split.isPartitioned();
    this.isFileProcessed = false;
  }

  @Override
  public boolean nextKeyValue() {
    return !isFileProcessed;
  }

  @Override
  public NullWritable getCurrentKey() {
    return NullWritable.get();
  }

  @Override
  public StructuredRecord getCurrentValue() throws IOException {
    // read file and content
    isFileProcessed = true;
    try {
      if (isPartitioned) {
        return FilesFromFolderTransformer.transform(
            googleDriveSourceClient.getFilePartition(fileId, bytesFrom, bytesTo), googleDriveSourceConfig.getSchema());
      } else {
        return FilesFromFolderTransformer.transform(googleDriveSourceClient.getFile(fileId),
            googleDriveSourceConfig.getSchema());
      }
    } catch (ExecutionException | RetryException e) {
      throw new RuntimeException("Exception during file or file part reading.", e);
    }
  }

  @Override
  public float getProgress() {
    // progress is unknown
    return 0.0f;
  }

  @Override
  public void close() {
    // no-op
  }
}
