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
import com.google.api.services.drive.model.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Input format class which generates splits for each query.
 */
public class GoogleDriveInputFormat extends InputFormat {

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    Configuration conf = jobContext.getConfiguration();
    GoogleDriveSourceConfig googleDriveSourceConfig =
      GoogleDriveInputFormatProvider.extractPropertiesFromConfig(conf);

    GoogleDriveSourceClient client = new GoogleDriveSourceClient(googleDriveSourceConfig);
    Long maxBodySize = googleDriveSourceConfig.getMaxPartitionSize();

    try {
      return getSplitsFromFiles(client.getFilesSummary(), maxBodySize);
    } catch (ExecutionException | RetryException e) {
      throw new RuntimeException("Failed to prepare splits.", e);
    }
  }

  private List<InputSplit> getSplitsFromFiles(List<File> files, Long maxBodySize) {
    List<InputSplit> splits = new ArrayList<>();
    for (File file : files) {
      Long fileSize = file.getSize();
      // fileSize == null for files in Google formats
      if (maxBodySize == 0L || fileSize == null || fileSize <= maxBodySize) {
        splits.add(getSplitWithUnlimitedPartitionSize(file.getId()));
      } else {
        long currentPoint = 0L;
        while (currentPoint < fileSize) {
          splits.add(new GoogleDriveSplit(file.getId(), currentPoint,
                                          Math.min(fileSize, currentPoint + maxBodySize) - 1));
          currentPoint += maxBodySize;
        }
      }
    }
    return splits;
  }

  private GoogleDriveSplit getSplitWithUnlimitedPartitionSize(String fileId) {
    return new GoogleDriveSplit(fileId);
  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    return new GoogleDriveRecordReader();
  }
}
