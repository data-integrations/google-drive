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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A split used for mapreduce.
 */
public class GoogleSheetsSplit extends InputSplit implements Writable {
  private String fileId;
  private String headers;
  private String metadates;

  @SuppressWarnings("unused")
  public GoogleSheetsSplit() {
    // For serialization
  }

  /**
   * Constructor for GoogleSheetsSplit object.
   * @param fileId the file id
   * @param headers the headers
   * @param metadates the meta dates
   */
  public GoogleSheetsSplit(String fileId, String headers, String metadates) {
    this.fileId = fileId;
    this.headers = headers;
    this.metadates = metadates;
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    fileId = dataInput.readUTF();
    headers = dataInput.readUTF();
    metadates = dataInput.readUTF();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(fileId);
    dataOutput.writeUTF(headers);
    dataOutput.writeUTF(metadates);
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String[] getLocations() {
    return new String[0];
  }

  public String getFileId() {
    return fileId;
  }

  public String getHeaders() {
    return headers;
  }

  public String getMetadates() {
    return metadates;
  }
}
