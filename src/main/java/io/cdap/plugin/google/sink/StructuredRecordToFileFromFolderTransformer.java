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

package io.cdap.plugin.google.sink;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.common.FileFromFolder;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * Transforms a {@link io.cdap.cdap.api.data.format.StructuredRecord}
 * to a {@link io.cdap.plugin.google.common.FileFromFolder}
 */
public class StructuredRecordToFileFromFolderTransformer {
  private static final Integer RANDON_FILE_NAME_LENGTH = 16;
  private final String bodyFieldName;
  private final String nameFieldName;

  public StructuredRecordToFileFromFolderTransformer(String bodyFieldName, String nameFieldName) {
    this.bodyFieldName = bodyFieldName;
    this.nameFieldName = nameFieldName;
  }

  public FileFromFolder transform(StructuredRecord input) {
    byte[] content = new byte[]{};
    String name;

    Schema schema = input.getSchema();
    if (schema.getField(bodyFieldName) != null) {
      content = input.get(bodyFieldName);
    }
    if (schema.getField(nameFieldName) != null) {
      name = input.get(nameFieldName);
    } else {
      name = generateRandomName();
    }
    FileFromFolder fileFromFolder = new FileFromFolder(content, name);
    return fileFromFolder;
  }

  public String generateRandomName() {
    return RandomStringUtils.randomAlphanumeric(RANDON_FILE_NAME_LENGTH);
  }
}
