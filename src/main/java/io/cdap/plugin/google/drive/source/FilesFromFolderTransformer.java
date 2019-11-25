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

package io.cdap.plugin.google.drive.source;

import com.google.api.client.json.GenericJson;
import com.google.api.client.util.DateTime;
import com.google.api.services.drive.model.File;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.drive.common.FileFromFolder;

import java.time.ZonedDateTime;

/**
 * Transforms {@link FileFromFolder} wrapper to {@link StructuredRecord} instance.
 */
public class FilesFromFolderTransformer {

  public static StructuredRecord transform(FileFromFolder fileFromFolder, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    File file = fileFromFolder.getFile();

    for (Schema.Field field : schema.getFields()) {
      String name = field.getName();
      if (name.equals(SchemaBuilder.BODY_FIELD_NAME)) {
        if (Schema.Type.STRING == field.getSchema().getType()) {
          builder.set(SchemaBuilder.BODY_FIELD_NAME, new String(fileFromFolder.getContent()));
        } else {
          builder.set(SchemaBuilder.BODY_FIELD_NAME, fileFromFolder.getContent());
        }
      } else if (name.equals(SchemaBuilder.OFFSET_FIELD_NAME)) {
        builder.set(SchemaBuilder.OFFSET_FIELD_NAME, fileFromFolder.getOffset());
      } else {
        if (name.equals(SchemaBuilder.IMAGE_METADATA_FIELD_NAME)) {
          File.ImageMediaMetadata imageMediaMetadata = file.getImageMediaMetadata();
          if (imageMediaMetadata != null) {
            builder.set(field.getName(),
                        parseSubSchema(field.getSchema().getNonNullable(), imageMediaMetadata));
          }
        } else if (name.equals(SchemaBuilder.VIDEO_METADATA_FIELD_NAME)) {
          File.VideoMediaMetadata videoMediaMetadata = file.getVideoMediaMetadata();
          if (videoMediaMetadata != null) {
            builder.set(field.getName(),
                        parseSubSchema(field.getSchema().getNonNullable(), videoMediaMetadata));
          }
        } else if (Schema.LogicalType.TIMESTAMP_MILLIS.equals(field.getSchema().getLogicalType())) {
          DateTime dateTime = (DateTime) file.get(name);
          builder.setTimestamp(name, ZonedDateTime.parse(dateTime.toStringRfc3339()));
        } else {
          builder.set(name, file.get(name));
        }
      }
    }
    return builder.build();
  }

  private static StructuredRecord parseSubSchema(Schema subSchema, GenericJson info) {
    StructuredRecord.Builder subBuilder = StructuredRecord.builder(subSchema);
    for (Schema.Field field : subSchema.getFields()) {
      Object value = info.get(field.getName());
      if (value instanceof GenericJson) {
        subBuilder.set(field.getName(),
                parseSubSchema(field.getSchema().getNonNullable(), (GenericJson) value));
      } else {
        subBuilder.set(field.getName(), info.get(field.getName()));
      }
    }
    return subBuilder.build();
  }
}
