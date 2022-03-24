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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.sheets.source.utils.ColumnComplexSchemaInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Util class for building pipeline schema.
 */
public class SchemaBuilder {
  public static final String SCHEMA_ROOT_RECORD_NAME = "RowRecord";

  /**
   * Returns the instance of Schema.
   * @param config The GoogleSheetsSourceConfig
   * @param dataSchemaInfo The list of ColumnComplexSchemaInfo
   * @return The instance of Schema
   */
  public static Schema buildSchema(GoogleSheetsSourceConfig config,
                                   List<ColumnComplexSchemaInfo> dataSchemaInfo) {
    List<Schema.Field> generalFields = new ArrayList<>();
    if (!config.containsMacro(GoogleSheetsSourceConfig.ADD_NAME_FIELDS) && config.getAddNameFields()) {
      generalFields.add(Schema.Field.of(config.getSpreadsheetFieldName(), Schema.of(Schema.Type.STRING)));
      generalFields.add(Schema.Field.of(config.getSheetFieldName(), Schema.of(Schema.Type.STRING)));
    }

    for (ColumnComplexSchemaInfo schemaInfo : dataSchemaInfo) {
      if (schemaInfo.getSubColumns().isEmpty()) {
        generalFields.add(Schema.Field.of(schemaInfo.getHeaderTitle(), Schema.nullableOf(schemaInfo.getDataSchema())));
      } else {
        List<Schema.Field> recordFields = schemaInfo.getSubColumns().stream()
          .map(c -> Schema.Field.of(c.getHeaderTitle(), Schema.nullableOf(c.getDataSchema())))
          .collect(Collectors.toList());
        generalFields.add(Schema.Field.of(schemaInfo.getHeaderTitle(),
          Schema.nullableOf(Schema.recordOf(schemaInfo.getHeaderTitle(), recordFields))));
      }
    }

    if (!config.containsMacro(GoogleSheetsSourceConfig.EXTRACT_METADATA) &&
      !config.containsMacro(GoogleSheetsSourceConfig.METADATA_FIELD_NAME) && config.isExtractMetadata()) {
      generalFields.add(Schema.Field.of(config.getMetadataFieldName(),
          Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))));
    }

    return Schema.recordOf(SCHEMA_ROOT_RECORD_NAME, generalFields);
  }
}
