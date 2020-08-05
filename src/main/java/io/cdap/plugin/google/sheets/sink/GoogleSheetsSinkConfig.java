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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.google.common.GoogleInputSchemaFieldsUsageConfig;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Configurations for Google Sheets Batch Sink plugin.
 */
public class GoogleSheetsSinkConfig extends GoogleInputSchemaFieldsUsageConfig {
  public static final String SHEET_NAME_FIELD_NAME = "sheetName";
  public static final String SPREADSHEET_NAME_FIELD_NAME = "spreadsheetName";
  public static final String SCHEMA_SPREAD_SHEET_NAME_FIELD_NAME = "schemaSpreadsheetNameFieldName";
  public static final String SCHEMA_SHEET_NAME_FIELD_NAME = "schemaSheetNameFieldName";
  public static final String WRITE_SCHEMA_FIELD_NAME = "writeSchema";
  public static final String MERGE_DATA_CELLS_FIELD_NAME = "mergeDataCells";
  public static final String MIN_PAGE_EXTENSION_PAGE_FIELD_NAME = "minPageExtensionSize";
  public static final String THREADS_NUMBER_FIELD_NAME = "threadsNumber";
  public static final String MAX_BUFFER_SIZE_FIELD_NAME = "maxBufferSize";
  public static final String RECORDS_QUEUE_LENGTH_FIELD_NAME = "recordsQueueLength";
  public static final String MAX_FLUSH_INTERVAL_FIELD_NAME = "maxFlushInterval";
  public static final String FLUSH_EXECUTION_TIMEOUT_FIELD_NAME = "flushExecutionTimeout";
  public static final String SKIP_NAME_FIELDS_FIELD_NAME = "skipNameFields";

  public static final String TOP_LEVEL_SCHEMA_MESSAGE =
    "Field '%s' has unsupported schema type '%s' or logical type '%s'.";
  public static final String TOP_LEVEL_SCHEMA_CORRECTIVE_MESSAGE =
    "Supported top level types: '%s' and logical types: '%s'.";
  public static final String ARRAY_COMPONENTS_SCHEMA_MESSAGE =
    "Array field '%s' has unsupported components schema type '%s' or logical type '%s'.";
  public static final String ARRAY_COMPONENTS_SCHEMA_CORRECTIVE_MESSAGE =
    "Supported array types: '%s' and logical types: '%s'.";
  public static final String RECORD_FIELD_SCHEMA_MESSAGE =
    "Record '%s' has field '%s' with unsupported schema type '%s' or logical type '%s'";
  public static final String RECORD_FIELD_SCHEMA_CORRECTIVE_MESSAGE =
    "Supported types for record fields: '%s'. Supported logical types for record fields: '%s'.";

  public static final List<Schema.LogicalType> SUPPORTED_LOGICAL_TYPES = Arrays.asList(Schema.LogicalType.DATE,
    Schema.LogicalType.TIME_MILLIS,
    Schema.LogicalType.TIME_MICROS,
    Schema.LogicalType.TIMESTAMP_MILLIS,
    Schema.LogicalType.TIMESTAMP_MICROS,
    Schema.LogicalType.DECIMAL);

  public static final List<Schema.Type> SUPPORTED_TYPES = Arrays.asList(Schema.Type.STRING,
    Schema.Type.LONG,
    Schema.Type.INT,
    Schema.Type.DOUBLE,
    Schema.Type.FLOAT,
    Schema.Type.BYTES,
    Schema.Type.BOOLEAN,
    Schema.Type.NULL,
    Schema.Type.ARRAY,
    Schema.Type.RECORD);

  public static final List<Schema.Type> SUPPORTED_NESTED_TYPES = Arrays.asList(Schema.Type.STRING,
    Schema.Type.LONG,
    Schema.Type.INT,
    Schema.Type.DOUBLE,
    Schema.Type.FLOAT,
    Schema.Type.BYTES,
    Schema.Type.BOOLEAN,
    Schema.Type.NULL);

  @Name(SHEET_NAME_FIELD_NAME)
  @Description("Default sheet title. Is used when user doesn't specify schema field with sheet title.")
  @Macro
  private String sheetName;

  @Name(SPREADSHEET_NAME_FIELD_NAME)
  @Description("Default spreadsheet file name. Is used if user doesn't specify schema field with spreadsheet name.")
  @Macro
  private String spreadsheetName;

  @Nullable
  @Name(SCHEMA_SPREAD_SHEET_NAME_FIELD_NAME)
  @Description("Name of the schema field (should be STRING type) which will be used as name of file. \n" +
    "Is optional. In the case it is not set Google API will use the value of **Default Spreadsheet name** property.")
  @Macro
  private String schemaSpreadsheetNameFieldName;

  @Nullable
  @Name(SCHEMA_SHEET_NAME_FIELD_NAME)
  @Description("Name of the schema field (should be STRING type) which will be used as sheet title. \n" +
    "Is optional. In the case it is not set Google API will use the value of **Default sheet name** property.")
  @Macro
  private String schemaSheetNameFieldName;

  @Name(WRITE_SCHEMA_FIELD_NAME)
  @Description("Toggle that defines if the sink writes out the input schema as first row of an output sheet.")
  @Macro
  private boolean writeSchema;

  @Name(MERGE_DATA_CELLS_FIELD_NAME)
  @Description("Toggle that defines if the sink merges data cells created as result of input arrays flattering.")
  @Macro
  private boolean mergeDataCells;

  @Name(MIN_PAGE_EXTENSION_PAGE_FIELD_NAME)
  @Description("Minimal size of sheet extension when default sheet size is exceeded.")
  @Macro
  private int minPageExtensionSize;

  @Name(THREADS_NUMBER_FIELD_NAME)
  @Description("Number of threads which send batched API requests. " +
    "The greater value allows to process records quickly, but requires extended Google Sheets API quota.")
  @Macro
  private int threadsNumber;

  @Name(MAX_BUFFER_SIZE_FIELD_NAME)
  @Description("Maximal size in records of the batch API request. " +
    "The greater value allows to reduce the number of API requests, but causes increase of their size.")
  @Macro
  private int maxBufferSize;

  @Name(RECORDS_QUEUE_LENGTH_FIELD_NAME)
  @Description("Size of the queue used to receive records and for onwards grouping of them to batched API requests. " +
    "With the greater value it is more likely that the sink will group received records in the batches " +
    "of maximal size. Also greater value leads to more memory consumption.")
  @Macro
  private int recordsQueueLength;

  @Name(MAX_FLUSH_INTERVAL_FIELD_NAME)
  @Description("Number of seconds between the sink tries to get batched requests from the records queue " +
    "and send them to threads for sending to Sheets API.")
  @Macro
  private int maxFlushInterval;

  @Name(FLUSH_EXECUTION_TIMEOUT_FIELD_NAME)
  @Description("Timeout for single thread to process the batched API request. " +
    "Be careful, the number of retries and maximal retry time also should be taken into account.")
  @Macro
  private int flushExecutionTimeout;

  @Name(SKIP_NAME_FIELDS_FIELD_NAME)
  @Description("Toggle that defines if the sink skips spreadsheet/sheet name fields during structure record " +
    "transforming.")
  @Macro
  private boolean skipNameFields;

  /**
   * Validate that the given schema is compatible with the given extension.
   * @param collector failure collector with
   * @param schema the schema to check compatibility
   */
  public void validate(FailureCollector collector, Schema schema) {
    super.validate(collector);

    // validate spreadsheet name field is in schema and has valid format
    validateSchemaField(collector, schema, SCHEMA_SPREAD_SHEET_NAME_FIELD_NAME, schemaSpreadsheetNameFieldName,
      "Spreadsheet name field", Schema.Type.STRING);

    // validate sheet field is in schema and has valid format
    validateSchemaField(collector, schema, SCHEMA_SHEET_NAME_FIELD_NAME, schemaSheetNameFieldName,
      "Sheet name field", Schema.Type.STRING);

    // validate schema
    validateSchema(collector, schema);
  }

  private void validateSchema(FailureCollector collector, Schema schema) {
    for (Schema.Field field : schema.getFields()) {
      Schema fieldSchema = field.getSchema();
      checkSupportedSchemas(collector, fieldSchema, SUPPORTED_LOGICAL_TYPES, SUPPORTED_TYPES,
        String.format(TOP_LEVEL_SCHEMA_MESSAGE, field.getName(), fieldSchema.getType(),
          fieldSchema.getLogicalType()),
        String.format(TOP_LEVEL_SCHEMA_CORRECTIVE_MESSAGE, SUPPORTED_TYPES.toString(),
          SUPPORTED_LOGICAL_TYPES.toString()));
      // for array and record check that they don't have nested complex structures
      if (Schema.Type.ARRAY.equals(fieldSchema.getType())) {
        Schema componentSchema = fieldSchema.getComponentSchema();
        checkSupportedSchemas(collector, componentSchema, SUPPORTED_LOGICAL_TYPES, SUPPORTED_NESTED_TYPES,
          String.format(ARRAY_COMPONENTS_SCHEMA_MESSAGE, field.getName(),
            componentSchema.getType(), componentSchema.getLogicalType()),
          String.format(ARRAY_COMPONENTS_SCHEMA_CORRECTIVE_MESSAGE, SUPPORTED_NESTED_TYPES.toString(),
          SUPPORTED_LOGICAL_TYPES.toString()));
      }
      if (Schema.Type.RECORD.equals(fieldSchema.getType())) {
        for (Schema.Field nestedField : fieldSchema.getFields()) {
          Schema nestedComponentSchema = nestedField.getSchema();
          checkSupportedSchemas(collector, nestedComponentSchema, SUPPORTED_LOGICAL_TYPES, SUPPORTED_NESTED_TYPES,
            String.format(RECORD_FIELD_SCHEMA_MESSAGE, field.getName(), nestedField.getName(),
              nestedComponentSchema.getType(), nestedComponentSchema.getLogicalType()),
            String.format(String.format(RECORD_FIELD_SCHEMA_CORRECTIVE_MESSAGE,
              SUPPORTED_NESTED_TYPES.toString(), SUPPORTED_LOGICAL_TYPES.toString())));
        }
      }
    }
  }

  private void checkSupportedSchemas(FailureCollector collector, Schema fieldSchema,
                                     List<Schema.LogicalType> allowedLogicalTypes, List<Schema.Type> allowedTypes,
                                     String message, String correctiveAction) {
    Schema.LogicalType nonNullableLogicalType = fieldSchema.isNullable() ?
      fieldSchema.getNonNullable().getLogicalType() :
      fieldSchema.getLogicalType();
    Schema.Type nonNullableType = fieldSchema.isNullable() ?
      fieldSchema.getNonNullable().getType() :
      fieldSchema.getType();
    if (!allowedLogicalTypes.contains(nonNullableLogicalType) && !allowedTypes.contains(nonNullableType)) {
      collector.addFailure(message, correctiveAction);
    }
  }

  public String getSheetName() {
    return sheetName;
  }

  public String getSpreadsheetName() {
    return spreadsheetName;
  }

  @Nullable
  public String getSchemaSpreadsheetNameFieldName() {
    return schemaSpreadsheetNameFieldName;
  }

  @Nullable
  public String getSchemaSheetNameFieldName() {
    return schemaSheetNameFieldName;
  }

  public boolean isWriteSchema() {
    return writeSchema;
  }

  public boolean isMergeDataCells() {
    return mergeDataCells;
  }

  public int getMinPageExtensionSize() {
    return minPageExtensionSize;
  }

  public int getThreadsNumber() {
    return threadsNumber;
  }

  public int getMaxBufferSize() {
    return maxBufferSize;
  }

  public int getRecordsQueueLength() {
    return recordsQueueLength;
  }

  public int getMaxFlushInterval() {
    return maxFlushInterval;
  }

  public int getFlushExecutionTimeout() {
    return flushExecutionTimeout;
  }

  public boolean isSkipNameFields() {
    return skipNameFields;
  }
}
