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

import com.google.api.services.sheets.v4.model.ExtendedValue;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.sheets.source.utils.ComplexSingleValueColumn;
import io.cdap.plugin.google.sheets.source.utils.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;

/**
 * Transforms {@link RowRecord} wrapper to {@link StructuredRecord} instance.
 */
public class SheetTransformer {
  private static final Logger LOG = LoggerFactory.getLogger(SheetTransformer.class);

  private static final LocalDate SHEETS_START_DATE = LocalDate.of(1899, 12, 30);
  private static final ZonedDateTime SHEETS_START_DATE_TIME =
    ZonedDateTime.of(1899, 12, 30, 0, 0, 0, 0, ZoneId.ofOffset("UTC", ZoneOffset.UTC));

  /**
   * Returns the StructuredRecord.
   *
   * @param rowRecord The rowRecord with
   * @param schema The schema with
   * @param extractMetadata The extractMetadata with
   * @param metadataRecordName The metadataRecordName with
   * @param addNames The addNames with
   * @param spreadsheetFieldName The spreadsheetFieldName with
   * @param sheetFieldName The sheetFieldName
   * @return The StructuredRecord
   */
  public static StructuredRecord transform(RowRecord rowRecord, Schema schema, boolean extractMetadata,
                                           String metadataRecordName,
                                           boolean addNames,
                                           String spreadsheetFieldName,
                                           String sheetFieldName) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      String name = field.getName();
      if (addNames && name.equals(sheetFieldName)) {
        builder.set(sheetFieldName, rowRecord.getSheetTitle());
      } else if (addNames && name.equals(spreadsheetFieldName)) {
        builder.set(spreadsheetFieldName, rowRecord.getSpreadsheetName());
      } else if (extractMetadata && name.equals(metadataRecordName)) {
        builder.set(metadataRecordName, rowRecord.getMetadata());
      } else {
        ComplexSingleValueColumn complexSingleValueColumn = rowRecord.getHeaderedCells().get(name);
        if (complexSingleValueColumn == null || (complexSingleValueColumn.getData() == null
          && complexSingleValueColumn.getSubColumns().isEmpty())) {
          builder.set(name, null);
        } else {
          processCellData(builder, field, complexSingleValueColumn);
        }
      }
    }
    return builder.build();
  }

  private static void processCellData(StructuredRecord.Builder builder, Schema.Field field,
                                      ComplexSingleValueColumn complexSingleValueColumn) {
    String fieldName = field.getName();
    Schema fieldSchema = field.getSchema();
    Schema.LogicalType fieldLogicalType = fieldSchema.getNonNullable().getLogicalType();
    Schema.Type fieldType = fieldSchema.getNonNullable().getType();

    if (Schema.LogicalType.DATE.equals(fieldLogicalType)) {
      ExtendedValue userEnteredValue = complexSingleValueColumn.getData().getUserEnteredValue();
      if (userEnteredValue != null) {
        builder.setDate(fieldName, getDateValue(userEnteredValue, fieldName));
      }

    } else if (Schema.LogicalType.TIMESTAMP_MILLIS.equals(fieldLogicalType)) {
      ExtendedValue userEnteredValue = complexSingleValueColumn.getData().getUserEnteredValue();
      if (userEnteredValue != null) {
        builder.setTimestamp(fieldName, getTimeStampValue(userEnteredValue, fieldName));
      }

    } else if (Schema.Type.LONG.equals(fieldType)) {
      ExtendedValue userEnteredValue = complexSingleValueColumn.getData().getUserEnteredValue();
      if (userEnteredValue != null) {
        builder.set(fieldName, getIntervalValue(userEnteredValue, fieldName));
      }

    } else if (Schema.Type.BOOLEAN.equals(fieldType)) {
      ExtendedValue effectiveValue = complexSingleValueColumn.getData().getEffectiveValue();
      if (effectiveValue != null) {
        builder.set(fieldName, effectiveValue.getBoolValue());
      }

    } else if (Schema.Type.STRING.equals(fieldType)) {
      builder.set(fieldName, complexSingleValueColumn.getData().getFormattedValue());

    } else if (Schema.Type.DOUBLE.equals(fieldType)) {
      ExtendedValue effectiveValue = complexSingleValueColumn.getData().getEffectiveValue();
      if (effectiveValue != null) {
        builder.set(fieldName, effectiveValue.getNumberValue());
      }
    } else if (Schema.Type.RECORD.equals(fieldType)) {
      builder.set(fieldName, processRecord(fieldSchema.getNonNullable(), complexSingleValueColumn));
    }
  }

  private static StructuredRecord processRecord(Schema fieldSchema, ComplexSingleValueColumn complexSingleValueColumn) {
    StructuredRecord.Builder builder = StructuredRecord.builder(fieldSchema);
    for (Schema.Field subField : fieldSchema.getFields()) {
      String subFieldName = subField.getName();
      ComplexSingleValueColumn complexSubColumn = complexSingleValueColumn.getSubColumns().get(subFieldName);
      if (complexSubColumn.getData() == null) {
        builder.set(subFieldName, null);
      } else {
        processCellData(builder, subField, complexSubColumn);
      }
    }
    return builder.build();
  }

  private static LocalDate getDateValue(ExtendedValue userEnteredValue, String fieldName) {
    Double dataValue = userEnteredValue.getNumberValue();
    if (dataValue == null) {
      LOG.warn(String.format("Field '%s' has no DATE value, '%s' instead", fieldName, userEnteredValue.toString()));
      return null;
    }
    return SHEETS_START_DATE.plusDays(dataValue.intValue());
  }

  private static ZonedDateTime getTimeStampValue(ExtendedValue userEnteredValue, String fieldName) {
    Double dataValue = userEnteredValue.getNumberValue();
    if (dataValue == null) {
      LOG.warn(String.format("Field '%s' has no DATE value, '%s' instead", fieldName, userEnteredValue.toString()));
      return null;
    }
    long dayMicros = ChronoField.MICRO_OF_DAY.range().getMaximum();
    return SHEETS_START_DATE_TIME.plus((long) (dataValue * dayMicros), ChronoUnit.MICROS);
  }

  private static Long getIntervalValue(ExtendedValue userEnteredValue, String fieldName) {
    Double dataValue = userEnteredValue.getNumberValue();
    if (dataValue == null) {
      LOG.warn(String.format("Field '%s' has no DATE value, '%s' instead", fieldName, userEnteredValue.toString()));
      return null;
    }
    long dayMicros = ChronoField.MICRO_OF_DAY.range().getMaximum();
    return (long) (dataValue * dayMicros / 1000);
  }
}
