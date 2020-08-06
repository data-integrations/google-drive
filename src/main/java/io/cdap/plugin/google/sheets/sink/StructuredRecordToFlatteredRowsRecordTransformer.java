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

import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.CellFormat;
import com.google.api.services.sheets.v4.model.ExtendedValue;
import com.google.api.services.sheets.v4.model.GridRange;
import com.google.api.services.sheets.v4.model.NumberFormat;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.sheets.sink.utils.ComplexHeader;
import io.cdap.plugin.google.sheets.sink.utils.FlatteredRowsRecord;
import org.apache.commons.collections.CollectionUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Transforms a {@link StructuredRecord} to a {@link FlatteredRowsRecord}.
 */
public class StructuredRecordToFlatteredRowsRecordTransformer {
  public static final LocalDate SHEETS_START_DATE = LocalDate.of(1899, 12, 30);
  public static final ZoneId UTC_ZONE_ID = ZoneId.ofOffset("UTC", ZoneOffset.UTC);
  public static final String SHEETS_CELL_DATE_TYPE = "DATE";
  public static final String SHEETS_CELL_TIME_TYPE = "TIME";
  public static final String SHEETS_CELL_DATE_TIME_TYPE = "DATE_TIME";

  private final String spreadsheetNameFieldName;
  private final String sheetNameFieldName;
  private final String spreadsheetName;
  private final String sheetName;
  private final boolean skipNameFields;

  /**
   * Constructor for StructuredRecordToFlatteredRowsRecordTransformer object.
   * @param spreadsheetNameFieldName The spread sheet Name Field Name
   * @param sheetNameFieldName The sheet Name Field Name
   * @param spreadsheetName The spread sheet Name
   * @param sheetName The sheet Name
   * @param skipNameFields The skip Name Fields
   */
  public StructuredRecordToFlatteredRowsRecordTransformer(String spreadsheetNameFieldName,
                                                          String sheetNameFieldName,
                                                          String spreadsheetName,
                                                          String sheetName,
                                                          boolean skipNameFields) {
    this.spreadsheetNameFieldName = spreadsheetNameFieldName;
    this.sheetNameFieldName = sheetNameFieldName;
    this.spreadsheetName = spreadsheetName;
    this.sheetName = sheetName;
    this.skipNameFields = skipNameFields;
  }

  /**
   * Returns selected StructuredRecord.
   * @param input The StructuredRecord
   * @return the instance of StructuredRecord
   */
  public FlatteredRowsRecord transform(StructuredRecord input) {
    List<List<CellData>> data = new ArrayList<>();
    List<GridRange> mergeRanges = new ArrayList<>();
    ComplexHeader header = new ComplexHeader(null);
    String spreadsheetName = null;
    String sheetName = null;

    data.add(new ArrayList<>());
    Schema schema = input.getSchema();
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      if (fieldName.equals(spreadsheetNameFieldName)) {
        spreadsheetName = input.get(spreadsheetNameFieldName);
        if (skipNameFields) {
          continue;
        }
      } else if (fieldName.equals(sheetNameFieldName)) {
        sheetName = input.get(sheetNameFieldName);
        if (skipNameFields) {
          continue;
        }
      }
      processField(field, input, data, header, mergeRanges, true);
    }
    if (spreadsheetName == null) {
      spreadsheetName = this.spreadsheetName;
    }

    if (sheetName == null) {
      sheetName = this.sheetName;
    }

    FlatteredRowsRecord flatteredRowsRecord = new FlatteredRowsRecord(spreadsheetName, sheetName, header,
      data, mergeRanges);
    return flatteredRowsRecord;
  }

  /**
   * Method that parse field of input structure record and add it to data list by appropriate way depends
   * on field's schema. Also merge ranges and headers are extended.
   *
   * @param field field to process.
   * @param input input structure record.
   * @param data list of rows to extend with parsed data.
   * @param header result complex header.
   * @param mergeRanges merge ranges, that units cells resulting from arrays flattering.
   * @param isComplexTypeSupported variable that defines are complex data formats are supported
   */
  private void processField(Schema.Field field, StructuredRecord input, List<List<CellData>> data,
                            ComplexHeader header, List<GridRange> mergeRanges, boolean isComplexTypeSupported) {
    String fieldName = field.getName();
    Schema fieldSchema = field.getSchema();
    ComplexHeader subHeader = new ComplexHeader(fieldName);

    Schema.LogicalType fieldLogicalType = getFieldLogicalType(fieldSchema);
    Schema.Type fieldType = getFieldType(fieldSchema);

    Object value = input == null ? null : input.get(fieldName);

    CellData cellData;
    if (fieldLogicalType != null) {
      cellData = processDateTimeValue(fieldLogicalType, fieldSchema, value);
      addDataValue(cellData, data, mergeRanges);
    } else if (isSimpleType(fieldType)) {
      cellData = processSimpleTypes(fieldType, value);
      addDataValue(cellData, data, mergeRanges);
    } else if (isComplexType(fieldType)) {
      if (isComplexTypeSupported) {
        processComplexTypes(fieldType, fieldName, input, data, subHeader, mergeRanges);
      } else {
        throw new IllegalStateException("Nested arrays/records are not supported.");
      }
    } else {
      throw new IllegalStateException(String.format("Data type '%s' is not supported.", fieldType));
    }
    header.addHeader(subHeader);
  }

  /**
   * Method that adds cellData to the end of each data row. Also it creates merge range for whole column from data rows.
   *
   * @param cellData cell data to add.
   * @param data data rows to extend.
   * @param mergeRanges merge ranges to extend.
   */
  private void addDataValue(CellData cellData, List<List<CellData>> data, List<GridRange> mergeRanges) {
    data.forEach(r -> r.add(cellData));
    mergeRanges.add(new GridRange().setStartRowIndex(0).setEndRowIndex(data.size())
      .setStartColumnIndex(data.get(0).size() - 1).setEndColumnIndex(data.get(0).size()));
  }

  private CellData processDateTimeValue(Schema.LogicalType fieldLogicalType, Schema fieldSchema, Object value) {
    CellData cellData = new CellData();
    ExtendedValue userEnteredValue = new ExtendedValue();
    CellFormat userEnteredFormat = new CellFormat();
    NumberFormat dateFormat = new NumberFormat();
    switch (fieldLogicalType) {
      case DATE:
        if (value != null) {
          LocalDate date = LocalDate.ofEpochDay((Integer) value);
          userEnteredValue.setNumberValue(toSheetsDate(date));
        }
        dateFormat.setType(SHEETS_CELL_DATE_TYPE);
        break;
      case TIMESTAMP_MILLIS:
        if (value != null) {
          ZonedDateTime dateTime = getZonedDateTime((long) value, TimeUnit.MILLISECONDS,
            ZoneId.ofOffset("UTC", ZoneOffset.UTC));
          userEnteredValue.setNumberValue(toSheetsDateTime(dateTime));
        }
        dateFormat.setType(SHEETS_CELL_DATE_TIME_TYPE);
        break;
      case TIMESTAMP_MICROS:
        if (value != null) {
          ZonedDateTime dateTime = getZonedDateTime((long) value, TimeUnit.MICROSECONDS,
            ZoneId.ofOffset("UTC", ZoneOffset.UTC));
          userEnteredValue.setNumberValue(toSheetsDateTime(dateTime));
        }
        dateFormat.setType(SHEETS_CELL_DATE_TIME_TYPE);
        break;
      case TIME_MILLIS:
        if (value != null) {
          LocalTime time = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos((Integer) value));
          userEnteredValue.setNumberValue(toSheetsTime(time));
        }
        dateFormat.setType(SHEETS_CELL_TIME_TYPE);
        break;
      case TIME_MICROS:
        if (value != null) {
          LocalTime time = LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos((Long) value));
          userEnteredValue.setNumberValue(toSheetsTime(time));
        }
        dateFormat.setType(SHEETS_CELL_TIME_TYPE);
        break;
      case DECIMAL:
        if (value != null) {
          int scale = fieldSchema.getScale();
          BigDecimal parsedValue;
          if (value instanceof ByteBuffer) {
            parsedValue = new BigDecimal(new BigInteger(Bytes.toBytes((ByteBuffer) value)), scale);
          } else {
            parsedValue = new BigDecimal(new BigInteger((byte[]) value), scale);
          }
          userEnteredValue.setNumberValue(parsedValue.doubleValue());
        }
        dateFormat.setType(SHEETS_CELL_TIME_TYPE);
        break;
      default:
        throw new IllegalStateException(String.format("Logical data type '%s' is not supported.",
          fieldLogicalType.toString()));
    }
    userEnteredFormat.setNumberFormat(dateFormat);
    cellData.setUserEnteredValue(userEnteredValue);
    cellData.setUserEnteredFormat(userEnteredFormat);
    return cellData;
  }

  private CellData processSimpleTypes(Schema.Type fieldType, Object value) {
    if (value == null) {
      return new CellData();
    }
    CellData cellData = new CellData();
    ExtendedValue userEnteredValue = new ExtendedValue();
    CellFormat userEnteredFormat = new CellFormat();

    switch (fieldType) {
      case STRING:
        userEnteredValue.setStringValue((String) value);
        break;
      case BYTES:
        userEnteredValue.setStringValue(new String((byte[]) value));
        break;
      case BOOLEAN:
        userEnteredValue.setBoolValue((Boolean) value);
        break;
      case LONG:
        userEnteredValue.setNumberValue((double) (Long) value);
        break;
      case INT:
        userEnteredValue.setNumberValue((double) (Integer) value);
        break;
      case DOUBLE:
        userEnteredValue.setNumberValue((Double) value);
        break;
      case FLOAT:
        userEnteredValue.setNumberValue((double) (Long) value);
        break;
      case NULL:
        // do nothing
        break;
      default:
        throw new IllegalStateException(String.format("Simple data type '%s' is not supported.", fieldType.toString()));
    }
    cellData.setUserEnteredValue(userEnteredValue);
    cellData.setUserEnteredFormat(userEnteredFormat);
    return cellData;
  }

  /**
   * Method that processes complex data formats. Arrays and records are supported.
   * For arrays all present data rows are duplicated for each array element.
   * All existing data merge ranges also are modified accordingly.
   * For records each record field is interpreted as separate column in data rows.
   * Nested complex data types are not supported.
   *
   * @param fieldType field type.
   * @param fieldName field name.
   * @param input input structure record.
   * @param data data rows to extend.
   * @param header complex header to extend.
   * @param mergeRanges data merge ranges to extend.
   */
  private void processComplexTypes(Schema.Type fieldType, String fieldName, StructuredRecord input,
                                   List<List<CellData>> data, ComplexHeader header, List<GridRange> mergeRanges) {
    switch (fieldType) {
      case ARRAY:
        List<Object> arrayData = input.get(fieldName);
        if (CollectionUtils.isEmpty(arrayData)) {
          arrayData = Collections.singletonList(null);
        }
        List<CellData>[] extendedData = new ArrayList[data.size() * arrayData.size()];

        Schema componentFieldSchema = getNonNullableSchema(input.getSchema().getField(fieldName).getSchema())
          .getComponentSchema();
        Schema.LogicalType componentFieldLogicalType = getFieldLogicalType(componentFieldSchema);
        Schema.Type componentFieldType = getFieldType(componentFieldSchema);

        // update merges
        for (GridRange range : mergeRanges) {
          Integer newStartRowIndex = range.getStartRowIndex() * arrayData.size();
          Integer newEndRowIndex = newStartRowIndex +
            (range.getEndRowIndex() - range.getStartRowIndex()) * arrayData.size();
          range.setStartRowIndex(newStartRowIndex).setEndRowIndex(newEndRowIndex);
        }

        // flatter the array
        for (int i = 0; i < arrayData.size(); i++) {
          CellData nestedData;
          if (componentFieldLogicalType != null) {
            nestedData = processDateTimeValue(componentFieldLogicalType, componentFieldSchema, arrayData.get(i));
          } else if (isSimpleType(componentFieldType)) {
            nestedData = processSimpleTypes(componentFieldType, arrayData.get(i));
          } else {
            throw new IllegalStateException("Nested complex data formats are not supported.");
          }
          for (int j = 0; j < data.size(); j++) {
            List<CellData> flattenRow = copyRow(data.get(j));
            flattenRow.add(nestedData);
            mergeRanges.add(new GridRange().setStartRowIndex(i + arrayData.size() * j)
              .setEndRowIndex(i + arrayData.size() * j + 1)
              .setStartColumnIndex(flattenRow.size() - 1)
              .setEndColumnIndex(flattenRow.size()));
            extendedData[i + arrayData.size() * j] = flattenRow;
          }
        }
        data.clear();
        data.addAll(Arrays.asList(extendedData));
        break;
      case RECORD:
        StructuredRecord nestedRecord = input.get(fieldName);
        Schema schema = getNonNullableSchema(input.getSchema().getField(fieldName).getSchema());
        for (Schema.Field field : schema.getFields()) {
          processField(field, nestedRecord, data, header, mergeRanges, false);
        }
        break;
      default:
        throw new IllegalStateException(String.format("Complex data format '%s' is not supported.",
          fieldType.toString()));
    }
  }

  private Schema.LogicalType getFieldLogicalType(Schema fieldSchema) {
    return fieldSchema.isNullable() ?
      fieldSchema.getNonNullable().getLogicalType() :
      fieldSchema.getLogicalType();
  }

  private Schema.Type getFieldType(Schema fieldSchema) {
    return fieldSchema.isNullable() ?
      fieldSchema.getNonNullable().getType() :
      fieldSchema.getType();
  }

  private Schema getNonNullableSchema(Schema fieldSchema) {
    return fieldSchema.isNullable() ?
      fieldSchema.getNonNullable() :
      fieldSchema;
  }

  private List<CellData> copyRow(List<CellData> row) {
    List<CellData> copiedRow = new ArrayList<>();
    copiedRow.addAll(row);
    return copiedRow;
  }

  private boolean isSimpleType(Schema.Type fieldType) {
    return Arrays.asList(Schema.Type.STRING, Schema.Type.BYTES, Schema.Type.BOOLEAN, Schema.Type.LONG,
      Schema.Type.INT, Schema.Type.DOUBLE, Schema.Type.FLOAT, Schema.Type.NULL).contains(fieldType);
  }

  private boolean isComplexType(Schema.Type fieldType) {
    return fieldType.equals(Schema.Type.ARRAY) || fieldType.equals(Schema.Type.RECORD);
  }

  private ZonedDateTime getZonedDateTime(long ts, TimeUnit unit, ZoneId zoneId) {
    long mod = unit.convert(1, TimeUnit.SECONDS);
    int fraction = (int) (ts % mod);
    long tsInSeconds = unit.toSeconds(ts);
    Instant instant = Instant.ofEpochSecond(tsInSeconds, unit.toNanos(fraction));
    return ZonedDateTime.ofInstant(instant, zoneId);
  }

  private Double toSheetsDate(LocalDate date) {
    return (double) ChronoUnit.DAYS.between(SHEETS_START_DATE, date);
  }

  private Double toSheetsDateTime(ZonedDateTime dateTime) {
    ZonedDateTime startOfTheDay = dateTime.toLocalDate().atStartOfDay(UTC_ZONE_ID);
    long daysNumber = ChronoUnit.DAYS.between(SHEETS_START_DATE, dateTime);
    long micros = ChronoUnit.MICROS.between(startOfTheDay, dateTime);
    return (double) daysNumber + (double) micros / (double) ChronoField.MICRO_OF_DAY.range().getMaximum();
  }

  private Double toSheetsTime(LocalTime localTime) {
    long micros = localTime.getLong(ChronoField.MICRO_OF_DAY);
    return (double) micros / (double) ChronoField.MICRO_OF_DAY.range().getMaximum();
  }
}
