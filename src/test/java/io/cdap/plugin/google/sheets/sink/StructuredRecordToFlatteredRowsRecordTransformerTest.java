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

package io.cdap.plugin.google.sheets.sink;

import com.google.api.services.sheets.v4.model.CellData;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.sheets.sink.utils.FlatteredRowsRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.List;

public class StructuredRecordToFlatteredRowsRecordTransformerTest {
  private static final String SCHEMA_NAME = "default";
  private static final String DATE_FIELD_NAME = "date";
  private static final String TIME_FIELD_NAME = "time";
  private static final String DATE_TIME_FIELD_NAME = "date_time";
  private static final String SPREADSHEET_NAME_FIELD_NAME = "spreadsheet";
  private static final String STRING_FIELD_NAME = "testString";
  private static final String SHEET_TITLE_FIELD_NAME = "sheet";

  private static final LocalDate TEST_DATE = LocalDate.of(2019, 03, 14);
  private static final LocalTime TEST_TIME = LocalTime.of(13, 03, 14);
  private static final ZonedDateTime TEST_DATE_TIME = ZonedDateTime.of(TEST_DATE, TEST_TIME,
    StructuredRecordToFlatteredRowsRecordTransformer.UTC_ZONE_ID);
  private static final String SPREADSHEET_NAME = "spName";
  private static final String SHEET_TITLE = "title";
  private static final String PRESET_SPREADSHEET_TITLE = "generalName";
  private static final String PRESET_SHEET_TITLE = "generalTitle";
  private static final String STRING_VALUE = "any string";

  @Test
  public void testToSheetsDate() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    LocalDate testDate = LocalDate.of(2020, 01, 11);
    StructuredRecordToFlatteredRowsRecordTransformer transformer =
      new StructuredRecordToFlatteredRowsRecordTransformer("", "", "", "", false);

    Method toSheetsDataMethod = transformer.getClass().getDeclaredMethod("toSheetsDate", LocalDate.class);
    toSheetsDataMethod.setAccessible(true);

    Double expected = (Double) toSheetsDataMethod.invoke(transformer, testDate);
    Assert.assertEquals(Double.valueOf(43841.0), expected);
  }

  @Test
  public void testToSheetsDateTime() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    ZonedDateTime testZonedDateTime = ZonedDateTime.of(1991, 03, 8, 13, 54, 20, 0,
      StructuredRecordToFlatteredRowsRecordTransformer.UTC_ZONE_ID);
    StructuredRecordToFlatteredRowsRecordTransformer transformer =
      new StructuredRecordToFlatteredRowsRecordTransformer("", "", "", "", false);

    Method toSheetsDateTimeMethod = transformer.getClass().getDeclaredMethod("toSheetsDateTime", ZonedDateTime.class);
    toSheetsDateTimeMethod.setAccessible(true);

    Double expected = (Double) toSheetsDateTimeMethod.invoke(transformer, testZonedDateTime);
    Assert.assertEquals(Double.valueOf(33305.57939814815), expected, Math.pow(10, -11));
  }

  @Test
  public void testToSheetsTime() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
    LocalTime testLocalTime = LocalTime.of(13, 54, 20, 0);
    StructuredRecordToFlatteredRowsRecordTransformer transformer =
      new StructuredRecordToFlatteredRowsRecordTransformer("", "", "", "", false);

    Method toSheetsTimeMethod = transformer.getClass().getDeclaredMethod("toSheetsTime", LocalTime.class);
    toSheetsTimeMethod.setAccessible(true);

    Double expected = (Double) toSheetsTimeMethod.invoke(transformer, testLocalTime);
    Assert.assertEquals(Double.valueOf(0.57939814815), expected, Math.pow(10, -11));
  }

  @Test
  public void testProcessDateTimeDateValue() throws NoSuchMethodException, InvocationTargetException,
    IllegalAccessException {
    StructuredRecordToFlatteredRowsRecordTransformer transformer =
      new StructuredRecordToFlatteredRowsRecordTransformer("", "", "", "", false);

    Schema dataSchema = Schema.recordOf(SCHEMA_NAME,
      Schema.Field.of(DATE_FIELD_NAME, Schema.of(Schema.LogicalType.DATE)));
    StructuredRecord.Builder builder = StructuredRecord.builder(dataSchema);
    builder.setDate(DATE_FIELD_NAME, TEST_DATE);
    StructuredRecord dateRecord = builder.build();

    Method processDateTimeValueMethod = transformer.getClass().getDeclaredMethod("processDateTimeValue",
      Schema.LogicalType.class, Schema.class, Object.class);
    processDateTimeValueMethod.setAccessible(true);

    CellData resultCell = (CellData) processDateTimeValueMethod.invoke(transformer,
      dataSchema.getField(DATE_FIELD_NAME).getSchema().getLogicalType(),
      Schema.of(Schema.LogicalType.DATE),
      dateRecord.get(DATE_FIELD_NAME));
    Assert.assertNotNull(resultCell.getUserEnteredFormat());
    Assert.assertNotNull(resultCell.getUserEnteredValue());
    Assert.assertNotNull(resultCell.getUserEnteredValue().getNumberValue());
    Assert.assertNotNull(resultCell.getUserEnteredFormat().getNumberFormat());

    Method toSheetsDateMethod = transformer.getClass().getDeclaredMethod("toSheetsDate", LocalDate.class);
    toSheetsDateMethod.setAccessible(true);

    Assert.assertEquals((Double) toSheetsDateMethod.invoke(transformer, TEST_DATE),
      resultCell.getUserEnteredValue().getNumberValue());
    Assert.assertEquals(StructuredRecordToFlatteredRowsRecordTransformer.SHEETS_CELL_DATE_TYPE,
      resultCell.getUserEnteredFormat().getNumberFormat().getType());
  }

  @Test
  public void testProcessDateTimeTimeValue() throws NoSuchMethodException, InvocationTargetException,
    IllegalAccessException {
    StructuredRecordToFlatteredRowsRecordTransformer transformer =
      new StructuredRecordToFlatteredRowsRecordTransformer("", "", "", "", false);

    Schema timeSchema = Schema.recordOf(SCHEMA_NAME,
      Schema.Field.of(TIME_FIELD_NAME, Schema.of(Schema.LogicalType.TIME_MILLIS)));
    StructuredRecord.Builder builder = StructuredRecord.builder(timeSchema);
    builder.setTime(TIME_FIELD_NAME, TEST_TIME);
    StructuredRecord dateRecord = builder.build();

    Method processDateTimeValueMethod = transformer.getClass().getDeclaredMethod("processDateTimeValue",
      Schema.LogicalType.class, Schema.class, Object.class);
    processDateTimeValueMethod.setAccessible(true);

    CellData resultCell = (CellData) processDateTimeValueMethod.invoke(transformer,
      timeSchema.getField(TIME_FIELD_NAME).getSchema().getLogicalType(),
      Schema.of(Schema.LogicalType.TIME_MILLIS),
      dateRecord.get(TIME_FIELD_NAME));
    Assert.assertNotNull(resultCell.getUserEnteredFormat());
    Assert.assertNotNull(resultCell.getUserEnteredValue());
    Assert.assertNotNull(resultCell.getUserEnteredValue().getNumberValue());
    Assert.assertNotNull(resultCell.getUserEnteredFormat().getNumberFormat());

    Method toSheetsTimeMethod = transformer.getClass().getDeclaredMethod("toSheetsTime", LocalTime.class);
    toSheetsTimeMethod.setAccessible(true);

    Assert.assertEquals((Double) toSheetsTimeMethod.invoke(transformer, TEST_TIME),
      resultCell.getUserEnteredValue().getNumberValue());
    Assert.assertEquals(StructuredRecordToFlatteredRowsRecordTransformer.SHEETS_CELL_TIME_TYPE,
      resultCell.getUserEnteredFormat().getNumberFormat().getType());
  }

  @Test
  public void testProcessDateTimeDateTimeValue() throws NoSuchMethodException, InvocationTargetException,
    IllegalAccessException {
    StructuredRecordToFlatteredRowsRecordTransformer transformer =
      new StructuredRecordToFlatteredRowsRecordTransformer("", "", "", "", false);

    Schema dateTimeSchema = Schema.recordOf(SCHEMA_NAME,
      Schema.Field.of(DATE_TIME_FIELD_NAME, Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)));
    StructuredRecord.Builder builder = StructuredRecord.builder(dateTimeSchema);
    builder.setTimestamp(DATE_TIME_FIELD_NAME, TEST_DATE_TIME);
    StructuredRecord dateRecord = builder.build();

    Method processDateTimeValueMethod = transformer.getClass().getDeclaredMethod("processDateTimeValue",
      Schema.LogicalType.class, Schema.class, Object.class);
    processDateTimeValueMethod.setAccessible(true);

    CellData resultCell = (CellData) processDateTimeValueMethod.invoke(transformer,
      dateTimeSchema.getField(DATE_TIME_FIELD_NAME).getSchema().getLogicalType(),
      Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS),
      dateRecord.get(DATE_TIME_FIELD_NAME));
    Assert.assertNotNull(resultCell.getUserEnteredFormat());
    Assert.assertNotNull(resultCell.getUserEnteredValue());
    Assert.assertNotNull(resultCell.getUserEnteredValue().getNumberValue());
    Assert.assertNotNull(resultCell.getUserEnteredFormat().getNumberFormat());

    Method toSheetsDateTimeMethod =
      transformer.getClass().getDeclaredMethod("toSheetsDateTime", ZonedDateTime.class);
    toSheetsDateTimeMethod.setAccessible(true);

    Assert.assertEquals((Double) toSheetsDateTimeMethod.invoke(transformer, TEST_DATE_TIME),
      resultCell.getUserEnteredValue().getNumberValue());
    Assert.assertEquals(StructuredRecordToFlatteredRowsRecordTransformer.SHEETS_CELL_DATE_TIME_TYPE,
      resultCell.getUserEnteredFormat().getNumberFormat().getType());
  }

  @Test
  public void testTransformWithSpreadsheetAndSheetNames() {
    StructuredRecordToFlatteredRowsRecordTransformer transformer = new StructuredRecordToFlatteredRowsRecordTransformer(
      SPREADSHEET_NAME_FIELD_NAME,
      SHEET_TITLE_FIELD_NAME,
      PRESET_SPREADSHEET_TITLE,
      PRESET_SHEET_TITLE,
      false);
    StructuredRecord testRecord = getTestTransformRecord();

    FlatteredRowsRecord result = transformer.transform(testRecord);

    Assert.assertEquals(SPREADSHEET_NAME, result.getSpreadsheetName());
    Assert.assertEquals(SHEET_TITLE, result.getSheetTitle());

    checkSimpleRecord(result);
  }

  @Test
  public void testTransformWithDefaultName() throws IOException {
    StructuredRecordToFlatteredRowsRecordTransformer transformer = new StructuredRecordToFlatteredRowsRecordTransformer(
      "",
      "",
      PRESET_SPREADSHEET_TITLE,
      PRESET_SHEET_TITLE,
      false);
    StructuredRecord testRecord = getTestTransformRecord();

    FlatteredRowsRecord result = transformer.transform(testRecord);

    Assert.assertEquals(PRESET_SPREADSHEET_TITLE, result.getSpreadsheetName());
    Assert.assertEquals(PRESET_SHEET_TITLE, result.getSheetTitle());

    checkSimpleRecord(result);
  }

  @Test
  public void testTransformWithSpreadsheetAndDefaultSheetNames() throws IOException {
    StructuredRecordToFlatteredRowsRecordTransformer transformer = new StructuredRecordToFlatteredRowsRecordTransformer(
      SPREADSHEET_NAME_FIELD_NAME,
      "",
      "",
      PRESET_SHEET_TITLE,
      false);
    StructuredRecord testRecord = getTestTransformRecord();

    FlatteredRowsRecord result = transformer.transform(testRecord);

    Assert.assertEquals(SPREADSHEET_NAME, result.getSpreadsheetName());
    Assert.assertEquals(PRESET_SHEET_TITLE, result.getSheetTitle());

    checkSimpleRecord(result);
  }

  private StructuredRecord getTestTransformRecord() {
    Schema testSchema = Schema.recordOf(SCHEMA_NAME,
      Schema.Field.of(SPREADSHEET_NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SHEET_TITLE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(STRING_FIELD_NAME, Schema.of(Schema.Type.STRING)));

    StructuredRecord.Builder builder = StructuredRecord.builder(testSchema);
    builder.set(SPREADSHEET_NAME_FIELD_NAME, SPREADSHEET_NAME);
    builder.set(SHEET_TITLE_FIELD_NAME, SHEET_TITLE);
    builder.set(STRING_FIELD_NAME, STRING_VALUE);
    return builder.build();
  }

  private void checkSimpleRecord(FlatteredRowsRecord result) {
    // just one row
    Assert.assertNotNull(result.getSingleRowRecords().size());
    Assert.assertEquals(1, result.getSingleRowRecords().size());

    // with three columns
    List<CellData> row = result.getSingleRowRecords().get(0);
    Assert.assertNotNull(row);
    Assert.assertEquals(3, row.size());
    Assert.assertEquals(SPREADSHEET_NAME, row.get(0).getUserEnteredValue().getStringValue());
    Assert.assertEquals(SHEET_TITLE, row.get(1).getUserEnteredValue().getStringValue());
    Assert.assertEquals(STRING_VALUE, row.get(2).getUserEnteredValue().getStringValue());

    // check the headers
    Assert.assertEquals(3, result.getHeader().getSubHeaders().size());
    Assert.assertEquals(3, result.getHeader().getWidth());
    Assert.assertEquals(1, result.getHeader().getDepth() - 1);
    Assert.assertEquals(SPREADSHEET_NAME_FIELD_NAME, result.getHeader().getSubHeaders().get(0).getName());
    Assert.assertEquals(SHEET_TITLE_FIELD_NAME, result.getHeader().getSubHeaders().get(1).getName());
    Assert.assertEquals(STRING_FIELD_NAME, result.getHeader().getSubHeaders().get(2).getName());
  }
}
