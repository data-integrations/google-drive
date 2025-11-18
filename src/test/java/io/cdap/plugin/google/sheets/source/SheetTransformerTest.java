/*
 * Copyright © 2025 Cask Data, Inc.
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

import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.ExtendedValue;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.sheets.source.utils.ComplexSingleValueColumn;
import io.cdap.plugin.google.sheets.source.utils.RowRecord;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

public class SheetTransformerTest {

  private static final String SPREADSHEET_NAME = "TestSpreadsheet";
  private static final String SHEET_TITLE = "TestSheet";
  private static final String METADATA_RECORD_NAME = "metadata";
  private static final String SPREADSHEET_FIELD_NAME = "spreadsheetName";
  private static final String SHEET_FIELD_NAME = "sheetTitle";

  @Test
  public void transform_testAllDataTypes_runSuccessfully() {
    Schema schema =
      Schema.recordOf("record",
                      Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                      Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                      Schema.Field.of("date_field",
                                      Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                      Schema.Field.of("ts_field",
                                      Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))),
                      Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG)))
      );

    Map<String, ComplexSingleValueColumn> headeredCells = new HashMap<>();
    ComplexSingleValueColumn stringColumn = new ComplexSingleValueColumn();
    stringColumn.setData(new CellData()
                           .setFormattedValue("test_string")
                           .setEffectiveValue(new ExtendedValue().setStringValue("test_string")));
    headeredCells.put("string_field", stringColumn);

    ComplexSingleValueColumn doubleColumn = new ComplexSingleValueColumn();
    doubleColumn.setData(new CellData()
                           .setFormattedValue("123.45")
                           .setEffectiveValue(new ExtendedValue().setNumberValue(123.45)));
    headeredCells.put("double_field", doubleColumn);

    ComplexSingleValueColumn booleanColumn = new ComplexSingleValueColumn();
    booleanColumn.setData(new CellData()
                            .setFormattedValue("TRUE")
                            .setEffectiveValue(new ExtendedValue().setBoolValue(true)));
    headeredCells.put("boolean_field", booleanColumn);

    // Corresponds to 2023-10-27
    ComplexSingleValueColumn dateColumn = new ComplexSingleValueColumn();
    dateColumn.setData(new CellData()
                         .setUserEnteredValue(new ExtendedValue().setNumberValue(45226.0)));
    headeredCells.put("date_field", dateColumn);

    // Corresponds to 2023-10-26 09:59:59 UTC
    double tsValue = 45225.4166666667;
    ComplexSingleValueColumn tsColumn = new ComplexSingleValueColumn();
    tsColumn.setData(new CellData()
                       .setUserEnteredValue(new ExtendedValue().setNumberValue(tsValue)));
    headeredCells.put("ts_field", tsColumn);

    // Corresponds to 1 day
    double longValue = 1.0;
    ComplexSingleValueColumn longColumn = new ComplexSingleValueColumn();
    longColumn.setData(new CellData()
                         .setUserEnteredValue(new ExtendedValue().setNumberValue(longValue)));
    headeredCells.put("long_field", longColumn);

    RowRecord rowRecord = new RowRecord(SPREADSHEET_NAME, SHEET_TITLE, null, headeredCells, false);

    StructuredRecord transformed = SheetTransformer.transform(rowRecord, schema, false, null, false, null, null);

    Assert.assertEquals("test_string", transformed.get("string_field"));
    Assert.assertEquals(123.45, transformed.get("double_field"), 0.001);
    Assert.assertEquals(true, transformed.get("boolean_field"));
    Assert.assertEquals(LocalDate.of(2023, 10, 27), transformed.getDate("date_field"));

    long dayMicros = ChronoField.MICRO_OF_DAY.range().getMaximum();
    ZonedDateTime expectedTimestamp = ZonedDateTime.of(1899, 12, 30, 0, 0, 0, 0,
                                                       ZoneId.ofOffset("UTC", ZoneOffset.UTC))
      .plus((long) (tsValue * dayMicros), ChronoUnit.MICROS);
    Assert.assertEquals(expectedTimestamp.toInstant().toEpochMilli(),
                        transformed.getTimestamp("ts_field").toInstant().toEpochMilli());

    long expectedLong = (long) (longValue * dayMicros / 1000);
    Assert.assertEquals(expectedLong, (long) transformed.get("long_field"));
  }

  @Test
  public void transform_addMetadata_metadataColumnAdded() {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of(METADATA_RECORD_NAME, Schema.nullableOf(Schema.mapOf(
                                      Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))))
    );

    Map<String, ComplexSingleValueColumn> headeredCells = new HashMap<>();
    ComplexSingleValueColumn stringColumn = new ComplexSingleValueColumn();
    stringColumn.setData(new CellData()
                           .setFormattedValue("test_string"));
    headeredCells.put("string_field", stringColumn);

    Map<String, String> metadataMap = new HashMap<>();
    metadataMap.put("spreadsheetName", SPREADSHEET_NAME);
    metadataMap.put("sheetTitle", SHEET_TITLE);

    RowRecord rowRecord = new RowRecord(SPREADSHEET_NAME, SHEET_TITLE, metadataMap, headeredCells, false);

    StructuredRecord transformed = SheetTransformer.transform(rowRecord, schema, true, METADATA_RECORD_NAME,
                                                              false, null, null);

    Assert.assertEquals("test_string", transformed.get("string_field"));
    Assert.assertEquals(metadataMap, transformed.get(METADATA_RECORD_NAME));
  }

  @Test
  public void transform_WithAddedNames_sheetAndSpreadsheetFieldsAdded() {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of(SPREADSHEET_FIELD_NAME,
                                                    Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of(SHEET_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    Map<String, ComplexSingleValueColumn> headeredCells = new HashMap<>();
    ComplexSingleValueColumn stringColumn = new ComplexSingleValueColumn();
    stringColumn.setData(new CellData()
                           .setFormattedValue("test_string"));
    headeredCells.put("string_field", stringColumn);

    RowRecord rowRecord = new RowRecord(SPREADSHEET_NAME, SHEET_TITLE, null, headeredCells, false);

    StructuredRecord transformed = SheetTransformer.transform(rowRecord, schema, false, null,
                                                              true, SPREADSHEET_FIELD_NAME, SHEET_FIELD_NAME);

    Assert.assertEquals("test_string", transformed.get("string_field"));
    Assert.assertEquals(SPREADSHEET_NAME, transformed.get(SPREADSHEET_FIELD_NAME));
    Assert.assertEquals(SHEET_TITLE, transformed.get(SHEET_FIELD_NAME));
  }

  @Test
  public void transform_WithNullValues_runSuccessfully() {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("null_field", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    Map<String, ComplexSingleValueColumn> headeredCells = new HashMap<>();
    ComplexSingleValueColumn nullColumn = new ComplexSingleValueColumn();
    nullColumn.setData(null);
    headeredCells.put("null_field", nullColumn);

    RowRecord rowRecord = new RowRecord(SPREADSHEET_NAME, SHEET_TITLE, null, headeredCells, true);

    StructuredRecord transformed = SheetTransformer.transform(rowRecord, schema, false, null, false, null, null);

    Assert.assertNull(transformed.get("null_field"));
  }

  @Test
  public void transform_WithNestedRecord_runSuccessfully() {
    Schema nestedSchema = Schema.recordOf("nested",
                                          Schema.Field.of("sub_field",
                                                          Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("record_field", Schema.nullableOf(nestedSchema))
    );

    Map<String, ComplexSingleValueColumn> subColumns = new HashMap<>();
    ComplexSingleValueColumn subColumn = new ComplexSingleValueColumn();
    subColumn.setData(new CellData().setFormattedValue("nested_value"));
    subColumns.put("sub_field", subColumn);

    Map<String, ComplexSingleValueColumn> headeredCells = new HashMap<>();
    ComplexSingleValueColumn recordColumn = new ComplexSingleValueColumn();
    recordColumn.setSubColumns(subColumns);
    headeredCells.put("record_field", recordColumn);

    RowRecord rowRecord = new RowRecord(SPREADSHEET_NAME, SHEET_TITLE, null, headeredCells, false);

    StructuredRecord transformed = SheetTransformer.transform(rowRecord, schema, false, null, false, null, null);

    StructuredRecord nestedRecord = transformed.get("record_field");
    Assert.assertNotNull(nestedRecord);
    Assert.assertEquals("nested_value", nestedRecord.get("sub_field"));
  }
}
