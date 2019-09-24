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

package io.cdap.plugin.google.sheets.source.utils;

import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.ExtendedValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultipleRowRecordTest {

  private static final String TEST_SPREADSHEET_NAME = "spreadsheetName";
  private static final String TEST_SHEET_NAME = "sheetName";

  @Test
  public void testGetRowRecordInRange() {
    MultipleRowRecord testMultipleRowRecord = getTestRecord();
    RowRecord firstRecord = testMultipleRowRecord.getRowRecord(0);

    Assert.assertEquals(TEST_SPREADSHEET_NAME, firstRecord.getSpreadsheetName());
    Assert.assertEquals(TEST_SHEET_NAME, firstRecord.getSheetTitle());

    Map<String, ComplexSingleValueColumn> headeredCells = firstRecord.getHeaderedCells();

    // check top-level
    Assert.assertEquals(2, headeredCells.size());
    Assert.assertTrue(headeredCells.containsKey("h0"));
    Assert.assertTrue(headeredCells.containsKey("h1"));

    // check simple header
    ComplexSingleValueColumn simpleHeader = headeredCells.get("h0");
    Assert.assertEquals("r00", simpleHeader.getData().getUserEnteredValue().getStringValue());
    Assert.assertTrue(simpleHeader.getSubColumns().isEmpty());

    // check complex header
    ComplexSingleValueColumn complexHeader = headeredCells.get("h1");
    Assert.assertNull(complexHeader.getData());
    Assert.assertEquals(2, complexHeader.getSubColumns().size());

    // check first sub-column
    ComplexSingleValueColumn subColumn0 = complexHeader.getSubColumns().get("h10");
    Assert.assertEquals("r01", subColumn0.getData().getUserEnteredValue().getStringValue());
    Assert.assertTrue(subColumn0.getSubColumns().isEmpty());

    // check second sub-column
    ComplexSingleValueColumn subColumn1 = complexHeader.getSubColumns().get("h11");
    Assert.assertEquals("r02", subColumn1.getData().getUserEnteredValue().getStringValue());
    Assert.assertTrue(subColumn1.getSubColumns().isEmpty());
  }

  @Test
  public void testGetRowRecordLastInRange() {
    MultipleRowRecord testMultipleRowRecord = getTestRecord();
    RowRecord firstRecord = testMultipleRowRecord.getRowRecord(1);

    Assert.assertEquals(TEST_SPREADSHEET_NAME, firstRecord.getSpreadsheetName());
    Assert.assertEquals(TEST_SHEET_NAME, firstRecord.getSheetTitle());

    Map<String, ComplexSingleValueColumn> headeredCells = firstRecord.getHeaderedCells();

    // check top-level
    Assert.assertEquals(2, headeredCells.size());
    Assert.assertTrue(headeredCells.containsKey("h0"));
    Assert.assertTrue(headeredCells.containsKey("h1"));

    // check simple header
    ComplexSingleValueColumn simpleHeader = headeredCells.get("h0");
    Assert.assertEquals("r10", simpleHeader.getData().getUserEnteredValue().getStringValue());
    Assert.assertTrue(simpleHeader.getSubColumns().isEmpty());

    // check complex header
    ComplexSingleValueColumn complexHeader = headeredCells.get("h1");
    Assert.assertNull(complexHeader.getData());
    Assert.assertEquals(2, complexHeader.getSubColumns().size());

    // check first sub-column
    ComplexSingleValueColumn subColumn0 = complexHeader.getSubColumns().get("h10");
    Assert.assertEquals("r11", subColumn0.getData().getUserEnteredValue().getStringValue());
    Assert.assertTrue(subColumn0.getSubColumns().isEmpty());

    // check second sub-column
    ComplexSingleValueColumn subColumn1 = complexHeader.getSubColumns().get("h11");
    Assert.assertEquals("r12", subColumn1.getData().getUserEnteredValue().getStringValue());
    Assert.assertTrue(subColumn1.getSubColumns().isEmpty());
  }

  @Test
  public void testGetRowRecordOutOfRange() {
    MultipleRowRecord testMultipleRowRecord = getTestRecord();
    RowRecord firstRecord = testMultipleRowRecord.getRowRecord(2);

    Assert.assertEquals(TEST_SPREADSHEET_NAME, firstRecord.getSpreadsheetName());
    Assert.assertEquals(TEST_SHEET_NAME, firstRecord.getSheetTitle());

    Map<String, ComplexSingleValueColumn> headeredCells = firstRecord.getHeaderedCells();

    // check top-level
    Assert.assertEquals(2, headeredCells.size());
    Assert.assertTrue(headeredCells.containsKey("h0"));
    Assert.assertTrue(headeredCells.containsKey("h1"));

    // check simple header
    ComplexSingleValueColumn simpleHeader = headeredCells.get("h0");
    Assert.assertNull(simpleHeader.getData());
    Assert.assertTrue(simpleHeader.getSubColumns().isEmpty());

    // check complex header
    ComplexSingleValueColumn complexHeader = headeredCells.get("h1");
    Assert.assertNull(complexHeader.getData());
    Assert.assertEquals(2, complexHeader.getSubColumns().size());

    // check first sub-column
    ComplexSingleValueColumn subColumn0 = complexHeader.getSubColumns().get("h10");
    Assert.assertNull(subColumn0.getData());
    Assert.assertTrue(subColumn0.getSubColumns().isEmpty());

    // check second sub-column
    ComplexSingleValueColumn subColumn1 = complexHeader.getSubColumns().get("h11");
    Assert.assertNull(subColumn1.getData());
    Assert.assertTrue(subColumn1.getSubColumns().isEmpty());
  }

  private MultipleRowRecord getTestRecord() {
    Map<String, String> metadata = new HashMap<>();
    Map<String, ComplexMultiValueColumn> headeredCells = new HashMap<>();

    List<CellData> headerCells0 = Arrays.asList(
      new CellData().setUserEnteredValue(new ExtendedValue().setStringValue("r00")),
      new CellData().setUserEnteredValue(new ExtendedValue().setStringValue("r10")));
    headeredCells.put("h0", new ComplexMultiValueColumn(headerCells0));

    Map<String, ComplexMultiValueColumn> subHeaderdCells = new HashMap<>();

    List<CellData> headerCells10 = Arrays.asList(
      new CellData().setUserEnteredValue(new ExtendedValue().setStringValue("r01")),
      new CellData().setUserEnteredValue(new ExtendedValue().setStringValue("r11")));
    List<CellData> headerCells11 = Arrays.asList(
      new CellData().setUserEnteredValue(new ExtendedValue().setStringValue("r02")),
      new CellData().setUserEnteredValue(new ExtendedValue().setStringValue("r12")));
    subHeaderdCells.put("h10", new ComplexMultiValueColumn(headerCells10));
    subHeaderdCells.put("h11", new ComplexMultiValueColumn(headerCells11));
    headeredCells.put("h1", new ComplexMultiValueColumn(subHeaderdCells));

    return new MultipleRowRecord(TEST_SPREADSHEET_NAME, TEST_SHEET_NAME, metadata,
      headeredCells, Collections.emptyList());
  }
}
