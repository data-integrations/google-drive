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

import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.ExtendedValue;
import com.google.api.services.sheets.v4.model.GridRange;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.validation.DefaultFailureCollector;
import io.cdap.plugin.google.sheets.source.utils.CellCoordinate;
import io.cdap.plugin.google.sheets.source.utils.ColumnComplexSchemaInfo;
import io.cdap.plugin.google.sheets.source.utils.MetadataKeyValueAddress;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GoogleSheetsSourceConfigTest {

  private GoogleSheetsSourceConfig config = new GoogleSheetsSourceConfig();

  @Test
  public void testMetadataInputToMap() throws NoSuchMethodException, InvocationTargetException,
    IllegalAccessException {
    String metadataCellsInput = "A1:B1,A2:B2,A5:B3";
    Map<String, String> expectedKeyValues = new HashMap<String, String>() {{
      put("A1", "B1");
      put("A2", "B2");
      put("A5", "B3");
    }};
    Method metadataInputToMapMethod = config.getClass().getDeclaredMethod("metadataInputToMap", String.class);
    metadataInputToMapMethod.setAccessible(true);

    Assert.assertEquals(expectedKeyValues, metadataInputToMapMethod.invoke(config, metadataCellsInput));
  }

  @Test
  public void testEmptyMetadataInputToMap() throws NoSuchMethodException, InvocationTargetException,
    IllegalAccessException {
    Method metadataInputToMapMethod = config.getClass().getDeclaredMethod("metadataInputToMap", String.class);
    metadataInputToMapMethod.setAccessible(true);
    Assert.assertEquals(Collections.EMPTY_MAP, metadataInputToMapMethod.invoke(config, ""));
  }

  @Test
  public void testToCoordinate() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method toCoordinateMethod = config.getClass().getDeclaredMethod("toCoordinate", String.class);
    toCoordinateMethod.setAccessible(true);

    Assert.assertEquals(new CellCoordinate(1, 1), toCoordinateMethod.invoke(config, "A1"));
    Assert.assertEquals(new CellCoordinate(100, 26), toCoordinateMethod.invoke(config, "Z100"));
    Assert.assertEquals(new CellCoordinate(100, 52), toCoordinateMethod.invoke(config, "AZ100"));
  }

  @Test(expected = InvocationTargetException.class)
  public void testToCoordinateInvalidValue() throws InvocationTargetException, IllegalAccessException,
    NoSuchMethodException {
    Method toCoordinateMethod = config.getClass().getDeclaredMethod("toCoordinate", String.class);
    toCoordinateMethod.setAccessible(true);

    Assert.assertEquals(new CellCoordinate(1, 1), toCoordinateMethod.invoke(config, "1A"));
  }

  @Test
  public void testGetMetadataCoordinates() throws NoSuchFieldException, IllegalAccessException {
    setFieldValue("metadataCells", "A1:B2,A2:B4,A5:B7");
    setFieldValue("extractMetadata", true);

    List<MetadataKeyValueAddress> metadataCoordinates = config.getMetadataCoordinates();

    List<MetadataKeyValueAddress> expectedCoordinates = new ArrayList<>();
    expectedCoordinates.add(new MetadataKeyValueAddress(new CellCoordinate(1, 1),
        new CellCoordinate(2, 2)));
    expectedCoordinates.add(new MetadataKeyValueAddress(new CellCoordinate(2, 1),
        new CellCoordinate(4, 2)));
    expectedCoordinates.add(new MetadataKeyValueAddress(new CellCoordinate(5, 1),
        new CellCoordinate(7, 2)));

    Assert.assertEquals(expectedCoordinates, metadataCoordinates);
  }

  @Test
  public void testValidateMetadataCellsOnlyHeader() throws NoSuchFieldException, IllegalAccessException,
    NoSuchMethodException, InvocationTargetException {
    Method validateMetadataCellsMethod =
      config.getClass().getDeclaredMethod("validateMetadataCells", FailureCollector.class);
    validateMetadataCellsMethod.setAccessible(true);

    setFieldValue("metadataCells", "A3:C3,Z3:AA3,B3:YU3");
    setFieldValue("firstHeaderRow", 3);
    setFieldValue("lastHeaderRow", 3);
    setFieldValue("firstFooterRow", -1);
    setFieldValue("lastFooterRow", -1);

    FailureCollector collector = new DefaultFailureCollector("", Collections.EMPTY_MAP);

    // all cells are from row with index 3
    validateMetadataCellsMethod.invoke(config, collector);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());

    // some cells are from rows with 2 and 4 indexes
    String beforeAddress = "A2";
    String afterAddress = "Z4";
    setFieldValue("metadataCells", String.format("%s:C3,Z3:%s,B3:YU3", beforeAddress, afterAddress));
    validateMetadataCellsMethod.invoke(config, collector);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(2, failures.size());
    Assert.assertTrue(failures.get(0).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", beforeAddress))
        || failures.get(0).getMessage().equals(
            String.format("Metadata cell '%s' is out of header or footer rows.", afterAddress)));
    Assert.assertTrue(failures.get(1).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", beforeAddress))
        || failures.get(1).getMessage().equals(
            String.format("Metadata cell '%s' is out of header or footer rows.", afterAddress)));
    Assert.assertNotEquals(failures.get(0).getMessage(), failures.get(1).getMessage());
  }

  @Test
  public void testValidateMetadataCellsOnlyFooter() throws NoSuchFieldException, IllegalAccessException,
    NoSuchMethodException, InvocationTargetException {
    Method validateMetadataCellsMethod =
      config.getClass().getDeclaredMethod("validateMetadataCells", FailureCollector.class);
    validateMetadataCellsMethod.setAccessible(true);

    setFieldValue("metadataCells", "A6:B7,Z7:U6,B8:A8");
    setFieldValue("firstHeaderRow", -1);
    setFieldValue("lastHeaderRow", -1);
    setFieldValue("firstFooterRow", 6);
    setFieldValue("lastFooterRow", 8);

    FailureCollector collector = new DefaultFailureCollector("", Collections.EMPTY_MAP);

    // all cells are from rows with indexes 6-8
    validateMetadataCellsMethod.invoke(config, collector);

    Assert.assertTrue(collector.getValidationFailures().isEmpty());

    // some cells are from rows with 5 and 9 indexes
    String beforeAddress = "B5";
    String afterAddress = "X9";
    setFieldValue("metadataCells",
        String.format("A6:B7,Z7:U6,%s:A8,B8:%s", beforeAddress, afterAddress));
    validateMetadataCellsMethod.invoke(config, collector);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(2, failures.size());
    Assert.assertTrue(failures.get(0).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", beforeAddress))
        || failures.get(0).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", afterAddress)));
    Assert.assertTrue(failures.get(1).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", beforeAddress))
        || failures.get(1).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", afterAddress)));
    Assert.assertNotEquals(failures.get(0).getMessage(), failures.get(1).getMessage());
  }

  @Test
  public void testValidateMetadataCellsHeaderAndFooter() throws NoSuchFieldException, IllegalAccessException,
    NoSuchMethodException, InvocationTargetException {
    Method validateMetadataCellsMethod =
      config.getClass().getDeclaredMethod("validateMetadataCells", FailureCollector.class);
    validateMetadataCellsMethod.setAccessible(true);

    setFieldValue("metadataCells", "D3:A3,A6:B6,Z7:F3,B8:V6");
    setFieldValue("firstHeaderRow", 3);
    setFieldValue("lastHeaderRow", 3);
    setFieldValue("firstFooterRow", 6);
    setFieldValue("lastFooterRow", 8);

    FailureCollector collector = new DefaultFailureCollector("", Collections.EMPTY_MAP);

    // all cells are from rows with indexes 3-3 and 6-8
    validateMetadataCellsMethod.invoke(config, collector);

    Assert.assertTrue(collector.getValidationFailures().isEmpty());

    // some cells are from rows with 5 and 9 indexes
    String beforeAddress = "B5";
    String afterAddress = "X9";
    setFieldValue("metadataCells",
        String.format("A6:A3,Z7:B8,%s:C8,B8:%s", beforeAddress, afterAddress));
    validateMetadataCellsMethod.invoke(config, collector);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(2, failures.size());
    Assert.assertTrue(failures.get(0).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", beforeAddress))
        || failures.get(0).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", afterAddress)));
    Assert.assertTrue(failures.get(1).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", beforeAddress))
        || failures.get(1).getMessage().equals(
        String.format("Metadata cell '%s' is out of header or footer rows.", afterAddress)));
    Assert.assertNotEquals(failures.get(0).getMessage(), failures.get(1).getMessage());
  }

  @Test
  public void testProcessColumns() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method processColumnsMethod = config.getClass().getDeclaredMethod("processColumns", List.class,
      List.class, List.class, List.class, FailureCollector.class);
    processColumnsMethod.setAccessible(true);

    List<CellData> columnsRow = new ArrayList<>();
    columnsRow.add(new CellData().setFormattedValue("a"));
    columnsRow.add(new CellData().setFormattedValue("b"));
    columnsRow.add(new CellData());

    List<CellData> subColumnsRow = new ArrayList<>();
    subColumnsRow.add(new CellData().setFormattedValue("no header value"));
    subColumnsRow.add(new CellData().setFormattedValue("c"));
    subColumnsRow.add(new CellData().setFormattedValue("d"));

    List<CellData> dataRow = new ArrayList<>();
    dataRow.add(new CellData().setUserEnteredValue(new ExtendedValue().setStringValue("aa")));
    dataRow.add(new CellData().setUserEnteredValue(new ExtendedValue().setNumberValue(13d)));
    dataRow.add(new CellData().setUserEnteredValue(new ExtendedValue().setBoolValue(true)));

    List<GridRange> columnMerges = new ArrayList<>();
    columnMerges.add(new GridRange().setStartRowIndex(0).setEndRowIndex(1).setStartColumnIndex(1).setEndColumnIndex(3));

    FailureCollector collector = new DefaultFailureCollector("", Collections.EMPTY_MAP);

    LinkedHashMap<Integer, ColumnComplexSchemaInfo> columns =
      (LinkedHashMap<Integer, ColumnComplexSchemaInfo>) processColumnsMethod.invoke(config, columnsRow,
      subColumnsRow, dataRow, columnMerges, collector);

    Assert.assertEquals(2, columns.size());
    Assert.assertTrue(columns.keySet().containsAll(Arrays.asList(0, 1)));

    // check simple column
    Assert.assertEquals("a", columns.get(0).getHeaderTitle());
    Assert.assertTrue(columns.get(0).getSubColumns().isEmpty());

    // check complex columns
    Assert.assertEquals("b", columns.get(1).getHeaderTitle());
    List<ColumnComplexSchemaInfo> subColumns = columns.get(1).getSubColumns();
    Assert.assertFalse(subColumns.isEmpty());

    // check sub-columns
    Assert.assertEquals(2, subColumns.size());
    Assert.assertEquals("c", subColumns.get(0).getHeaderTitle());
    Assert.assertTrue(subColumns.get(0).getSubColumns().isEmpty());
    Assert.assertEquals("d", subColumns.get(1).getHeaderTitle());
    Assert.assertTrue(subColumns.get(0).getSubColumns().isEmpty());
  }

  @Test
  public void testProcessColumnsInvalidTitles()
    throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method processColumnsMethod = config.getClass().getDeclaredMethod("processColumns", List.class,
      List.class, List.class, List.class, FailureCollector.class);
    processColumnsMethod.setAccessible(true);

    List<CellData> columnsRow = new ArrayList<>();
    columnsRow.add(new CellData().setFormattedValue("a"));
    columnsRow.add(new CellData().setFormattedValue("title with space"));
    columnsRow.add(new CellData());

    List<CellData> subColumnsRow = new ArrayList<>();
    subColumnsRow.add(new CellData().setFormattedValue("no header value"));
    subColumnsRow.add(new CellData().setFormattedValue("9titleWithFirstNumber"));
    subColumnsRow.add(new CellData().setFormattedValue("d"));

    List<CellData> dataRow = new ArrayList<>();
    dataRow.add(new CellData().setUserEnteredValue(new ExtendedValue().setStringValue("aa")));
    dataRow.add(new CellData().setUserEnteredValue(new ExtendedValue().setNumberValue(13d)));
    dataRow.add(new CellData().setUserEnteredValue(new ExtendedValue().setBoolValue(true)));

    List<GridRange> columnMerges = new ArrayList<>();
    columnMerges.add(new GridRange().setStartRowIndex(0).setEndRowIndex(1).setStartColumnIndex(1).setEndColumnIndex(3));

    FailureCollector collector = new DefaultFailureCollector("", Collections.EMPTY_MAP);

    LinkedHashMap<Integer, ColumnComplexSchemaInfo> columns =
      (LinkedHashMap<Integer, ColumnComplexSchemaInfo>) processColumnsMethod.invoke(config, columnsRow,
      subColumnsRow, dataRow, columnMerges, collector);

    Assert.assertEquals(2, columns.size());
    Assert.assertTrue(columns.keySet().containsAll(Arrays.asList(0, 1)));

    // check simple column
    Assert.assertEquals("a", columns.get(0).getHeaderTitle());
    Assert.assertTrue(columns.get(0).getSubColumns().isEmpty());

    // check complex columns, top header should have column name as name
    Assert.assertEquals("B", columns.get(1).getHeaderTitle());
    List<ColumnComplexSchemaInfo> subColumns = columns.get(1).getSubColumns();
    Assert.assertFalse(subColumns.isEmpty());

    // check sub-columns
    Assert.assertEquals(2, subColumns.size());
    Assert.assertEquals("B", subColumns.get(0).getHeaderTitle());
    Assert.assertTrue(subColumns.get(0).getSubColumns().isEmpty());
    Assert.assertEquals("d", subColumns.get(1).getHeaderTitle());
    Assert.assertTrue(subColumns.get(0).getSubColumns().isEmpty());
  }

  private void setFieldValue(String fieldName, Object fieldValue) throws NoSuchFieldException, IllegalAccessException {
    Field metadataKeyCellsField = config.getClass().getDeclaredField(fieldName);
    metadataKeyCellsField.setAccessible(true);
    metadataKeyCellsField.set(config, fieldValue);
  }
}
