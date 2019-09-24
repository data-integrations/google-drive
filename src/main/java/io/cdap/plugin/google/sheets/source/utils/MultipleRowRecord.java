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
import com.google.api.services.sheets.v4.model.GridRange;
import org.apache.commons.collections.MapUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation for several rows data. Row values are saved as recursive structures for nested columns support.
 */
public class MultipleRowRecord {
  private String spreadsheetName;
  private String sheetTitle;
  private Map<String, String> metadata;
  private Map<String, ComplexMultiValueColumn> headeredCells;
  private List<GridRange> merges;

  /**
   * Constructor for MultipleRowRecord object.
   * @param spreadsheetName The spread sheet name
   * @param sheetTitle The sheet title
   * @param metadata The metadata
   * @param headeredCells The headered cells
   * @param merges The merges
   */
  public MultipleRowRecord(String spreadsheetName, String sheetTitle, Map<String, String> metadata,
                           Map<String, ComplexMultiValueColumn> headeredCells, List<GridRange> merges) {
    this.spreadsheetName = spreadsheetName;
    this.sheetTitle = sheetTitle;
    this.metadata = metadata;
    this.headeredCells = headeredCells;
    this.merges = merges;
  }

  public String getSpreadsheetName() {
    return spreadsheetName;
  }

  public String getSheetTitle() {
    return sheetTitle;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public Map<String, ComplexMultiValueColumn> getHeaderedCells() {
    return headeredCells;
  }

  public List<GridRange> getMerges() {
    return merges;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  /**
   * Returns the instance of RowRecord.
   * @param index The index is int type
   * @return The instance of RowRecord
   */
  public RowRecord getRowRecord(int index) {
    Map<String, ComplexSingleValueColumn> row = new HashMap<>();
    boolean isEmptyRow = convertToSingleRow(headeredCells, row, index);
    return new RowRecord(spreadsheetName, sheetTitle, metadata, row, isEmptyRow);
  }

  private boolean convertToSingleRow(Map<String, ComplexMultiValueColumn> headeredCells,
                                     Map<String, ComplexSingleValueColumn> resultRow,
                                     int index) {
    boolean isEmptyRow = true;
    for (Map.Entry<String, ComplexMultiValueColumn> headeredCellsEntry : headeredCells.entrySet()) {
      String headerName = headeredCellsEntry.getKey();
      ComplexMultiValueColumn headerValue = headeredCellsEntry.getValue();
      ComplexSingleValueColumn singleValueColumn = new ComplexSingleValueColumn();

      if (MapUtils.isEmpty(headerValue.getSubColumns())) {
        CellData cellData;
        if (headerValue.getData().size() > index) {
          cellData = headerValue.getData().get(index);
          isEmptyRow = cellData == null;
        } else {
          cellData = null;
        }
        singleValueColumn.setData(cellData);
      } else {
        Map<String, ComplexSingleValueColumn> subRow = new HashMap<>();
        if (!convertToSingleRow(headerValue.getSubColumns(), subRow, index)) {
          isEmptyRow = false;
        }
        singleValueColumn.setSubColumns(subRow);
      }
      resultRow.put(headerName, singleValueColumn);
    }
    return isEmptyRow;
  }
}
