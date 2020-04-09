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

package io.cdap.plugin.google.sheets.source.utils;

import com.google.api.services.sheets.v4.model.CellData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Recursive wrapper for headered cells and headered sub-cells. All inner cells represents a several rows.
 */
public class ComplexMultiValueColumn {
  private List<CellData> data = new ArrayList<>();
  private Map<String, ComplexMultiValueColumn> subColumns = new HashMap<>();

  public ComplexMultiValueColumn() {
  }

  public ComplexMultiValueColumn(List<CellData> data) {
    this.data = data;
  }

  public ComplexMultiValueColumn(Map<String, ComplexMultiValueColumn> subColumns) {
    this.subColumns = subColumns;
  }

  public List<CellData> getData() {
    return data;
  }

  public void addData(CellData cellData) {
    data.add(cellData);
  }

  public Map<String, ComplexMultiValueColumn> getSubColumns() {
    return subColumns;
  }

  public void addSubColumn(String subHeaderName, ComplexMultiValueColumn subColumn) {
    this.subColumns.put(subHeaderName, subColumn);
  }

  public void setSubColumns(Map<String, ComplexMultiValueColumn> subColumns) {
    this.subColumns = subColumns;
  }
}
