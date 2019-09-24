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

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.StringJoiner;

/**
 * Represents coordinates (numbers of row and column) of a cell.
 */
public final class CellCoordinate implements Serializable {
  private final int rowNumber;
  private final int columnNumber;

  public CellCoordinate(int rowNumber, int columnNumber) {
    this.rowNumber = rowNumber;
    this.columnNumber = columnNumber;
  }

  public int getRowNumber() {
    return rowNumber;
  }

  public int getColumnNumber() {
    return columnNumber;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CellCoordinate that = (CellCoordinate) o;
    return rowNumber == that.rowNumber &&
        columnNumber == that.columnNumber;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(rowNumber, columnNumber);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", CellCoordinate.class.getSimpleName() + "[", "]")
        .add("rowNumber=" + rowNumber)
        .add("columnNumber=" + columnNumber)
        .toString();
  }
}
