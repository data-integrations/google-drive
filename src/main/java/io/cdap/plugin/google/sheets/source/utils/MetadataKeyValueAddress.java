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

import java.util.StringJoiner;

/**
 * Represents coordinates for metadata key and value cells.
 */
public class MetadataKeyValueAddress {
  private final CellCoordinate nameCoordinate;
  private final CellCoordinate valueCoordinate;

  public MetadataKeyValueAddress(CellCoordinate nameCoordinate, CellCoordinate valueCoordinate) {
    this.nameCoordinate = nameCoordinate;
    this.valueCoordinate = valueCoordinate;
  }

  public CellCoordinate getNameCoordinate() {
    return nameCoordinate;
  }

  public CellCoordinate getValueCoordinate() {
    return valueCoordinate;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetadataKeyValueAddress that = (MetadataKeyValueAddress) o;
    return Objects.equal(nameCoordinate, that.nameCoordinate) &&
        Objects.equal(valueCoordinate, that.valueCoordinate);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nameCoordinate, valueCoordinate);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", MetadataKeyValueAddress.class.getSimpleName() + "[", "]")
        .add("nameCoordinate=" + nameCoordinate)
        .add("valueCoordinate=" + valueCoordinate)
        .toString();
  }
}
