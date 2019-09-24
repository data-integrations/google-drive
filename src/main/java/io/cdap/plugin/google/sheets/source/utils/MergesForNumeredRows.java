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

import java.util.List;
import java.util.Map;

/**
 * Wrapper for list of single data rows with merge ranges that relate to this rows.
 */
public class MergesForNumeredRows {
  private List<GridRange> mergeRanges;
  private Map<Integer, List<CellData>> numeredRows;

  public MergesForNumeredRows(List<GridRange> mergeRanges, Map<Integer, List<CellData>> numeredRows) {
    this.mergeRanges = mergeRanges;
    this.numeredRows = numeredRows;
  }

  public List<GridRange> getMergeRanges() {
    return mergeRanges;
  }

  public Map<Integer, List<CellData>> getNumeredRows() {
    return numeredRows;
  }
}
