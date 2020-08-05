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

/**
 * Util class that converts sheet column names into column index and back.
 */
public class ColumnAddressConverter {
  private static final int NUMBER_OF_LETTERS = 26;
  private static final int DECIMAL_OF_A_CHAR = 64;

  /**
   * Method that converts index of column (it starts from 1) into name ('A', 'AC' etc.).
   * @param index index of the column.
   * @return name of the column.
   */
  public static String getColumnName(int index) {
    StringBuilder columnName = new StringBuilder();
    while (index > 0) {
      // Find remainder
      int rem = index % NUMBER_OF_LETTERS;

      // If remainder is 0, then a
      // 'Z' must be there in output
      if (rem == 0) {
        columnName.append("Z");
        index = (index / NUMBER_OF_LETTERS) - 1;
      } else {
        columnName.append((char) ((rem - 1) + 'A'));
        index = index / NUMBER_OF_LETTERS;
      }
    }

    // Reverse the string and print result
    return columnName.reverse().toString();
  }

  /**
   * Method that returns index of column (it starts from 1) for it name ('A', 'AC' etc.).
   * @param columnName name of the column.
   * @return index of the column (starts from 1).
   */
  public static int getNumberOfColumn(String columnName) {
    int sum = 0;
    for (int i = 0; i < columnName.length(); ++i) {
      int representation = columnName.charAt(i);
      representation -= DECIMAL_OF_A_CHAR;
      int value = (int) (representation * Math.pow(NUMBER_OF_LETTERS, columnName.length() - i - 1));
      sum += value;
    }
    return sum;
  }
}
