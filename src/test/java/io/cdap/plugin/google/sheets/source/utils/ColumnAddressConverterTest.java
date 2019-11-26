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

import org.junit.Assert;
import org.junit.Test;

public class ColumnAddressConverterTest {

  @Test
  public void testGetNumberOfColumn() {
    Assert.assertEquals(1, ColumnAddressConverter.getNumberOfColumn("A"));
    Assert.assertEquals(26, ColumnAddressConverter.getNumberOfColumn("Z"));
    Assert.assertEquals(27, ColumnAddressConverter.getNumberOfColumn("AA"));
    Assert.assertEquals(46, ColumnAddressConverter.getNumberOfColumn("AT"));
  }

  @Test
  public void testGetColumnName() {
    Assert.assertEquals("A", ColumnAddressConverter.getColumnName(1));
    Assert.assertEquals("Z", ColumnAddressConverter.getColumnName(26));
    Assert.assertEquals("AA", ColumnAddressConverter.getColumnName(27));
    Assert.assertEquals("AT", ColumnAddressConverter.getColumnName(46));
  }
}
