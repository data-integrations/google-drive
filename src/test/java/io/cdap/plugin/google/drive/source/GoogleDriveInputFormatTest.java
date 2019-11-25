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

package io.cdap.plugin.google.drive.source;

import com.google.api.services.drive.model.File;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GoogleDriveInputFormatTest {

  private static final String FILE_NAME_1 = "a";
  private static final String FILE_NAME_2 = "b";
  private static final String FILE_NAME_3 = "c";

  private static GoogleDriveInputFormat googleDriveInputFormat;
  private static Method getSplitsFromFiles;

  @BeforeClass
  public static void prepareTests() throws NoSuchMethodException {
    googleDriveInputFormat = new GoogleDriveInputFormat();
    getSplitsFromFiles = GoogleDriveInputFormat.class
      .getDeclaredMethod("getSplitsFromFiles", List.class, Long.class);
    getSplitsFromFiles.setAccessible(true);
  }

  @Test
  public void testGetSplitsUnlimited() throws InvocationTargetException, IllegalAccessException {
    List<GoogleDriveSplit> splits =
      (List<GoogleDriveSplit>) getSplitsFromFiles.invoke(googleDriveInputFormat, getTestFiles(), 0L);
    Map<String, List<GoogleDriveSplit>> splitsById = splits.stream()
      .collect(Collectors.groupingBy(GoogleDriveSplit::getFileId));
    assertEquals(3L, splitsById.size());

    assertEquals(1, splitsById.get(FILE_NAME_1).size());
    assertEquals(1, splitsById.get(FILE_NAME_2).size());
    assertEquals(1, splitsById.get(FILE_NAME_3).size());

    assertEquals(0, splitsById.get(FILE_NAME_1).get(0).getBytesFrom());
    assertEquals(0, splitsById.get(FILE_NAME_1).get(0).getBytesTo());
    assertFalse(splitsById.get(FILE_NAME_1).get(0).isPartitioned());

    assertEquals(0, splitsById.get(FILE_NAME_2).get(0).getBytesFrom());
    assertEquals(0, splitsById.get(FILE_NAME_2).get(0).getBytesTo());
    assertFalse(splitsById.get(FILE_NAME_2).get(0).isPartitioned());

    assertEquals(0, splitsById.get(FILE_NAME_2).get(0).getBytesFrom());
    assertEquals(0, splitsById.get(FILE_NAME_2).get(0).getBytesTo());
    assertFalse(splitsById.get(FILE_NAME_2).get(0).isPartitioned());
  }

  @Test
  public void testGetSplitsLimited() throws InvocationTargetException, IllegalAccessException {
    List<GoogleDriveSplit> splits =
      (List<GoogleDriveSplit>) getSplitsFromFiles.invoke(googleDriveInputFormat, getTestFiles(), 50L);
    Map<String, List<GoogleDriveSplit>> splitsById = splits.stream()
      .collect(Collectors.groupingBy(GoogleDriveSplit::getFileId));
    assertEquals(3L, splitsById.size());

    // Biggest file should be separated to two splits
    assertEquals(2, splitsById.get(FILE_NAME_1).size());
    assertEquals(1, splitsById.get(FILE_NAME_2).size());
    assertEquals(1, splitsById.get(FILE_NAME_3).size());

    assertEquals(0, splitsById.get(FILE_NAME_1).get(0).getBytesFrom());
    assertEquals(49, splitsById.get(FILE_NAME_1).get(0).getBytesTo());
    assertTrue(splitsById.get(FILE_NAME_1).get(0).isPartitioned());
    assertEquals(50, splitsById.get(FILE_NAME_1).get(1).getBytesFrom());
    assertEquals(50, splitsById.get(FILE_NAME_1).get(1).getBytesTo());
    assertTrue(splitsById.get(FILE_NAME_1).get(1).isPartitioned());

    assertEquals(0, splitsById.get(FILE_NAME_2).get(0).getBytesFrom());
    assertEquals(0, splitsById.get(FILE_NAME_2).get(0).getBytesTo());
    assertFalse(splitsById.get(FILE_NAME_2).get(0).isPartitioned());

    assertEquals(0, splitsById.get(FILE_NAME_3).get(0).getBytesFrom());
    assertEquals(0, splitsById.get(FILE_NAME_3).get(0).getBytesTo());
    assertFalse(splitsById.get(FILE_NAME_3).get(0).isPartitioned());
  }

  private List<File> getTestFiles() {
    File file0 = new File();
    file0.setSize(51L);
    file0.setId(FILE_NAME_1);

    File file1 = new File();
    file1.setSize(50L);
    file1.setId(FILE_NAME_2);

    File file2 = new File();
    file2.setSize(null);
    file2.setId(FILE_NAME_3);
    return new ArrayList<File>() {{
      add(file0);
      add(file1);
      add(file2);
    }};
  }
}
