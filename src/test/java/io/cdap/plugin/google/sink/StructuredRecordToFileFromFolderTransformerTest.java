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

package io.cdap.plugin.google.sink;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.common.FileFromFolder;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StructuredRecordToFileFromFolderTransformerTest {
  private static final String TEST_BODY_FIELD_NAME = "body";
  private static final String TEST_NAME_FIELD_NAME = "name";
  private static final String TEST_MIME_FIELD_NAME = "mimeType";

  private static final byte[] TEST_BYTES = new byte[]{-6, 67, 101, -65, -9};
  private static final String TEST_NAME = "testName";
  private static final String TEST_MIME = "text/plane";

  @Test
  public void testBodyOnlyRecord() {
    Schema schema =
      Schema.recordOf("FileFromFolder",
                      Schema.Field.of(TEST_BODY_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    recordBuilder.set(TEST_BODY_FIELD_NAME, TEST_BYTES);
    StructuredRecord record = recordBuilder.build();

    StructuredRecordToFileFromFolderTransformer transformer =
      new StructuredRecordToFileFromFolderTransformer(TEST_BODY_FIELD_NAME, "", "");
    FileFromFolder fileFromFolder = transformer.transform(record);

    assertNotNull(fileFromFolder.getFile());
    assertEquals((long) StructuredRecordToFileFromFolderTransformer.RANDOM_FILE_NAME_LENGTH,
      fileFromFolder.getFile().getName().length());
    assertEquals(TEST_BYTES, fileFromFolder.getContent());
    assertEquals(null, fileFromFolder.getFile().getMimeType());
    assertEquals(0, fileFromFolder.getOffset());
  }

  @Test
  public void testEmptyRecord() {
    Schema schema =
      Schema.recordOf("FileFromFolder",
                      Schema.Field.of(TEST_BODY_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    StructuredRecord record = recordBuilder.build();

    StructuredRecordToFileFromFolderTransformer transformer =
      new StructuredRecordToFileFromFolderTransformer(TEST_BODY_FIELD_NAME, "", "");
    FileFromFolder fileFromFolder = transformer.transform(record);

    assertNotNull(fileFromFolder.getFile());
    assertEquals((long) StructuredRecordToFileFromFolderTransformer.RANDOM_FILE_NAME_LENGTH,
      fileFromFolder.getFile().getName().length());
    assertTrue(Arrays.equals(new byte[]{}, fileFromFolder.getContent()));
    assertEquals(null, fileFromFolder.getFile().getMimeType());
    assertEquals(0, fileFromFolder.getOffset());
  }

  @Test
  public void testFullRecord() {
    Schema schema =
      Schema.recordOf("FileFromFolder",
                      Schema.Field.of(TEST_BODY_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                      Schema.Field.of(TEST_NAME_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of(TEST_MIME_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    recordBuilder.set(TEST_BODY_FIELD_NAME, TEST_BYTES);
    recordBuilder.set(TEST_NAME_FIELD_NAME, TEST_NAME);
    recordBuilder.set(TEST_MIME_FIELD_NAME, TEST_MIME);
    StructuredRecord record = recordBuilder.build();

    StructuredRecordToFileFromFolderTransformer transformer =
      new StructuredRecordToFileFromFolderTransformer(TEST_BODY_FIELD_NAME, TEST_NAME_FIELD_NAME, TEST_MIME_FIELD_NAME);
    FileFromFolder fileFromFolder = transformer.transform(record);

    assertNotNull(fileFromFolder.getFile());
    assertEquals(TEST_BYTES, fileFromFolder.getContent());
    assertEquals(TEST_MIME, fileFromFolder.getFile().getMimeType());
    assertEquals(TEST_NAME, fileFromFolder.getFile().getName());
    assertEquals(0, fileFromFolder.getOffset());
  }
}
