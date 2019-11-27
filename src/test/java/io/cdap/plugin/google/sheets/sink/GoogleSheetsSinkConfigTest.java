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

package io.cdap.plugin.google.sheets.sink;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.validation.DefaultFailureCollector;
import io.cdap.plugin.google.sheets.sink.utils.DimensionType;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

public class GoogleSheetsSinkConfigTest {
  private static final String SCHEMA_NAME = "default";
  private static final String TEST_RECORD_FIELD_NAME = "testRecord";
  private static final String TEST_SUB_RECORD_FIELD_NAME = "testSubRecord1";
  private static final String TEST_RECORD_SUB_1_FIELD_NAME = "string";
  private static final String TEST_RECORD_SUB_2_FIELD_NAME = "boolean";
  private static final String TEST_RECORD_SUB_3_FIELD_NAME = "double";
  private static final String TEST_SUB_RECORD_SUB_1_FIELD_NAME = "string1";
  private static final String TEST_ARRAY_FIELD_NAME = "testArray";
  private static final String TEST_SUB_ARRAY_RECORD_FIELD_NAME = "testSubRecord2";
  private static final String TEST_SUB_ARRAY_SUB_1_FIELD_NAME = "string2";
  private static final String TEST_MAP_FIELD_NAME = "testMap";
  private static final String TEST_ENUM_FIELD_NAME = "testEnum";

  @Test
  public void testValidSchema() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    GoogleSheetsSinkConfig sinkConfig = new GoogleSheetsSinkConfig();
    Method validateSchemaMethod = GoogleSheetsSinkConfig.class.getDeclaredMethod("validateSchema",
      FailureCollector.class, Schema.class);
    validateSchemaMethod.setAccessible(true);

    FailureCollector collector = new DefaultFailureCollector("", Collections.EMPTY_MAP);
    Schema testValidSchema = Schema.recordOf(SCHEMA_NAME,
      Schema.Field.of(TEST_RECORD_FIELD_NAME, Schema.recordOf(
        TEST_RECORD_FIELD_NAME,
        Schema.Field.of(TEST_RECORD_SUB_1_FIELD_NAME, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(TEST_RECORD_SUB_2_FIELD_NAME, Schema.of(Schema.Type.BOOLEAN)),
        Schema.Field.of(TEST_RECORD_SUB_3_FIELD_NAME, Schema.of(Schema.Type.DOUBLE))
      )),
      Schema.Field.of(TEST_ARRAY_FIELD_NAME, Schema.arrayOf(
        Schema.of(Schema.Type.STRING)
      )),
      Schema.Field.of("booleanField", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("stringField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("bytesField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("intField", Schema.of(Schema.Type.INT)),
      Schema.Field.of("longField", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("doubleField", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("floatField", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("nullfield", Schema.of(Schema.Type.NULL))
    );

    validateSchemaMethod.invoke(sinkConfig, collector, testValidSchema);

    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testInvalidSchema() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    GoogleSheetsSinkConfig sinkConfig = new GoogleSheetsSinkConfig();
    Method validateSchemaMethod = GoogleSheetsSinkConfig.class.getDeclaredMethod("validateSchema",
      FailureCollector.class, Schema.class);
    validateSchemaMethod.setAccessible(true);

    FailureCollector collector = new DefaultFailureCollector("", Collections.EMPTY_MAP);
    Schema testInvalidSchema = Schema.recordOf(SCHEMA_NAME,
      Schema.Field.of(TEST_RECORD_FIELD_NAME, Schema.recordOf(
        TEST_RECORD_FIELD_NAME,
        Schema.Field.of(TEST_RECORD_SUB_1_FIELD_NAME, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(TEST_RECORD_SUB_2_FIELD_NAME, Schema.of(Schema.Type.BOOLEAN)),
        Schema.Field.of(TEST_RECORD_SUB_3_FIELD_NAME, Schema.recordOf(TEST_SUB_RECORD_FIELD_NAME,
          Schema.Field.of(TEST_SUB_RECORD_SUB_1_FIELD_NAME, Schema.of(Schema.Type.STRING))))
      )),
      Schema.Field.of(TEST_ARRAY_FIELD_NAME, Schema.arrayOf(
        Schema.recordOf(TEST_SUB_ARRAY_RECORD_FIELD_NAME,
          Schema.Field.of(TEST_SUB_ARRAY_SUB_1_FIELD_NAME, Schema.of(Schema.Type.STRING)))
      )),
      Schema.Field.of(TEST_MAP_FIELD_NAME, Schema.mapOf(
        Schema.of(Schema.Type.STRING),
        Schema.of(Schema.Type.STRING)
      )),
      Schema.Field.of(TEST_ENUM_FIELD_NAME, Schema.enumWith(
        DimensionType.COLUMNS.getValue(),
        DimensionType.ROWS.getValue()
      )));

    validateSchemaMethod.invoke(sinkConfig, collector, testInvalidSchema);

    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(4, failures.size());
    Assert.assertEquals(String.format(GoogleSheetsSinkConfig.RECORD_FIELD_SCHEMA_MESSAGE,
      TEST_RECORD_FIELD_NAME, TEST_RECORD_SUB_3_FIELD_NAME, Schema.Type.RECORD, null), failures.get(0).getMessage());
    Assert.assertEquals(String.format(GoogleSheetsSinkConfig.ARRAY_COMPONENTS_SCHEMA_MESSAGE,
      TEST_ARRAY_FIELD_NAME, Schema.Type.RECORD, null), failures.get(1).getMessage());
    Assert.assertEquals(String.format(GoogleSheetsSinkConfig.TOP_LEVEL_SCHEMA_MESSAGE,
      TEST_MAP_FIELD_NAME, Schema.Type.MAP, null), failures.get(2).getMessage());
    Assert.assertEquals(String.format(GoogleSheetsSinkConfig.TOP_LEVEL_SCHEMA_MESSAGE,
      TEST_ENUM_FIELD_NAME, Schema.Type.ENUM, null), failures.get(3).getMessage());
  }
}
