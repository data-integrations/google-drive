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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.drive.source.utils.BodyFormat;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SchemaBuilderTest {

  @Test
  public void testGetImageMediaMetadata() {
    List<String> fields = new ArrayList<>();
    fields.add(getFullImageName(SchemaBuilder.IMAGE_WIDTH_FIELD_NAME));
    fields.add(getFullImageName(SchemaBuilder.IMAGE_CAMERA_MODEL_FIELD_NAME));
    fields.add(getFullImageName(SchemaBuilder.IMAGE_APERTURE_FIELD_NAME));
    fields.add(getFullImageName(SchemaBuilder.IMAGE_FLASH_USED_FIELD_NAME));
    fields.add(getFullImageLocationName(SchemaBuilder.IMAGE_LATITUDE_FIELD_NAME));
    fields.add(getFullImageLocationName(SchemaBuilder.IMAGE_LONGITUDE_FIELD_NAME));

    Schema schema = SchemaBuilder.buildSchema(fields, BodyFormat.BYTES);

    // expected fields for body and offset and record for all image metadata fields
    assertEquals(3, schema.getFields().size());
    testField(schema, Schema.Type.BYTES, SchemaBuilder.BODY_FIELD_NAME, false);
    testField(schema, Schema.Type.LONG, SchemaBuilder.OFFSET_FIELD_NAME, false);
    testField(schema, Schema.Type.RECORD, SchemaBuilder.IMAGE_METADATA_FIELD_NAME, true);

    Schema imageMetadataRecord = schema.getField(SchemaBuilder.IMAGE_METADATA_FIELD_NAME)
      .getSchema().getNonNullable();

    assertEquals(5, imageMetadataRecord.getFields().size());
    testField(imageMetadataRecord, Schema.Type.INT, SchemaBuilder.IMAGE_WIDTH_FIELD_NAME, true);
    testField(imageMetadataRecord, Schema.Type.STRING, SchemaBuilder.IMAGE_CAMERA_MODEL_FIELD_NAME, true);
    testField(imageMetadataRecord, Schema.Type.FLOAT, SchemaBuilder.IMAGE_APERTURE_FIELD_NAME, true);
    testField(imageMetadataRecord, Schema.Type.BOOLEAN, SchemaBuilder.IMAGE_FLASH_USED_FIELD_NAME, true);
    testField(imageMetadataRecord, Schema.Type.RECORD, SchemaBuilder.LOCATION_FIELD_NAME, true);

    Schema locationRecord = imageMetadataRecord.getField(SchemaBuilder.LOCATION_FIELD_NAME)
      .getSchema().getNonNullable();

    assertEquals(2, locationRecord.getFields().size());
    testField(locationRecord, Schema.Type.DOUBLE, SchemaBuilder.IMAGE_LATITUDE_FIELD_NAME, true);
    testField(locationRecord, Schema.Type.DOUBLE, SchemaBuilder.IMAGE_LONGITUDE_FIELD_NAME, true);
  }

  @Test
  public void testGetVideoMediaMetadata() {
    List<String> fields = new ArrayList<>();
    fields.add(getFullVideoName(SchemaBuilder.VIDEO_WIDTH_FIELD_NAME));
    fields.add(getFullVideoName(SchemaBuilder.VIDEO_HEIGHT_FIELD_NAME));
    fields.add(getFullVideoName(SchemaBuilder.VIDEO_DURATION_MILLIS_FIELD_NAME));

    Schema schema = SchemaBuilder.buildSchema(fields, BodyFormat.BYTES);

    // expected fields for body and offset and record for all video metadata fields
    assertEquals(3, schema.getFields().size());
    testField(schema, Schema.Type.BYTES, SchemaBuilder.BODY_FIELD_NAME, false);
    testField(schema, Schema.Type.LONG, SchemaBuilder.OFFSET_FIELD_NAME, false);
    testField(schema, Schema.Type.RECORD, SchemaBuilder.VIDEO_METADATA_FIELD_NAME, true);

    Schema videoMetadataRecord = schema.getField(SchemaBuilder.VIDEO_METADATA_FIELD_NAME)
      .getSchema().getNonNullable();

    assertEquals(3, videoMetadataRecord.getFields().size());
    testField(videoMetadataRecord, Schema.Type.INT, SchemaBuilder.VIDEO_WIDTH_FIELD_NAME, true);
    testField(videoMetadataRecord, Schema.Type.INT, SchemaBuilder.VIDEO_HEIGHT_FIELD_NAME, true);
    testField(videoMetadataRecord, Schema.Type.LONG, SchemaBuilder.VIDEO_DURATION_MILLIS_FIELD_NAME, true);
  }

  @Test
  public void testGeneralMetadata() {
    List<String> fields = new ArrayList<>();
    fields.add(SchemaBuilder.ID_FIELD_NAME);
    fields.add(getFullImageName(SchemaBuilder.IMAGE_WIDTH_FIELD_NAME));
    fields.add(getFullImageLocationName(SchemaBuilder.IMAGE_LATITUDE_FIELD_NAME));
    fields.add(getFullVideoName(SchemaBuilder.VIDEO_HEIGHT_FIELD_NAME));

    Schema schema = SchemaBuilder.buildSchema(fields, BodyFormat.BYTES);

    // expected fields for body and offset and record for all video metadata fields
    assertEquals(5, schema.getFields().size());
    testField(schema, Schema.Type.BYTES, SchemaBuilder.BODY_FIELD_NAME, false);
    testField(schema, Schema.Type.LONG, SchemaBuilder.OFFSET_FIELD_NAME, false);
    testField(schema, Schema.Type.STRING, SchemaBuilder.ID_FIELD_NAME, false);
    testField(schema, Schema.Type.RECORD, SchemaBuilder.IMAGE_METADATA_FIELD_NAME, true);
    testField(schema, Schema.Type.RECORD, SchemaBuilder.VIDEO_METADATA_FIELD_NAME, true);

    Schema videoMetadataRecord = schema.getField(SchemaBuilder.VIDEO_METADATA_FIELD_NAME)
      .getSchema().getNonNullable();

    assertEquals(1, videoMetadataRecord.getFields().size());
    testField(videoMetadataRecord, Schema.Type.INT, SchemaBuilder.VIDEO_HEIGHT_FIELD_NAME, true);

    Schema imageMetadataRecord = schema.getField(SchemaBuilder.IMAGE_METADATA_FIELD_NAME)
      .getSchema().getNonNullable();

    assertEquals(2, imageMetadataRecord.getFields().size());
    testField(imageMetadataRecord, Schema.Type.INT, SchemaBuilder.IMAGE_WIDTH_FIELD_NAME, true);
    testField(imageMetadataRecord, Schema.Type.RECORD, SchemaBuilder.LOCATION_FIELD_NAME, true);

    Schema locationRecord = imageMetadataRecord.getField(SchemaBuilder.LOCATION_FIELD_NAME)
      .getSchema().getNonNullable();

    assertEquals(1, locationRecord.getFields().size());
    testField(locationRecord, Schema.Type.DOUBLE, SchemaBuilder.IMAGE_LATITUDE_FIELD_NAME, true);
  }

  @Test
  public void testEmptyFields() {
    List<String> fields = new ArrayList<>();

    // bytes body
    Schema schema = SchemaBuilder.buildSchema(fields, BodyFormat.BYTES);

    assertEquals(2, schema.getFields().size());
    testField(schema, Schema.Type.BYTES, SchemaBuilder.BODY_FIELD_NAME, false);
    testField(schema, Schema.Type.LONG, SchemaBuilder.OFFSET_FIELD_NAME, false);

    // string body
    schema = SchemaBuilder.buildSchema(fields, BodyFormat.STRING);

    assertEquals(2, schema.getFields().size());
    testField(schema, Schema.Type.STRING, SchemaBuilder.BODY_FIELD_NAME, false);
    testField(schema, Schema.Type.LONG, SchemaBuilder.OFFSET_FIELD_NAME, false);
  }

  private void testField(Schema schema, Schema.Type requiredType, String fieldName, boolean isNullable) {
    Schema.Field field = schema.getField(fieldName);
    assertNotNull(field);
    if (isNullable) {
      assertNotNull(field.getSchema().getUnionSchemas());
      assertEquals(requiredType, field.getSchema().getNonNullable().getType());
    } else {
      assertEquals(requiredType, field.getSchema().getType());
    }
    assertEquals(isNullable, field.getSchema().isNullable());
  }

  public static String getFullImageName(String fieldName) {
    return SchemaBuilder.IMAGE_METADATA_FIELD_NAME + "." + fieldName;
  }

  public static String getFullVideoName(String fieldName) {
    return SchemaBuilder.VIDEO_METADATA_FIELD_NAME + "." + fieldName;
  }

  public static String getFullImageLocationName(String fieldName) {
    return SchemaBuilder.IMAGE_METADATA_FIELD_NAME + "." + SchemaBuilder.LOCATION_FIELD_NAME + "." + fieldName;
  }
}
