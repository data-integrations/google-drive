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

import com.google.api.client.util.DateTime;
import com.google.api.services.drive.model.File;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.google.drive.common.FileFromFolder;
import io.cdap.plugin.google.drive.source.utils.BodyFormat;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class FilesFromFolderTransformerTest {
  private static final byte[] TEST_BYTES = new byte[]{-6, 67, 101, -65, -9};
  private static final Long TEST_OFFSET = 34L;
  private static final String TEST_ID = "FDGtewSvftr5r";
  private static final Map<String, String> TEST_PROPERTIES = new HashMap<String, String>() {{
    put("p0", "v0");
    put("p1", "v1");
    put("p2", "v2");
  }};
  private static final Double TEST_LATITUDE = 0.567;
  private static final Integer TEST_WIDTH = 640;
  private static final Long TEST_DURATION = 640L;
  private static final List<String> TEST_PARENTS = Arrays.asList("435435fe", "ergfvreg");
  private static final DateTime TEST_CREATED_TIME = new DateTime(42L);

  @Test
  public void testTransform() {
    List<String> fields = new ArrayList<>();
    fields.add(SchemaBuilder.ID_FIELD_NAME);
    fields.add(SchemaBuilder.PROPERTIES_FIELD_NAME);
    fields.add(SchemaBuilderTest.getFullImageName(SchemaBuilder.IMAGE_WIDTH_FIELD_NAME));
    fields.add(SchemaBuilderTest.getFullImageLocationName(SchemaBuilder.IMAGE_LATITUDE_FIELD_NAME));
    fields.add(SchemaBuilderTest.getFullVideoName(SchemaBuilder.VIDEO_DURATION_MILLIS_FIELD_NAME));
    fields.add(SchemaBuilder.PARENTS_FIELD_NAME);
    fields.add(SchemaBuilder.CREATED_TIME_FIELD_NAME);

    Schema schema = SchemaBuilder.buildSchema(fields, BodyFormat.BYTES);

    File file = new File();
    file.setId(TEST_ID);
    file.setProperties(TEST_PROPERTIES);
    file.setParents(TEST_PARENTS);
    file.setCreatedTime(TEST_CREATED_TIME);

    File.ImageMediaMetadata.Location location = new File.ImageMediaMetadata.Location();
    location.setLatitude(TEST_LATITUDE);
    File.ImageMediaMetadata imageMediaMetadata = new File.ImageMediaMetadata();
    imageMediaMetadata.setWidth(TEST_WIDTH);
    imageMediaMetadata.setLocation(location);

    file.setImageMediaMetadata(imageMediaMetadata);

    File.VideoMediaMetadata videoMediaMetadata = new File.VideoMediaMetadata();
    videoMediaMetadata.setDurationMillis(TEST_DURATION);

    file.setVideoMediaMetadata(videoMediaMetadata);

    FileFromFolder fileFromFolder = new FileFromFolder(TEST_BYTES, TEST_OFFSET, file);

    StructuredRecord record = FilesFromFolderTransformer.transform(fileFromFolder, schema);

    assertNotNull(record.get(SchemaBuilder.ID_FIELD_NAME));
    assertNotNull(record.get(SchemaBuilder.BODY_FIELD_NAME));
    assertNotNull(record.get(SchemaBuilder.OFFSET_FIELD_NAME));

    Map<String, String> retrievedProperties = record.get(SchemaBuilder.PROPERTIES_FIELD_NAME);
    assertNotNull(retrievedProperties);
    assertEquals(TEST_PROPERTIES, retrievedProperties);

    StructuredRecord imageMetadataResult = record.get(SchemaBuilder.IMAGE_METADATA_FIELD_NAME);
    assertNotNull(imageMetadataResult);
    assertEquals(2, imageMetadataResult.getSchema().getFields().size());
    assertEquals(TEST_WIDTH, imageMetadataResult.get(SchemaBuilder.IMAGE_WIDTH_FIELD_NAME));

    StructuredRecord locationResult =
            imageMetadataResult.get(SchemaBuilder.LOCATION_FIELD_NAME);
    assertEquals(TEST_LATITUDE, locationResult.get(SchemaBuilder.IMAGE_LATITUDE_FIELD_NAME));

    StructuredRecord videoMetadataResult = record.get(SchemaBuilder.VIDEO_METADATA_FIELD_NAME);
    assertNotNull(videoMetadataResult);
    assertEquals(TEST_DURATION, videoMetadataResult.get(SchemaBuilder.VIDEO_DURATION_MILLIS_FIELD_NAME));

    List<String> parents = record.get(SchemaBuilder.PARENTS_FIELD_NAME);
    assertNotNull(parents);
    assertEquals(TEST_PARENTS, parents);

    Long resultCreatedTime = record.get(SchemaBuilder.CREATED_TIME_FIELD_NAME);
    assertNotNull(resultCreatedTime);
    assertEquals((Long) TEST_CREATED_TIME.getValue(), resultCreatedTime);
  }
}
