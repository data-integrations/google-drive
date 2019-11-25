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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Util class for building pipeline schema.
 */
public class SchemaBuilder {
  public static final String SCHEMA_ROOT_RECORD_NAME = "FileFromFolder";

  public static final String IMAGE_METADATA_FIELD_NAME = "imageMediaMetadata";
  public static final String VIDEO_METADATA_FIELD_NAME = "videoMediaMetadata";
  public static final String LOCATION_FIELD_NAME = "location";
  public static final String IMAGE_METADATA_NAME_PREFIX = IMAGE_METADATA_FIELD_NAME + ".";
  public static final String VIDEO_METADATA_NAME_PREFIX = VIDEO_METADATA_FIELD_NAME + ".";
  public static final String LOCATION_NAME_PREFIX = LOCATION_FIELD_NAME + ".";
  public static final String IMAGE_METADATA_LOCATION_FIELD_NAME = IMAGE_METADATA_NAME_PREFIX + LOCATION_FIELD_NAME;
  public static final String IMAGE_METADATA_LOCATION_FIELD_NAME_PREFIX = IMAGE_METADATA_LOCATION_FIELD_NAME + ".";

  public static final String BODY_FIELD_NAME = "body";
  public static final String OFFSET_FIELD_NAME = "offset";
  public static final String ID_FIELD_NAME = "id";
  public static final String NAME_FIELD_NAME = "name";
  public static final String MIME_TYPE_FIELD_NAME = "mimeType";
  public static final String DESCRIPTION_FIELD_NAME = "description";
  public static final String DRIVE_ID_FIELD_NAME = "driveId";
  public static final String ORIGINAL_FILENAME_FIELD_NAME = "originalFilename";
  public static final String FULL_FILE_EXTENSION_FIELD_NAME = "fullFileExtension";
  public static final String MD_5_CHECKSUM_FIELD_NAME = "md5Checksum";
  public static final String SIZE_FIELD_NAME = "size";
  public static final String STARRED_FIELD_NAME = "starred";
  public static final String TRASHED_FIELD_NAME = "trashed";
  public static final String EXPLICITLY_TRASHED_FIELD_NAME = "explicitlyTrashed";
  public static final String TRASHED_TIME_FIELD_NAME = "trashedTime";
  public static final String CREATED_TIME_FIELD_NAME = "createdTime";
  public static final String MODIFIED_TIME_FIELD_NAME = "modifiedTime";
  public static final String PARENTS_FIELD_NAME = "parents";
  public static final String SPACES_FIELD_NAME = "spaces";
  public static final String PROPERTIES_FIELD_NAME = "properties";
  public static final String IMAGE_WIDTH_FIELD_NAME = "width";
  public static final String IMAGE_HEIGHT_FIELD_NAME = "height";
  public static final String IMAGE_ROTATION_FIELD_NAME = "rotation";
  public static final String IMAGE_ISO_SPEED_FIELD_NAME = "isoSpeed";
  public static final String IMAGE_SUBJECT_DISTANCE_FIELD_NAME = "subjectDistance";
  public static final String IMAGE_TIME_FIELD_NAME = "time";
  public static final String IMAGE_CAMERA_MAKE_FIELD_NAME = "cameraMake";
  public static final String IMAGE_CAMERA_MODEL_FIELD_NAME = "cameraModel";
  public static final String IMAGE_METERING_MODE_FIELD_NAME = "meteringMode";
  public static final String IMAGE_SENSOR_FIELD_NAME = "sensor";
  public static final String IMAGE_EXPOSURE_MODE_FIELD_NAME = "exposureMode";
  public static final String IMAGE_COLOR_SPACE_FIELD_NAME = "colorSpace";
  public static final String IMAGE_WHITE_BALANCE_FIELD_NAME = "whiteBalance";
  public static final String IMAGE_LENS_FIELD_NAME = "lens";
  public static final String IMAGE_EXPOSURE_TIME_FIELD_NAME = "exposureTime";
  public static final String IMAGE_APERTURE_FIELD_NAME = "aperture";
  public static final String IMAGE_FOCAL_LENGTH_FIELD_NAME = "focalLength";
  public static final String IMAGE_EXPOSURE_BIAS_FIELD_NAME = "exposureBias";
  public static final String IMAGE_MAX_APERTURE_VALUE_FIELD_NAME = "maxApertureValue";
  public static final String IMAGE_FLASH_USED_FIELD_NAME = "flashUsed";
  public static final String VIDEO_WIDTH_FIELD_NAME = "width";
  public static final String VIDEO_HEIGHT_FIELD_NAME = "height";
  public static final String VIDEO_DURATION_MILLIS_FIELD_NAME = "durationMillis";
  public static final String IMAGE_LATITUDE_FIELD_NAME = "latitude";
  public static final String IMAGE_LONGITUDE_FIELD_NAME = "longitude";
  public static final String IMAGE_ALTITUDE_FIELD_NAME = "altitude";

  public static Schema buildSchema(List<String> fields, BodyFormat bodyFormat) {
    List<String> extendedFields = new ArrayList<>(fields);
    extendedFields.add(BODY_FIELD_NAME);
    extendedFields.add(OFFSET_FIELD_NAME);
    List<Schema.Field> generalFields =
      extendedFields.stream().map(f -> SchemaBuilder.getTopLevelField(f, bodyFormat))
        .filter(f -> f != null).collect(Collectors.toList());
    processImageMediaMetadata(extendedFields, generalFields);
    processVideoMediaMetadata(extendedFields, generalFields);

    return Schema.recordOf(SCHEMA_ROOT_RECORD_NAME, generalFields);
  }

  public static Schema.Field getTopLevelField(String name, BodyFormat bodyFormat) {
    switch (name) {
      case BODY_FIELD_NAME:
        switch (bodyFormat) {
          case BYTES:
            return Schema.Field.of(name, Schema.of(Schema.Type.BYTES));
          case STRING:
            return Schema.Field.of(name, Schema.of(Schema.Type.STRING));
        }
      case OFFSET_FIELD_NAME:
      case SIZE_FIELD_NAME:
        return Schema.Field.of(name, Schema.of(Schema.Type.LONG));
      case ID_FIELD_NAME:
      case NAME_FIELD_NAME:
      case MIME_TYPE_FIELD_NAME:
      case DRIVE_ID_FIELD_NAME:
      case ORIGINAL_FILENAME_FIELD_NAME:
      case FULL_FILE_EXTENSION_FIELD_NAME:
      case MD_5_CHECKSUM_FIELD_NAME:
        return Schema.Field.of(name, Schema.of(Schema.Type.STRING));
      case DESCRIPTION_FIELD_NAME:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
      case STARRED_FIELD_NAME:
      case TRASHED_FIELD_NAME:
      case EXPLICITLY_TRASHED_FIELD_NAME:
        return Schema.Field.of(name, Schema.of(Schema.Type.BOOLEAN));
      case TRASHED_TIME_FIELD_NAME:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)));
      case CREATED_TIME_FIELD_NAME:
      case MODIFIED_TIME_FIELD_NAME:
        return Schema.Field.of(name, Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS));
      case PARENTS_FIELD_NAME:
      case SPACES_FIELD_NAME:
        return Schema.Field.of(name, Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING))));
      case PROPERTIES_FIELD_NAME:
        return Schema.Field.of(name, Schema.nullableOf(Schema.mapOf(
          Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))));
      default:
        if (!name.startsWith(IMAGE_METADATA_NAME_PREFIX) && !name.startsWith(VIDEO_METADATA_NAME_PREFIX)) {
          throw new IllegalStateException(String.format("Untreated value '%s' for top level field.", name));
        } else {
          return null;
        }
    }
  }

  public static void processImageMediaMetadata(List<String> fields, List<Schema.Field> schemaFields) {
    List<String> imageMediaFields =
      fields.stream().filter(f -> f.startsWith(IMAGE_METADATA_NAME_PREFIX))
        .map(f -> f.substring(IMAGE_METADATA_NAME_PREFIX.length())).collect(Collectors.toList());

    List<String> locationFields =
      fields.stream().filter(f -> f.startsWith(IMAGE_METADATA_LOCATION_FIELD_NAME_PREFIX))
        .map(f -> f.substring(IMAGE_METADATA_LOCATION_FIELD_NAME_PREFIX.length())).collect(Collectors.toList());

    List<Schema.Field> imageMediaFieldsSchemas = imageMediaFields.stream()
      .map(SchemaBuilder::fromImageMediaMetadataName)
      .filter(f -> f != null).collect(Collectors.toList());

    if (!locationFields.isEmpty()) {
      imageMediaFieldsSchemas.add(
        Schema.Field.of(LOCATION_FIELD_NAME,
                        Schema.nullableOf(Schema.recordOf(
                          LOCATION_FIELD_NAME,
                          locationFields.stream().map(SchemaBuilder::fromLocationName).collect(Collectors.toList())))));
    }

    if (!imageMediaFieldsSchemas.isEmpty()) {
      schemaFields.add(Schema.Field.of(IMAGE_METADATA_FIELD_NAME,
                                       Schema.nullableOf(Schema.recordOf(IMAGE_METADATA_FIELD_NAME,
                                                                         imageMediaFieldsSchemas))));
    }
  }

  public static void processVideoMediaMetadata(List<String> fields, List<Schema.Field> schemaFields) {
    List<String> videoMediaFields =
      fields.stream().filter(f -> f.startsWith(VIDEO_METADATA_NAME_PREFIX))
        .map(f -> f.substring(VIDEO_METADATA_NAME_PREFIX.length())).collect(Collectors.toList());

    List<Schema.Field> videoMediaFieldsSchemas = videoMediaFields.stream()
      .map(SchemaBuilder::fromVideoMediaMetadataName)
      .filter(f -> f != null).collect(Collectors.toList());

    if (!videoMediaFieldsSchemas.isEmpty()) {
      schemaFields.add(Schema.Field.of(VIDEO_METADATA_FIELD_NAME,
                                       Schema.nullableOf(Schema.recordOf(VIDEO_METADATA_FIELD_NAME,
                                                                         videoMediaFieldsSchemas))));
    }
  }

  public static Schema.Field fromImageMediaMetadataName(String name) {
    switch (name) {
      case IMAGE_WIDTH_FIELD_NAME:
      case IMAGE_HEIGHT_FIELD_NAME:
      case IMAGE_ROTATION_FIELD_NAME:
      case IMAGE_ISO_SPEED_FIELD_NAME:
      case IMAGE_SUBJECT_DISTANCE_FIELD_NAME:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.INT)));
      case IMAGE_TIME_FIELD_NAME:
      case IMAGE_CAMERA_MAKE_FIELD_NAME:
      case IMAGE_CAMERA_MODEL_FIELD_NAME:
      case IMAGE_METERING_MODE_FIELD_NAME:
      case IMAGE_SENSOR_FIELD_NAME:
      case IMAGE_EXPOSURE_MODE_FIELD_NAME:
      case IMAGE_COLOR_SPACE_FIELD_NAME:
      case IMAGE_WHITE_BALANCE_FIELD_NAME:
      case IMAGE_LENS_FIELD_NAME:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
      case IMAGE_EXPOSURE_TIME_FIELD_NAME:
      case IMAGE_APERTURE_FIELD_NAME:
      case IMAGE_FOCAL_LENGTH_FIELD_NAME:
      case IMAGE_EXPOSURE_BIAS_FIELD_NAME:
      case IMAGE_MAX_APERTURE_VALUE_FIELD_NAME:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.FLOAT)));
      case IMAGE_FLASH_USED_FIELD_NAME:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)));
      default:
        if (!name.startsWith(LOCATION_NAME_PREFIX)) {
          throw new IllegalStateException(String.format("Untreated value '%s' for image media metadata field.", name));
        } else {
          return null;
        }
    }
  }

  public static Schema.Field fromVideoMediaMetadataName(String name) {
    switch (name) {
      case VIDEO_WIDTH_FIELD_NAME:
      case VIDEO_HEIGHT_FIELD_NAME:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.INT)));
      case VIDEO_DURATION_MILLIS_FIELD_NAME:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.LONG)));
      default:
        throw new IllegalStateException(String.format("Untreated value '%s' for video media metadata field.", name));
    }
  }

  public static Schema.Field fromLocationName(String name) {
    switch (name) {
      case IMAGE_LATITUDE_FIELD_NAME:
      case IMAGE_LONGITUDE_FIELD_NAME:
      case IMAGE_ALTITUDE_FIELD_NAME:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)));
      default:
        throw new IllegalStateException(String.format("Untreated value '%s' for location field.", name));
    }
  }
}
