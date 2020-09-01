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

package io.cdap.plugin.google.drive.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.google.common.GoogleInputSchemaFieldsUsageConfig;

import javax.annotation.Nullable;

/**
 * Configurations for Google Drive Batch Sink plugin.
 */
public class GoogleDriveSinkConfig extends GoogleInputSchemaFieldsUsageConfig {
  public static final String SCHEMA_BODY_FIELD_NAME = "schemaBodyFieldName";
  public static final String SCHEMA_NAME_FIELD_NAME = "schemaNameFieldName";
  public static final String SCHEMA_MIME_FIELD_NAME = "schemaMimeFieldName";

  @Name(SCHEMA_BODY_FIELD_NAME)
  @Description("Name of the schema field (should be BYTES type) which will be used as body of file.\n" +
    "The minimal input schema should contain only this field.")
  @Macro
  protected String schemaBodyFieldName;

  @Nullable
  @Name(SCHEMA_NAME_FIELD_NAME)
  @Description("Name of the schema field (should be STRING type) which will be used as name of file. \n" +
    "Is optional. In the case it is not set files have randomly generated 16-symbols names.")
  @Macro
  protected String schemaNameFieldName;

  @Nullable
  @Name(SCHEMA_MIME_FIELD_NAME)
  @Description("Name of the schema field (should be STRING type) which will be used as MIME type of file. \n" +
    "All MIME types are supported except Google Drive types: https://developers.google.com/drive/api/v3/mime-types.\n" +
    "Is optional. In the case it is not set Google API will try to recognize file's MIME type automatically.")
  @Macro
  protected String schemaMimeFieldName;

  /**
   * Validate that the given schema is compatible with the given extension.
   * @param collector failure collector with
   * @param schema the schema to check compatibility
   */
  public void validate(FailureCollector collector, Schema schema) {
    super.validate(collector);

    // validate body field is in schema and has valid format
    validateSchemaField(collector, schema, SCHEMA_BODY_FIELD_NAME, schemaBodyFieldName,
                        "File body field", Schema.Type.BYTES);

    // validate name field is in schema and has valid format
    validateSchemaField(collector, schema, SCHEMA_NAME_FIELD_NAME, schemaNameFieldName,
                        "File name field", Schema.Type.STRING);

    // validate mime field is in schema and has valid format
    validateSchemaField(collector, schema, SCHEMA_MIME_FIELD_NAME, schemaMimeFieldName,
                        "File mime field", Schema.Type.STRING);
  }

  public String getSchemaBodyFieldName() {
    return schemaBodyFieldName;
  }

  @Nullable
  public String getSchemaNameFieldName() {
    return schemaNameFieldName;
  }

  @Nullable
  public String getSchemaMimeFieldName() {
    return schemaMimeFieldName;
  }
}
