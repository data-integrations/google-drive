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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.google.common.GoogleDriveBaseConfig;

import javax.annotation.Nullable;

/**
 * Configurations for Google Drive Batch Sink plugin.
 */
public class GoogleDriveSinkConfig extends GoogleDriveBaseConfig {
  public static final String SCHEMA_NAME_FIELD_NAME = "schemaNameFieldName";
  public static final String SCHEMA_BODY_FIELD_NAME = "schemaBodyFieldName";

  @Nullable
  @Name(SCHEMA_NAME_FIELD_NAME)
  @Description("Name of the schema field (should be STRING type) which will be used as name of file. \n" +
    "Is optional. In the case it is not set files have randomly generated 16-symbols names.")
  @Macro
  protected String schemaNameFieldName;

  @Name(SCHEMA_BODY_FIELD_NAME)
  @Description("Name of the schema field (should be BYTES type) which will be used as body of file.\n" +
    "The minimal input schema should contain only this field.")
  @Macro
  protected String schemaBodyFieldName;

  public void validate(FailureCollector collector, Schema schema) {
    super.validate(collector);

    // validate body field is in schema and has valid format
    validateSchemaField(collector, schema, SCHEMA_BODY_FIELD_NAME, schemaBodyFieldName,
                        "File body field", Schema.Type.BYTES);

    // validate name field is in schema and has valid format
    validateSchemaField(collector, schema, SCHEMA_NAME_FIELD_NAME, schemaNameFieldName,
                        "File name field", Schema.Type.STRING);
  }

  private void validateSchemaField(FailureCollector collector, Schema schema, String propertyName,
                                   String propertyValue, String propertyLabel, Schema.Type requiredSchemaType) {
    if (!containsMacro(propertyName)) {
      if (!Strings.isNullOrEmpty(propertyValue)) {
        Schema.Field field = schema.getField(propertyValue);
        if (field == null) {
          collector.addFailure(String.format("Input schema doesn't contain '%s' field", propertyValue),
                               String.format("Provide existent field from input schema for '%s'", propertyLabel))
            .withConfigProperty(propertyName);
        } else {
          Schema fieldSchema = field.getSchema();
          if (fieldSchema.isNullable()) {
            fieldSchema = fieldSchema.getNonNullable();
          }

          if (fieldSchema.getLogicalType() != null || fieldSchema.getType() != requiredSchemaType) {
            collector.addFailure(String.format("Field '%s' must be of type '%s' but is of type '%s'",
                                               field.getName(),
                                               requiredSchemaType,
                                               fieldSchema.getDisplayName()),
                                 String.format("Provide field with '%s' format for '%s' property",
                                               requiredSchemaType,
                                               propertyLabel))
              .withConfigProperty(propertyName).withInputSchemaField(propertyValue);
          }
        }
      }
    }
  }

  @Nullable
  public String getSchemaNameFieldName() {
    return schemaNameFieldName;
  }

  public String getSchemaBodyFieldName() {
    return schemaBodyFieldName;
  }
}
