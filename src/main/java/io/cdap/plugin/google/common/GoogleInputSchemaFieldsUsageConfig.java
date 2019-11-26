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

package io.cdap.plugin.google.common;

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;

/**
 * Base Google config for validating of properties that use schema field names as values.
 */
public class GoogleInputSchemaFieldsUsageConfig extends GoogleRetryingConfig {

  protected void validateSchemaField(FailureCollector collector, Schema schema, String propertyName,
                                   String propertyValue, String propertyLabel, Schema.Type requiredSchemaType) {
    if (!containsMacro(propertyName)) {
      if (!Strings.isNullOrEmpty(propertyValue)) {
        Schema.Field field = schema.getField(propertyValue);
        if (field == null) {
          collector.addFailure(String.format("Input schema doesn't contain '%s' field.", propertyValue),
              String.format("Provide existent field from input schema for '%s'.", propertyLabel))
              .withConfigProperty(propertyName);
        } else {
          Schema fieldSchema = field.getSchema();
          if (fieldSchema.isNullable()) {
            fieldSchema = fieldSchema.getNonNullable();
          }

          if (fieldSchema.getLogicalType() != null || fieldSchema.getType() != requiredSchemaType) {
            collector.addFailure(String.format("Field '%s' must be of type '%s' but is of type '%s'.",
                field.getName(),
                requiredSchemaType,
                fieldSchema.getDisplayName()),
                String.format("Provide field with '%s' format for '%s' property.",
                    requiredSchemaType,
                    propertyLabel))
                .withConfigProperty(propertyName).withInputSchemaField(propertyValue);
          }
        }
      }
    }
  }
}
