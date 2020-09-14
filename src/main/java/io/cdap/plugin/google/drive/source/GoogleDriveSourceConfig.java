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

package io.cdap.plugin.google.drive.source;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.google.common.GoogleFilteringSourceConfig;
import io.cdap.plugin.google.common.ValidationResult;
import io.cdap.plugin.google.common.exceptions.InvalidPropertyTypeException;
import io.cdap.plugin.google.common.utils.ExportedType;
import io.cdap.plugin.google.drive.source.utils.BodyFormat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Configurations for Google Drive Batch Source plugin.
 */
public class GoogleDriveSourceConfig extends GoogleFilteringSourceConfig {
  public static final String FILE_METADATA_PROPERTIES = "fileMetadataProperties";
  public static final String FILE_TYPES_TO_PULL = "fileTypesToPull";
  public static final String MAX_PARTITION_SIZE = "maxPartitionSize";
  public static final String BODY_FORMAT = "bodyFormat";
  public static final String DOCS_EXPORTING_FORMAT = "docsExportingFormat";
  public static final String SHEETS_EXPORTING_FORMAT = "sheetsExportingFormat";
  public static final String DRAWINGS_EXPORTING_FORMAT = "drawingsExportingFormat";
  public static final String PRESENTATIONS_EXPORTING_FORMAT = "presentationsExportingFormat";

  public static final String FILE_METADATA_PROPERTIES_LABEL = "File properties";
  public static final String FILE_TYPES_TO_PULL_LABEL = "File types to pull";
  public static final String BODY_FORMAT_LABEL = "Body output format";

  @Nullable
  @Name(FILE_METADATA_PROPERTIES)
  @Description("Properties that represent metadata of files. \n" +
    "They will be a part of output structured record.")
  @Macro
  protected String fileMetadataProperties;

  @Name(FILE_TYPES_TO_PULL)
  @Description("Types of files which should be pulled from a specified directory. \n" +
    "The following values are supported: binary (all non-Google Drive formats), Google Documents, " +
    "Google Spreadsheets, Google Drawings, Google Presentations and Google Apps Scripts. \n" +
    "For Google Drive formats user should specify exporting format in **Exporting** section.")
  @Macro
  protected String fileTypesToPull;

  @Name(MAX_PARTITION_SIZE)
  @Description("Maximum body size for each structured record specified in bytes. \n" +
    "Default 0 value means unlimited. Is not applicable for files in Google formats.")
  @Macro
  protected String maxPartitionSize;

  @Name(BODY_FORMAT)
  @Description("Output format for body of file. \"Bytes\" and \"String\" values are available.")
  @Macro
  protected String bodyFormat;

  @Name(DOCS_EXPORTING_FORMAT)
  @Description("MIME type which is used for Google Documents when converted to structured records.")
  @Macro
  protected String docsExportingFormat;

  @Name(SHEETS_EXPORTING_FORMAT)
  @Description("MIME type which is used for Google Spreadsheets when converted to structured records.")
  @Macro
  protected String sheetsExportingFormat;

  @Name(DRAWINGS_EXPORTING_FORMAT)
  @Description("MIME type which is used for Google Drawings when converted to structured records.")
  @Macro
  protected String drawingsExportingFormat;

  @Name(PRESENTATIONS_EXPORTING_FORMAT)
  @Description("MIME type which is used for Google Presentations when converted to structured records.")
  @Macro
  protected String presentationsExportingFormat;
  private transient Schema schema = null;

  /**
   * Returns the instance of Schema.
   * @return The instance of Schema
   */
  public Schema getSchema() {
    if (schema == null) {
      schema = SchemaBuilder.buildSchema(getFileMetadataProperties(), getBodyFormat());
    }
    return schema;
  }

  /**
   * Returns the ValidationResult.
   * @param collector the failure collector is provided
   * @return The ValidationResult
   */
  public ValidationResult validate(FailureCollector collector) {
    ValidationResult validationResult = super.validate(collector);

    validateFileTypesToPull(collector);

    validateBodyFormat(collector);

    validateFileProperties(collector);
    return validationResult;
  }

  private void validateFileTypesToPull(FailureCollector collector) {
    if (!containsMacro(FILE_TYPES_TO_PULL)) {
      if (!Strings.isNullOrEmpty(fileTypesToPull)) {
        List<String> exportedTypeStrings = Arrays.asList(fileTypesToPull.split(","));
        exportedTypeStrings.forEach(exportedTypeString -> {
          try {
            ExportedType.fromValue(exportedTypeString);
          } catch (InvalidPropertyTypeException e) {
            collector.addFailure(e.getMessage(), null).withConfigProperty(FILE_TYPES_TO_PULL);
          }
        });
      }
    }
  }

  private void validateBodyFormat(FailureCollector collector) {
    if (!containsMacro(BODY_FORMAT)) {
      try {
        getBodyFormat();
      } catch (InvalidPropertyTypeException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(BODY_FORMAT);
      }
    }
  }

  private void validateFileProperties(FailureCollector collector) {
    if (!containsMacro(FILE_METADATA_PROPERTIES) && !Strings.isNullOrEmpty(fileMetadataProperties)) {
      try {
        SchemaBuilder.buildSchema(getFileMetadataProperties(), getBodyFormat());
      } catch (InvalidPropertyTypeException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(FILE_METADATA_PROPERTIES);
      }
    }
  }

  List<String> getFileMetadataProperties() {
    if (Strings.isNullOrEmpty(fileMetadataProperties)) {
      return Collections.emptyList();
    }
    return Arrays.asList(fileMetadataProperties.split(","));
  }

  /**
   * returns the list of ExportedType.
   * @return The list of ExportedType
   */
  public List<ExportedType> getFileTypesToPull() {
    if (Strings.isNullOrEmpty(fileTypesToPull)) {
      return Collections.emptyList();
    }
    return Arrays.stream(fileTypesToPull.split(","))
      .map(type -> ExportedType.fromValue(type)).collect(Collectors.toList());
  }

  public BodyFormat getBodyFormat() {
    return BodyFormat.fromValue(bodyFormat);
  }

  public Long getMaxPartitionSize() {
    return Long.parseLong(maxPartitionSize);
  }

  public String getDocsExportingFormat() {
    return docsExportingFormat;
  }

  public String getSheetsExportingFormat() {
    return sheetsExportingFormat;
  }

  public String getDrawingsExportingFormat() {
    return drawingsExportingFormat;
  }

  public String getPresentationsExportingFormat() {
    return presentationsExportingFormat;
  }
}
