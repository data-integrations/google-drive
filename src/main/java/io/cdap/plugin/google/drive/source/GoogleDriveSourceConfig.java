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
import com.google.gson.JsonObject;
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

import java.io.IOException;
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
  public static final String CONFIGURATION_PARSE_PROPERTY_NAME = "properties";
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

  public GoogleDriveSourceConfig(String referenceName, @Nullable String fileMetadataProperties, String fileTypesToPull,
                                 String maxPartitionSize, String bodyFormat, String sheetsExportingFormat,
                                 String drawingsExportingFormat, String presentationsExportingFormat,
                                 @Nullable String filter, String modificationDateRange, @Nullable String startDate,
                                 @Nullable String endDate) {
    super(referenceName);
    this.fileMetadataProperties = fileMetadataProperties;
    this.fileTypesToPull = fileTypesToPull;
    this.maxPartitionSize = maxPartitionSize;
    this.bodyFormat = bodyFormat;
    this.sheetsExportingFormat = sheetsExportingFormat;
    this.drawingsExportingFormat = drawingsExportingFormat;
    this.presentationsExportingFormat = presentationsExportingFormat;
    this.filter = filter;
    this.modificationDateRange = modificationDateRange;
    this.startDate = startDate;
    this.endDate = endDate;
  }

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

  public GoogleDriveSourceConfig(String referenceName) {
    super(referenceName);
  }

  private static GoogleDriveSourceConfig of(String referenceName) {
    return new GoogleDriveSourceConfig(referenceName);
  }

  public void setFileMetadataProperties(String fileMetadataProperties) {
    this.fileMetadataProperties = fileMetadataProperties;
  }

  public void getBodyFormat(String bodyFormat) {
    this.bodyFormat = bodyFormat;
  }

  public void setFileTypesToPull(String fileTypesToPull) {
    this.fileTypesToPull = fileTypesToPull;
  }

  public void setMaxPartitionSize(String maxPartitionSize) {
    this.maxPartitionSize = maxPartitionSize;
  }

  public void setDocsExportingFormat(String docsExportingFormat) {
    this.docsExportingFormat = docsExportingFormat;
  }

  public void setSheetsExportingFormat(String sheetsExportingFormat) {
    this.sheetsExportingFormat = sheetsExportingFormat;
  }

  public void setDrawingsExportingFormat(String drawingsExportingFormat) {
    this.drawingsExportingFormat = drawingsExportingFormat;
  }

  public void setPresentationsExportingFormat(String presentationsExportingFormat) {
    this.presentationsExportingFormat = presentationsExportingFormat;
  }

  public void setFilter(String filter) {
    this.filter = filter;
  }

  public void setSchema(String schema) throws IOException {
    this.schema = Schema.parseJson(schema);
  }

  public void setModificationDateRange(String modificationDateRange) {
    this.modificationDateRange = modificationDateRange;
  }

  public void setStartDate(String startDate) {
    this.startDate = startDate;
  }

  public void setEndDate(String endDate) {
    this.endDate = endDate;
  }

  public static GoogleDriveSourceConfig of(JsonObject properties) throws IOException {
    GoogleDriveSourceConfig googleDriveSourceConfig = GoogleDriveSourceConfig
      .of(properties.get(GoogleDriveSourceConfig.REFERENCE_NAME).getAsString());

    if (properties.has(GoogleDriveSourceConfig.FILE_METADATA_PROPERTIES)) {
      googleDriveSourceConfig.setFileMetadataProperties(
        properties.get(GoogleDriveSourceConfig.FILE_METADATA_PROPERTIES).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.FILE_TYPES_TO_PULL)) {
      googleDriveSourceConfig.setFileTypesToPull(
        properties.get(GoogleDriveSourceConfig.FILE_TYPES_TO_PULL).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.MAX_PARTITION_SIZE)) {
      googleDriveSourceConfig.setMaxPartitionSize(
        properties.get(GoogleDriveSourceConfig.MAX_PARTITION_SIZE).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.DOCS_EXPORTING_FORMAT)) {
      googleDriveSourceConfig.setDocsExportingFormat(
        properties.get(GoogleDriveSourceConfig.DOCS_EXPORTING_FORMAT).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.SHEETS_EXPORTING_FORMAT)) {
      googleDriveSourceConfig.setSheetsExportingFormat(
        properties.get(GoogleDriveSourceConfig.SHEETS_EXPORTING_FORMAT).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.BODY_FORMAT)) {
      googleDriveSourceConfig.getBodyFormat(properties.get(GoogleDriveSourceConfig.BODY_FORMAT).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.DRAWINGS_EXPORTING_FORMAT)) {
      googleDriveSourceConfig.setDrawingsExportingFormat(
        properties.get(GoogleDriveSourceConfig.DRAWINGS_EXPORTING_FORMAT).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.PRESENTATIONS_EXPORTING_FORMAT)) {
      googleDriveSourceConfig.setPresentationsExportingFormat(
        properties.get(GoogleDriveSourceConfig.PRESENTATIONS_EXPORTING_FORMAT).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.DIRECTORY_IDENTIFIER)) {
      googleDriveSourceConfig.setDirectoryIdentifier(
        properties.get(GoogleDriveSourceConfig.DIRECTORY_IDENTIFIER).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.FILE_IDENTIFIER)) {
      googleDriveSourceConfig.setFileIdentifier(
        properties.get(GoogleDriveSourceConfig.FILE_IDENTIFIER).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.FILTER)) {
      googleDriveSourceConfig.setFilter(properties.get(GoogleDriveSourceConfig.FILTER).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.NAME_SERVICE_ACCOUNT_TYPE)) {
      googleDriveSourceConfig.setServiceAccountType(
        properties.get(GoogleDriveSourceConfig.NAME_SERVICE_ACCOUNT_TYPE).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.NAME_SERVICE_ACCOUNT_JSON)) {
      googleDriveSourceConfig.setServiceAccountJson(
        properties.get(GoogleDriveSourceConfig.NAME_SERVICE_ACCOUNT_JSON).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.ACCOUNT_FILE_PATH)) {
      googleDriveSourceConfig.setAccountFilePath(
        properties.get(GoogleDriveSourceConfig.ACCOUNT_FILE_PATH).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.SCHEMA)) {
      googleDriveSourceConfig.setSchema(properties.get(GoogleDriveSourceConfig.SCHEMA).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.AUTH_TYPE)) {
      googleDriveSourceConfig.setAuthType(properties.get(GoogleDriveSourceConfig.AUTH_TYPE).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.MODIFICATION_DATE_RANGE)) {
      googleDriveSourceConfig.setModificationDateRange(
        properties.get(GoogleDriveSourceConfig.MODIFICATION_DATE_RANGE).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.START_DATE)) {
      googleDriveSourceConfig.setStartDate(properties.get(GoogleDriveSourceConfig.START_DATE).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.END_DATE)) {
      googleDriveSourceConfig.setEndDate(properties.get(GoogleDriveSourceConfig.END_DATE).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.CLIENT_ID)) {
      googleDriveSourceConfig.setClientId(properties.get(GoogleDriveSourceConfig.CLIENT_ID).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.CLIENT_SECRET)) {
      googleDriveSourceConfig.setClientSecret(properties.get(GoogleDriveSourceConfig.CLIENT_SECRET).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.REFRESH_TOKEN)) {
      googleDriveSourceConfig.setRefreshToken(properties.get(GoogleDriveSourceConfig.REFRESH_TOKEN).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.ACCESS_TOKEN)) {
      googleDriveSourceConfig.setAccessToken(properties.get(GoogleDriveSourceConfig.ACCESS_TOKEN).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.OAUTH_METHOD)) {
      googleDriveSourceConfig.setOauthMethod(properties.get(GoogleDriveSourceConfig.OAUTH_METHOD).getAsString());
    }
    return googleDriveSourceConfig;
  }
}
