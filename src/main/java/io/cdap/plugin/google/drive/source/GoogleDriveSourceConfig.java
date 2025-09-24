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
import io.cdap.cdap.etl.api.StageContext;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.format.plugin.FileSourceProperties;
import io.cdap.plugin.google.common.GoogleFilteringSourceConfig;
import io.cdap.plugin.google.common.IdentifierType;
import io.cdap.plugin.google.common.ValidationResult;
import io.cdap.plugin.google.common.exceptions.InvalidPropertyTypeException;
import io.cdap.plugin.google.common.utils.ExportedType;
import io.cdap.plugin.google.drive.source.utils.BodyFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Configurations for Google Drive Batch Source plugin.
 */
public class GoogleDriveSourceConfig extends GoogleFilteringSourceConfig implements FileSourceProperties {
  public static final String FILE_METADATA_PROPERTIES = "fileMetadataProperties";
  public static final String CONFIGURATION_PARSE_PROPERTY_NAME = "properties";
  public static final String FILE_TYPES_TO_PULL = "fileTypesToPull";
  public static final String MAX_PARTITION_SIZE = "maxPartitionSize";
  public static final String BODY_FORMAT = "bodyFormat";
  public static final String DOCS_EXPORTING_FORMAT = "docsExportingFormat";
  public static final String SHEETS_EXPORTING_FORMAT = "sheetsExportingFormat";
  public static final String DRAWINGS_EXPORTING_FORMAT = "drawingsExportingFormat";
  public static final String PRESENTATIONS_EXPORTING_FORMAT = "presentationsExportingFormat";
  public static final String IS_STRUCTURED_SCHEMA_REQUIRED = "structuredSchemaRequired";
  public static final String NAME_SCHEMA = "schema";
  public static final String NAME_FORMAT = "format";

  public static final String DEFAULT_BODY_FORMAT = "bytes";
  public static final long DEFAULT_MAX_PARTITION_SIZE = 0;
  public static final String DEFAULT_DOCS_EXPORTING_FORMAT = "text/plain";
  public static final String DEFAULT_SHEETS_EXPORTING_FORMAT = "text/csv";
  public static final String DEFAULT_DRAWINGS_EXPORTING_FORMAT = "image/svg+xml";
  public static final String DEFAULT_PRESENTATIONS_EXPORTING_FORMAT = "text/plain";

  public static final String FILE_METADATA_PROPERTIES_LABEL = "File properties";
  public static final String FILE_TYPES_TO_PULL_LABEL = "File types to pull";
  public static final String BODY_FORMAT_LABEL = "Body output format";

  public static final String GOOGLE_DRIVE_SCHEMA = "drive";
  public static final String GOOGLE_DRIVE_AUTHORITY = "drive.google.com";
  public static final String GOOGLE_DRIVE_FILE_PATH_PREFIX = "/drive/file/d";
  public static final String GOOGLE_DRIVE_FOLDER_PATH_PREFIX = "/drive/folders";
  public static final String GOOGLE_DRIVE_DEFAULT_FILENAME = "default.txt";

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
  @Nullable
  @Macro
  protected String fileTypesToPull;

  @Name(MAX_PARTITION_SIZE)
  @Description("Maximum body size for each structured record specified in bytes. \n" +
    "Default 0 value means unlimited. Is not applicable for files in Google formats.")
  @Nullable
  @Macro
  protected String maxPartitionSize;

  @Name(BODY_FORMAT)
  @Description("Output format for body of file. \"Bytes\" and \"String\" values are available.")
  @Nullable
  @Macro
  protected String bodyFormat;

  @Name(DOCS_EXPORTING_FORMAT)
  @Description("MIME type which is used for Google Documents when converted to structured records.")
  @Nullable
  @Macro
  protected String docsExportingFormat;

  @Name(SHEETS_EXPORTING_FORMAT)
  @Description("MIME type which is used for Google Spreadsheets when converted to structured records.")
  @Nullable
  @Macro
  protected String sheetsExportingFormat;

  @Name(DRAWINGS_EXPORTING_FORMAT)
  @Description("MIME type which is used for Google Drawings when converted to structured records.")
  @Nullable
  @Macro
  protected String drawingsExportingFormat;

  @Name(PRESENTATIONS_EXPORTING_FORMAT)
  @Description("MIME type which is used for Google Presentations when converted to structured records.")
  @Nullable
  @Macro
  protected String presentationsExportingFormat;

  @Macro
  @Nullable
  @Description("Output schema for the source. Formats like 'avro' and 'parquet' require a schema in order to "
    + "read the data.")
  private String schema;

  @Name(IS_STRUCTURED_SCHEMA_REQUIRED)
  @Description("Wheather to fetch schema or not")
  @Nullable
  protected Boolean isStructuredSchemaRequired;

  @Name(NAME_FORMAT)
  @Macro
  @Description("Format of the data to read. Supported formats are 'csv'....")
  @Nullable
  private String format;

  @Macro
  @Nullable
  @Description("Whether to recursively read directories within the input directory. The default is false.")
  private Boolean recursive;

  @Macro
  @Nullable
  @Description("Whether to allow an input that does not exist. When false, the source will fail the run if the input "
      + "does not exist. When true, the run will not fail and the source will not generate any output. "
      + "The default value is false.")
  private Boolean ignoreNonExistingFolders;

  @Macro
  @Nullable
  @Description("The maximum number of rows that will get investigated for automatic data type detection.")
  private Long sampleSize;

  @Macro
  @Nullable
  @Description("A list of columns with the corresponding data types for whom the automatic data type detection gets " +
      "skipped.")
  private String override;

  @Macro
  @Nullable
  @Description("The delimiter to use if the format is 'delimited'. The delimiter will be ignored if the format "
      + "is anything other than 'delimited'.")
  private String delimiter;

  @Macro
  @Nullable
  @Description("Whether to use first row as header. Supported formats are 'text', 'csv', 'tsv', " +
      "'delimited'. Default value is false.")
  private Boolean skipHeader;

  @Macro
  @Nullable
  @Description("Whether to treat content between quotes as a value. This value will only be used if the format " +
      "is 'csv', 'tsv' or 'delimited'. The default value is false.")
  protected Boolean enableQuotedValues;

  @Macro
  @Nullable
  @Description("Any additional properties to use when reading from the filesystem. "
      + "This is an advanced feature that requires knowledge of the properties supported by the underlying filesystem.")
  private String fileSystemProperties;

  @Macro
  @Nullable
  @Description("File encoding for the source files. The default encoding is 'UTF-8'")
  private String fileEncoding;

  @Macro
  @Nullable
  @Description("Select the sheet by name or number. Default is 'Sheet Number'.")
  private String sheet;

  @Macro
  @Nullable
  @Description("The name/number of the sheet to read from. If not specified, the first sheet will be read." +
      "Sheet Numbers are 0 based, ie first sheet is 0.")
  private String sheetValue;

  @Macro
  @Nullable
  @Description("Specify whether to stop reading after encountering the first empty row. Defaults to false.")
  private String terminateIfEmptyRow;

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

  @Override
  public void validate(FailureCollector collector) {
    getSchema();
    // Extra validation when structure schema is required
  }

  @Override
  public String getPath() {
    IdentifierType idType = getIdentifierType();
    if (idType == IdentifierType.FILE_IDENTIFIER) {
      return String.format("%s://%s%s/%s/%s", GOOGLE_DRIVE_SCHEMA, GOOGLE_DRIVE_AUTHORITY,
        GOOGLE_DRIVE_FILE_PATH_PREFIX, getFileIdentifier(), GOOGLE_DRIVE_DEFAULT_FILENAME);
    } else if (idType == IdentifierType.DIRECTORY_IDENTIFIER) {
      return String.format("%s://%s%s/%s/", GOOGLE_DRIVE_SCHEMA, GOOGLE_DRIVE_AUTHORITY,
        GOOGLE_DRIVE_FOLDER_PATH_PREFIX, getDirectoryIdentifier());
    }
    throw new IllegalArgumentException(String.format("Invalid identifier type '%s'. Expected one of: %s or %s.", idType,
        IdentifierType.FILE_IDENTIFIER, IdentifierType.DIRECTORY_IDENTIFIER));
  }

  @Override
  public String getPath(StageContext context) {
    return getPath();
  }

  @Override
  public String getFormatName() {
    // need to do this for backwards compatibility, where the pre-packaged format names were case insensitive.
    try {
      FileFormat fileFormat = FileFormat.from(format, x -> true);
      return fileFormat.name().toLowerCase();
    } catch (IllegalArgumentException e) {
      // ignore
    }
    return format;
  }

  @Nullable
  @Override
  public FileFormat getFormat() {
    throw new UnsupportedOperationException("GDrive does not support: FileFormat getFormat() method");
  }

  @Nullable
  @Override
  public Pattern getFilePattern() {
    return null;
  }

  @Override
  public long getMaxSplitSize() {
    return Long.MAX_VALUE;
  }

  @Override
  public boolean shouldAllowEmptyInput() {
    return ignoreNonExistingFolders != null && ignoreNonExistingFolders;
  }

  @Override
  public boolean shouldReadRecursively() {
    return recursive != null && recursive;
  }

  @Nullable
  @Override
  public String getPathField() {
    return null;
  }

  @Override
  public boolean useFilenameAsPath() {
    throw new UnsupportedOperationException("GDrive does not support: boolean useFilenameAsPath() method");
  }

  @Override
  public boolean skipHeader() {
    throw new UnsupportedOperationException("GDrive does not support: boolean skipHeader() method");
  }

  /**
   * throw new UnsupportedOperationException("GDrive  does not support: /**() method;
   * Returns the instance of Schema.
   * @return The instance of Schema
   */
  public Schema getSchema() {
    if (!isStructuredSchemaRequired() && Strings.isNullOrEmpty(schema)) {
      schema = SchemaBuilder.buildSchema(getFileMetadataProperties(), getBodyFormat()).toString();
    }
    if (Strings.isNullOrEmpty(schema)) {
      return null;
    }
    try {
      return Schema.parseJson(schema);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid schema: " + e.getMessage(), e);
    }
  }

  /**
   * Returns the ValidationResult.
   * @param collector the failure collector is provided
   * @return The ValidationResult
   */
  public ValidationResult getValidationResult(FailureCollector collector) {
    ValidationResult validationResult = super.getValidationResult(collector);

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
    return bodyFormat == null ? BodyFormat.fromValue(DEFAULT_BODY_FORMAT) : BodyFormat.fromValue(bodyFormat);
  }

  public Long getMaxPartitionSize() {
    return Strings.isNullOrEmpty(maxPartitionSize) ? DEFAULT_MAX_PARTITION_SIZE : Long.parseLong(maxPartitionSize);
  }

  public String getDocsExportingFormat() {
    return Strings.isNullOrEmpty(docsExportingFormat) ? DEFAULT_DOCS_EXPORTING_FORMAT : docsExportingFormat;
  }

  public String getSheetsExportingFormat() {
    return Strings.isNullOrEmpty(sheetsExportingFormat) ? DEFAULT_SHEETS_EXPORTING_FORMAT : sheetsExportingFormat;
  }

  public String getDrawingsExportingFormat() {
    return Strings.isNullOrEmpty(drawingsExportingFormat) ? DEFAULT_DRAWINGS_EXPORTING_FORMAT : drawingsExportingFormat;
  }

  public String getPresentationsExportingFormat() {
    return Strings.isNullOrEmpty(presentationsExportingFormat) ?
      DEFAULT_PRESENTATIONS_EXPORTING_FORMAT : presentationsExportingFormat;
  }

  public boolean isStructuredSchemaRequired() {
    if (isStructuredSchemaRequired == null) {
      return false; // for backward compatibility, default to false
    }
    return isStructuredSchemaRequired;
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

  public void setSchema(String schema) {
    this.schema = schema;
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
      googleDriveSourceConfig.setoAuthMethod(properties.get(GoogleDriveSourceConfig.OAUTH_METHOD).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.FILE_IDENTIFIER)) {
      googleDriveSourceConfig.setFileIdentifier(
        properties.get(GoogleDriveSourceConfig.FILE_IDENTIFIER).getAsString());
    }
    if (properties.has(GoogleDriveSourceConfig.IDENTIFIER_TYPE)) {
      googleDriveSourceConfig.setIdentifierType(
        properties.get(GoogleDriveSourceConfig.IDENTIFIER_TYPE).getAsString());
    }
    return googleDriveSourceConfig;
  }
}
