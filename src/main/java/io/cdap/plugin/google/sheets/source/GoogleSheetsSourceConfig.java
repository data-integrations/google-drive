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

package io.cdap.plugin.google.sheets.source;

import com.github.rholder.retry.RetryException;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.drive.model.File;
import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.CellFormat;
import com.google.api.services.sheets.v4.model.ExtendedValue;
import com.google.api.services.sheets.v4.model.GridRange;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.google.common.GoogleDriveFilteringClient;
import io.cdap.plugin.google.common.GoogleFilteringSourceConfig;
import io.cdap.plugin.google.common.ValidationResult;
import io.cdap.plugin.google.common.exceptions.InvalidPropertyTypeException;
import io.cdap.plugin.google.common.utils.ExportedType;
import io.cdap.plugin.google.sheets.source.utils.CellCoordinate;
import io.cdap.plugin.google.sheets.source.utils.ColumnAddressConverter;
import io.cdap.plugin.google.sheets.source.utils.ColumnComplexSchemaInfo;
import io.cdap.plugin.google.sheets.source.utils.Formatting;
import io.cdap.plugin.google.sheets.source.utils.HeaderSelection;
import io.cdap.plugin.google.sheets.source.utils.MergesForNumeredRows;
import io.cdap.plugin.google.sheets.source.utils.MetadataKeyValueAddress;
import io.cdap.plugin.google.sheets.source.utils.SheetsToPull;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Configurations for Google Sheets Batch Source plugin.
 */
public class GoogleSheetsSourceConfig extends GoogleFilteringSourceConfig {

  public static final String SHEETS_TO_PULL = "sheetsToPull";
  public static final String SHEETS_IDENTIFIERS = "sheetsIdentifiers";
  public static final String FORMATTING = "formatting";
  public static final String SKIP_EMPTY_DATA = "skipEmptyData";
  public static final String COLUMN_NAMES_SELECTION = "columnNamesSelection";
  public static final String CUSTOM_COLUMN_NAMES_ROW = "customColumnNamesRow";
  public static final String METADATA_FIELD_NAME = "metadataFieldName";
  public static final String EXTRACT_METADATA = "extractMetadata";
  public static final String FIRST_HEADER_ROW = "firstHeaderRow";
  public static final String LAST_HEADER_ROW = "lastHeaderRow";
  public static final String FIRST_FOOTER_ROW = "firstFooterRow";
  public static final String LAST_FOOTER_ROW = "lastFooterRow";
  public static final String AUTO_DETECT_ROWS_AND_COLUMNS = "autoDetectRowsAndColumns";
  public static final String LAST_DATA_COLUMN = "lastDataColumn";
  public static final String LAST_DATA_ROW = "lastDataRow";
  public static final String METADATA_CELLS = "metadataCells";
  public static final String READ_BUFFER_SIZE = "readBufferSize";
  public static final String ADD_NAME_FIELDS = "addNameFields";
  public static final String SPREADSHEET_FIELD_NAME = "spreadsheetFieldName";
  public static final String SHEET_FIELD_NAME = "sheetFieldName";
  public static final String NAME_SCHEMA = "schema";
  public static final String HEADERS_SELECTION_LABEL = "Header selection";
  public static final String SHEETS_TO_PULL_LABEL = "Sheets to pull";
  public static final String FORMATTING_LABEL = "Formatting";
  public static final String SHEETS_IDENTIFIERS_LABEL = "Sheets identifiers";
  public static final String CONFIGURATION_PARSE_PROPERTY_NAME = "properties";
  private static final Logger LOG = LoggerFactory.getLogger(GoogleSheetsSourceConfig.class);
  private static final Pattern CELL_ADDRESS = Pattern.compile("^([A-Z]+)([0-9]+)$");
  private static final Pattern NOT_VALID_PATTERN = Pattern.compile("[^A-Za-z0-9_]+");
  private static final Pattern COLUMN_NAME = Pattern.compile("^[A-Za-z_][A-Za-z0-9_-]*$");
  public static final String CLEANSE_COLUMN_NAMES = "columnNameCleansingEnabled";
  private static LinkedHashMap<Integer, ColumnComplexSchemaInfo> dataSchemaInfo = new LinkedHashMap<>();

  @Name(SHEETS_TO_PULL)
  @Description("Filter for specifying set of sheets to process.  " +
    "For 'numbers' or 'titles' selections user can populate specific values in 'Sheets identifiers' field.")
  @Macro
  private String sheetsToPull;

  @Nullable
  @Name(SHEETS_IDENTIFIERS)
  @Description("Set of sheets' numbers/titles to process. " +
    "Is shown only when 'titles' or 'numbers' are selected for 'Sheets to pull' field.")
  @Macro
  private String sheetsIdentifiers;

  @Nullable
  @Name(NAME_SCHEMA)
  @Description("The schema of the table to read.")
  @Macro
  private String schema;

  @Name(FORMATTING)
  @Description("Output format for numeric sheet cells. " +
    "In 'Formatted values' case the value will contain appropriate format of source cell e.g. '1.23$', '123%'." +
    "For 'Values only' only number value will be returned.")
  @Macro
  private String formatting;

  @Name(SKIP_EMPTY_DATA)
  @Description("Field to allow skipping of empty structure records.")
  @Macro
  private boolean skipEmptyData;

  @Name(COLUMN_NAMES_SELECTION)
  @Description("Source for column names. User can specify where from the plugin should get schema filed names." +
    "Are available following values: _No column names_ - default sheet column names will be used ('A', 'B' etc.), " +
    "_Treat first row as column names_ - uses first row for schema defining and field names, " +
    "_Custom row as column names_ - as previous, but for custom row.")
  @Macro
  private String columnNamesSelection;

  @Nullable
  @Name(CUSTOM_COLUMN_NAMES_ROW)
  @Description("Number of the row to be treated as a header. " +
    "Only shown when the 'Column Names Selection' field is set to 'Custom row as column names' header.")
  @Macro
  private Integer customColumnNamesRow;

  @Name(METADATA_FIELD_NAME)
  @Description("Name of the record with metadata content. " +
    "It is needed to distinguish metadata record from possible column with the same name.")
  @Macro
  private String metadataFieldName;

  @Name(EXTRACT_METADATA)
  @Description("Field to enable metadata extraction. Metadata extraction is useful when user wants to specify " +
    "a header or a footer for a sheet. The rows in headers and footers are not available as data records. " +
    "Instead, they are available in every record as a field called 'metadata', " +
    "which is a record of the specified metadata.")
  @Macro
  private boolean extractMetadata;

  @Nullable
  @Name(FIRST_HEADER_ROW)
  @Description("Row number of the first row to be treated as header.")
  @Macro
  private Integer firstHeaderRow;

  @Nullable
  @Name(LAST_HEADER_ROW)
  @Description("Row number of the last row to be treated as header.")
  @Macro
  private Integer lastHeaderRow;

  @Nullable
  @Name(FIRST_FOOTER_ROW)
  @Description("Row number of the first row to be treated as footer.")
  @Macro
  private Integer firstFooterRow;

  @Nullable
  @Name(LAST_FOOTER_ROW)
  @Description("Row number of the last row to be treated as footer.")
  @Macro
  private Integer lastFooterRow;

  @Nullable
  @Name(AUTO_DETECT_ROWS_AND_COLUMNS)
  @Description("Field to enable automatic detection of the number of rows and columns to read from the sheet.")
  private Boolean autoDetectRowsAndColumns;

  @Name(LAST_DATA_COLUMN)
  @Nullable
  @Description("Last column plugin will read as data. It will be ignored if the Column Names " +
    "Row contain less number of columns.")
  @Macro
  private String lastDataColumn;

  @Name(LAST_DATA_ROW)
  @Nullable
  @Description("Last row plugin will read as data.")
  @Macro
  private String lastDataRow;

  @Nullable
  @Name(METADATA_CELLS)
  @Description("Set of the cells for key-value pairs to extract as metadata from the specified metadata sections. " +
    "Only shown if Extract metadata is set to true. The cell numbers should be within the header or footer.")
  @Macro
  private String metadataCells;

  @Nullable
  @Name(READ_BUFFER_SIZE)
  @Description("Number of rows the source reads with single API request.")
  @Macro
  private Integer readBufferSize;

  @Nullable
  @Name(ADD_NAME_FIELDS)
  @Description("Toggle that defines if the source extends output schema with spreadsheet and sheet names.")
  @Macro
  private Boolean addNameFields;

  @Nullable
  @Name(SPREADSHEET_FIELD_NAME)
  @Description("Schema field name for spreadsheet name.")
  @Macro
  private String spreadsheetFieldName;

  @Nullable
  @Name(SHEET_FIELD_NAME)
  @Description("Schema field name for sheet name.")
  @Macro
  private String sheetFieldName;

  @Nullable
  @Name(CLEANSE_COLUMN_NAMES)
  @Description("Toggle that specifies whether to cleanse column names containing special characters. " +
    "Special characters will be replaced by underscores.")
  @Macro
  private Boolean columnNameCleansingEnabled;

  public GoogleSheetsSourceConfig(String referenceName, @Nullable String sheetsIdentifiers, String formatting,
                                  Boolean skipEmptyData, String columnNamesSelection,
                                  @Nullable Integer customColumnNamesRow, String metadataFieldName,
                                  Boolean extractMetadata, @Nullable Integer firstFooterRow,
                                  @Nullable Integer lastHeaderRow, @Nullable Integer lastFooterRow,
                                  @Nullable String lastDataColumn, @Nullable String lastDataRow,
                                  @Nullable String metadataCells, @Nullable Integer readBufferSize,
                                  @Nullable Boolean addNameFields, @Nullable String spreadsheetFieldName,
                                  @Nullable String sheetFieldName, @Nullable String schema) {

    super(referenceName);
    this.sheetsIdentifiers = sheetsIdentifiers;
    this.formatting = formatting;
    this.skipEmptyData = skipEmptyData;
    this.metadataFieldName = metadataFieldName;
    this.columnNamesSelection = columnNamesSelection;
    this.customColumnNamesRow = customColumnNamesRow;
    this.extractMetadata = extractMetadata;
    this.firstFooterRow = firstFooterRow;
    this.lastHeaderRow = lastHeaderRow;
    this.lastDataColumn = lastDataColumn;
    this.lastDataRow = lastDataRow;
    this.lastFooterRow = lastFooterRow;
    this.metadataCells = metadataCells;
    this.readBufferSize = readBufferSize;
    this.addNameFields = addNameFields;
    this.spreadsheetFieldName = spreadsheetFieldName;
    this.sheetFieldName = sheetFieldName;
    this.schema = schema;
  }


  public GoogleSheetsSourceConfig(String referenceName) {
    super(referenceName);
  }

  /**
   * Returns the instance of Schema.
   * @return The instance of Schema
   * @param collector throws validation exception
   */
  public Schema getSchema(FailureCollector collector) {
    if (schema == null && shouldGetSchema()) {
      if (dataSchemaInfo.isEmpty()) {
        collector.addFailure("There are no headers to process.",
                             "Perhaps no validation step was executed before schema generation.")
          .withConfigProperty(SCHEMA);
      }
      return SchemaBuilder.buildSchema(this, new ArrayList<>(dataSchemaInfo.values()));
    }
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      collector.addFailure("Invalid schema: " + e.getMessage(),
          null).withConfigProperty(NAME_SCHEMA);
    }
    throw collector.getOrThrowException();
  }

  private boolean shouldGetSchema() {
    return !containsMacro(SHEETS_TO_PULL) && !containsMacro(SHEETS_IDENTIFIERS) &&
      !containsMacro(COLUMN_NAMES_SELECTION) && !containsMacro(CUSTOM_COLUMN_NAMES_ROW) &&
      !containsMacro(LAST_DATA_COLUMN) && !containsMacro(NAME_SERVICE_ACCOUNT_TYPE) &&
      !containsMacro(ACCOUNT_FILE_PATH) && !containsMacro(NAME_SERVICE_ACCOUNT_JSON) &&
      !containsMacro(CLIENT_ID) && !containsMacro(CLIENT_SECRET) &&
      !containsMacro(REFRESH_TOKEN) && !containsMacro(ACCESS_TOKEN) &&
      !containsMacro(OAUTH_METHOD) && !containsMacro(FILE_IDENTIFIER) &&
      !containsMacro(DIRECTORY_IDENTIFIER);
  }

  /**
   * Returns the ValidationResult
   * @param collector the failure collector is provided
   * @return The ValidationResult
   */
  public ValidationResult validate(FailureCollector collector) {
    ValidationResult validationResult = super.validate(collector);

    // reset current headers info
    dataSchemaInfo = new LinkedHashMap<>();

    validateColumnNamesRow(collector);
    if (!getAutoDetectRowsAndColumns()) {
      validateLastDataColumnIndexAndLastRowIndex(collector);
    }
    validateSpreadsheetAndSheetFieldNames(collector);

    if (collector.getValidationFailures().isEmpty() && validationResult.isDirectoryOrFileAccessible()) {
      GoogleDriveFilteringClient driveClient;
      GoogleSheetsSourceClient sheetsSourceClient;
      try {
        driveClient = new GoogleSheetsFilteringClient(this);
        sheetsSourceClient = new GoogleSheetsSourceClient(this);
      } catch (IOException e) {
        collector.addFailure("Exception during drive and sheets connections instantiating.", null);
        return validationResult;
      }
      List<File> spreadsheetsFiles = null;
      try {
        spreadsheetsFiles = driveClient
          .getFilesSummary(Collections.singletonList(ExportedType.SPREADSHEETS), 1);
      } catch (ExecutionException | RetryException e) {
        collector.addFailure(
          String.format("Failed to get spreadsheet file summary due to reason : %s", e.getMessage()),
                        null).withStacktrace(e.getStackTrace());
        return validationResult;
      }

      // validate source folder is not empty
      validateSourceFolder(collector, spreadsheetsFiles);

      try {
        // validate titles/numbers set
        validateSheetIdentifiers(collector, sheetsSourceClient, spreadsheetsFiles);

        // validate all sheets have the same schema
        getAndValidateSheetSchema(collector, sheetsSourceClient, spreadsheetsFiles);
      } catch (ExecutionException | RetryException e) {
        String message = e.getMessage();
        if (e.getCause() instanceof GoogleJsonResponseException) {
          GoogleJsonResponseException cause = (GoogleJsonResponseException) e.getCause();
          message = cause.getDetails().getMessage();
        }
        collector.addFailure(String.format(message), null)
                .withStacktrace(e.getStackTrace());
      }
    }

    // validate metadata
    validateMetadata(collector);
    return validationResult;
  }

  private void validateColumnNamesRow(FailureCollector collector) {
    if (!containsMacro(COLUMN_NAMES_SELECTION)) {
      try {
        getColumnNamesSelection();
      } catch (InvalidPropertyTypeException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(COLUMN_NAMES_SELECTION);
      }
    }
  }

  private void validateLastDataColumnIndexAndLastRowIndex(FailureCollector collector) {
    if (!containsMacro(LAST_DATA_COLUMN)) {
      if (getLastDataColumn() <= 0) {
        collector.addFailure("Last Data Column Index should be greater than 0",
                             null).withConfigProperty(LAST_DATA_COLUMN);
      }
    }
    if (!containsMacro(LAST_DATA_ROW)) {
      if (getLastDataRow() <= 0) {
        collector.addFailure("Last Data Row Index should be greater than 0",
                             null).withConfigProperty(LAST_DATA_ROW);
      }
    }
  }

  private void validateSpreadsheetAndSheetFieldNames(FailureCollector collector) {
    if (!containsMacro(SPREADSHEET_FIELD_NAME) && getAddNameFields()) {
      if (Strings.isNullOrEmpty(getSpreadsheetFieldName())) {
        collector.addFailure("Spreadsheet Field Name cannot be empty or null",
                             null).withConfigProperty(SPREADSHEET_FIELD_NAME);
      }
    }
    if (!containsMacro(SHEET_FIELD_NAME) && getAddNameFields()) {
      if (Strings.isNullOrEmpty(getSpreadsheetFieldName())) {
        collector.addFailure("Sheet Field Name cannot be empty or null",
                             null).withConfigProperty(SHEET_FIELD_NAME);
      }
    }
  }

  /**
   * Method that validates that source folder has at least single spreadsheet to process.
   *
   * @param collector failure collector.
   * @param spreadsheetsFiles list of spreadsheet files.
   */
  private void validateSourceFolder(FailureCollector collector, List<File> spreadsheetsFiles) {
    if (spreadsheetsFiles.isEmpty()) {
      collector.addFailure(String.format("No spreadsheets found in '%s' folder with '%s' filter.",
        getDirectoryIdentifier(), getFilter()), null)
        .withConfigProperty(DIRECTORY_IDENTIFIER).withConfigProperty(FILTER);
    }
  }

  /**
   * Method that validates that retrieved spreadsheet file contains all required sheets. Is applicable only if
   * {@link SheetsToPull#ALL} option is not selected for "Sheets identifiers" property.
   *
   * @param collector failure collector.
   * @param sheetsSourceClient sheets source client;
   * @param spreadsheetsFiles spreadsheet files.
   * @throws ExecutionException on API call is failed.
   * @throws RetryException on API call retry number is exceeded.
   */
  private void validateSheetIdentifiers(FailureCollector collector, GoogleSheetsSourceClient sheetsSourceClient,
                                        List<File> spreadsheetsFiles) throws ExecutionException, RetryException {
    if (!containsMacro(SHEETS_TO_PULL) && !containsMacro(SHEETS_IDENTIFIERS)
      && collector.getValidationFailures().isEmpty() && !getSheetsToPull().equals(SheetsToPull.ALL)
      && checkPropertyIsSet(collector, sheetsIdentifiers, SHEETS_IDENTIFIERS, SHEETS_IDENTIFIERS_LABEL)) {

      String currentSpreadsheetId = null;
      Map<String, List<String>> allTitles = new HashMap<>();
      Map<String, List<Integer>> allIndexes = new HashMap<>();
      File spreadsheetFile = spreadsheetsFiles.get(0);
      currentSpreadsheetId = spreadsheetFile.getId();
      List<Sheet> sheets = sheetsSourceClient.getSheets(currentSpreadsheetId);
      allTitles.put(currentSpreadsheetId, sheets.stream()
        .map(s -> s.getProperties().getTitle()).collect(Collectors.toList()));
      allIndexes.put(currentSpreadsheetId, sheets.stream()
        .map(s -> s.getProperties().getIndex()).collect(Collectors.toList()));

      SheetsToPull sheetsToPull = getSheetsToPull();
      switch (sheetsToPull) {
        case TITLES:
          List<String> titles = getSheetsIdentifiers();
          checkSheetIdentifiers(collector, titles, allTitles);
          break;
        case NUMBERS:
          List<Integer> indexes = getSheetsIdentifiers().stream().map(Integer::parseInt).collect(Collectors.toList());
          checkSheetIdentifiers(collector, indexes, allIndexes);
          break;
        default:
          collector.addFailure(String.format("'%s' is not processed value.", sheetsToPull.toString()), null)
            .withConfigProperty(AUTH_TYPE);
      }
    }
  }

  private <I> void checkSheetIdentifiers(FailureCollector collector, List<I> requiredIdentifiers,
                                         Map<String, List<I>> availableIdentifiers) {
    if (requiredIdentifiers.isEmpty()) {
      collector.addFailure(String.format("No required sheets present."), null)
        .withConfigProperty(SHEETS_IDENTIFIERS);
    }
    for (Map.Entry<String, List<I>> spreadsheetTitles : availableIdentifiers.entrySet()) {
      List<I> availableTitles = spreadsheetTitles.getValue();
      if (!availableTitles.containsAll(requiredIdentifiers)) {
        collector.addFailure(
          String.format("Spreadsheet '%s' doesn't have all required sheets. Required: '%s', available: '%s'.",
            spreadsheetTitles.getKey(), requiredIdentifiers, availableTitles), null)
          .withConfigProperty(SHEETS_IDENTIFIERS);
      }
    }
  }

  private void getAndValidateSheetSchema(FailureCollector collector, GoogleSheetsSourceClient sheetsSourceClient,
                                         List<File> spreadsheetsFiles) throws ExecutionException, RetryException {
    if (!containsMacro(SHEETS_TO_PULL) && !containsMacro(SHEETS_IDENTIFIERS)
      && !containsMacro(COLUMN_NAMES_SELECTION) && !containsMacro(CUSTOM_COLUMN_NAMES_ROW)
      && !containsMacro(LAST_DATA_COLUMN) && collector.getValidationFailures().isEmpty()) {

      String currentSpreadsheetId = null;
      String currentSheetTitle = null;
      try {
        Map<String, List<String>> requiredTitles = new HashMap<>();
        File spreadsheetFile = spreadsheetsFiles.get(0);
        currentSpreadsheetId = spreadsheetFile.getId();
        SheetsToPull sheetsToPull = getSheetsToPull();
        switch (sheetsToPull) {
          case ALL:
            requiredTitles.put(currentSpreadsheetId,
              sheetsSourceClient.getSheetsTitles(currentSpreadsheetId));
            break;
          case TITLES:
            requiredTitles.put(currentSpreadsheetId, getSheetsIdentifiers());
            break;
          case NUMBERS:
            requiredTitles.put(currentSpreadsheetId, sheetsSourceClient.getSheetsTitles(currentSpreadsheetId,
              getSheetsIdentifiers().stream().map(Integer::parseInt).collect(Collectors.toList())));
            break;
          default:
            collector.addFailure(String.format("'%s' is not processed value.", sheetsToPull.toString()), null)
              .withConfigProperty(SHEETS_TO_PULL);
        }

        // get needed row numbers
        int columnNamesRow = getColumnNamesRow();
        int subColumnNamesRow = columnNamesRow + 1;
        int firstDataRow = getActualFirstDataRow();
        if (subColumnNamesRow == getActualFirstDataRow()) {
          firstDataRow = subColumnNamesRow + 1;
        }

        int lastDataColumn = getLastDataColumn();

        if (!getColumnNamesSelection().equals(HeaderSelection.NO_COLUMN_NAMES)) {
          LinkedHashMap<Integer, ColumnComplexSchemaInfo> resultHeaderTitles = new LinkedHashMap<>();
          Map.Entry<String, List<String>> fileTitles = requiredTitles.entrySet().iterator().next();
          currentSpreadsheetId = fileTitles.getKey();
          currentSheetTitle = fileTitles.getValue().get(0);

          // get rows for columns and data
          MergesForNumeredRows headerDataRows = sheetsSourceClient.getSingleRows(currentSpreadsheetId,
            currentSheetTitle, new HashSet<>(Arrays.asList(columnNamesRow, subColumnNamesRow, firstDataRow)));

          List<CellData> columnsRow = headerDataRows.getNumeredRows().get(columnNamesRow);
          List<CellData> subColumnsRow = headerDataRows.getNumeredRows().get(subColumnNamesRow);
          List<CellData> dataRow = headerDataRows.getNumeredRows().get(firstDataRow);
          if (CollectionUtils.isEmpty(columnsRow)) {
            collector.addFailure(
              String.format("No headers found for row '%d', spreadsheet id '%s', sheet title '%s'.",
                columnNamesRow, currentSpreadsheetId, currentSheetTitle), null)
              .withConfigProperty(CUSTOM_COLUMN_NAMES_ROW);
            return;
          }
          List<GridRange> merges = headerDataRows.getMergeRanges() == null ?
            Collections.emptyList() : headerDataRows.getMergeRanges();

          // invalid merges starts from rows before column one
          List<GridRange> invalidColumnMerges = merges.stream()
            .filter(gr -> gr.getStartRowIndex() < columnNamesRow - 1)
            .collect(Collectors.toList());
          if (!invalidColumnMerges.isEmpty()) {
            collector.addFailure(String.format("Invalid merged cells for first column row."),
              "Column row shouldn't have merges cells from previous rows.")
              .withConfigProperty(CUSTOM_COLUMN_NAMES_ROW);
          }

          // analyse horizontal merges (that start at column row and have length more that 1 cell) for column row
          // if there ane no valid merges, update data row
          List<GridRange> columnMerges = merges.stream()
            .filter(gr -> gr.getStartRowIndex() == columnNamesRow - 1 && gr.getEndRowIndex() == columnNamesRow
              && gr.getEndColumnIndex() - gr.getStartColumnIndex() > 1)
            .collect(Collectors.toList());
          if (columnMerges.isEmpty()) {
            dataRow = subColumnsRow;
          }
          lastDataColumn = lastDataColumn == 0 ? columnsRow.size() : lastDataColumn;
          resultHeaderTitles = processColumns(columnsRow, subColumnsRow, dataRow, columnMerges,
                                              lastDataColumn, collector);
          if (collector.getValidationFailures().isEmpty()) {
            dataSchemaInfo = resultHeaderTitles;
          }
        } else {
          // read first row of data and get column number
          // if row is empty use last column

          Map.Entry<String, List<String>> firstFileTitles = requiredTitles.entrySet().iterator().next();
          MergesForNumeredRows firstRowData = sheetsSourceClient.getSingleRows(firstFileTitles.getKey(),
            firstFileTitles.getValue().get(0), Collections.singleton(firstDataRow));
          List<CellData> dataCells = firstRowData.getNumeredRows().get(firstDataRow);
          lastDataColumn = lastDataColumn == 0 ? dataCells.size() : lastDataColumn;
          if (CollectionUtils.isEmpty(dataCells)) {
            dataSchemaInfo = defaultGeneratedHeaders(lastDataColumn);
          } else {
            dataSchemaInfo = defaultGeneratedHeaders(Math.min(dataCells.size(), lastDataColumn));
          }
        }
      } catch (IOException e) {
        collector.addFailure(
          String.format("Failed to prepare headers, spreadsheet id: '%s', sheet title: '%s'.",
            currentSpreadsheetId, currentSheetTitle), null);
      }
    }
  }

  private LinkedHashMap<Integer, ColumnComplexSchemaInfo> processColumns(List<CellData> columnsRow,
                                                                         List<CellData> subColumnsRow,
                                                                         List<CellData> dataRow,
                                                                         List<GridRange> columnMerges,
                                                                         int lastDataColumn,
                                                                         FailureCollector collector) {
    LinkedHashMap<Integer, ColumnComplexSchemaInfo> columnHeaders = new LinkedHashMap<>();
    final Map<String, Integer> seenFieldNames = new HashMap<>();
    List<String> headerTitles = new ArrayList<>();
    for (int i = 0; i < Math.min(columnsRow.size(), lastDataColumn); i++) {
      CellData columnHeaderCell = columnsRow.get(i);
      int index = i;
      GridRange partOfMerge = columnMerges.stream()
        .filter(gr -> gr.getStartColumnIndex() <= index && gr.getEndColumnIndex() >= index)
        .findAny().orElse(null);
      boolean isMergeHead = columnMerges.stream().anyMatch(gr -> gr.getStartColumnIndex() == index);

      // skip headers which are in merge but are not heads
      if (partOfMerge != null && !isMergeHead) {
        continue;
      }
      String title = columnHeaderCell.getFormattedValue();
      if (StringUtils.isNotEmpty(title)) {
        title = checkTitleFormat(title, seenFieldNames, i);

        // for merge we should analyse sub headers for data schemas
        if (isMergeHead) {
          int mergeLength = partOfMerge.getEndColumnIndex() - partOfMerge.getStartColumnIndex();
          ColumnComplexSchemaInfo recordColumn = new ColumnComplexSchemaInfo(title, null);
          recordColumn.addSubColumns(processSubHeaders(index, mergeLength, subColumnsRow, dataRow, collector));
          columnHeaders.put(index, recordColumn);
        } else {
          Schema dataSchema = getDataCellSchema(dataRow, i, title);
          columnHeaders.put(index, new ColumnComplexSchemaInfo(title, dataSchema));
        }
        if (headerTitles.contains(title)) {
          collector.addFailure(String.format("Duplicate column name '%s'.", title),
            null);
        }
        headerTitles.add(title);
      }
    }
    return columnHeaders;
  }

  private List<ColumnComplexSchemaInfo> processSubHeaders(int startIndex, int length, List<CellData> subColumnsRow,
                                                          List<CellData> dataRow, FailureCollector collector) {
    List<ColumnComplexSchemaInfo> subHeaders = new ArrayList<>();
    final Map<String, Integer> seenFieldNames = new HashMap<>();
    List<String> titles = new ArrayList<>();
    for (int i = startIndex; i < startIndex + length; i++) {
      String subHeaderTitle;
      if (subColumnsRow != null && subColumnsRow.size() > i) {
        subHeaderTitle = subColumnsRow.get(i).getFormattedValue();
        if (StringUtils.isEmpty(subHeaderTitle)) {
          subHeaderTitle = ColumnAddressConverter.getColumnName(i + 1);
        }
        subHeaderTitle = checkTitleFormat(subHeaderTitle, seenFieldNames, i);
      } else {
        subHeaderTitle = ColumnAddressConverter.getColumnName(i + 1);
      }
      if (titles.contains(subHeaderTitle)) {
        collector.addFailure(String.format("Duplicate sub-column name '%s'.", subHeaderTitle),
          null);
        return subHeaders;
      }
      titles.add(subHeaderTitle);

      // get data schema
      Schema dataSchema = getDataCellSchema(dataRow, i, subHeaderTitle);

      subHeaders.add(new ColumnComplexSchemaInfo(subHeaderTitle, dataSchema));
    }
    return subHeaders;
  }

  private String checkTitleFormat(String title, Map<String, Integer> seenFieldNames, int columnIndex) {
    if (getColumnNameCleansingEnabled()) {
      return applyFileTitleConvention(title, seenFieldNames);
    }
    return applySheetTitleConvention(title, columnIndex);
  }

  private String applyFileTitleConvention(String title, Map<String, Integer> seenFieldNames) {
    final String replacementChar = "_";

    StringBuilder cleanFieldNameBuilder = new StringBuilder();

    // Remove any spaces at the end of the strings
    title = title.trim();

    // If it's an empty string replace it with BLANK
    if (title.isEmpty()) {
      cleanFieldNameBuilder.append("BLANK");
    } else if (Character.isDigit(title.charAt(0))) {
      // Prepend a col_ if the first character is a number
      cleanFieldNameBuilder.append("col_");
    }

    // Replace all invalid characters with the replacement char
    cleanFieldNameBuilder.append(NOT_VALID_PATTERN.matcher(title).replaceAll(replacementChar));

    String cleanFieldName = cleanFieldNameBuilder.toString();
    String lowerCaseCleanFieldName = cleanFieldName.toLowerCase();
    int count = seenFieldNames.getOrDefault(lowerCaseCleanFieldName, 0) + 1;
    seenFieldNames.put(lowerCaseCleanFieldName, count);
    // In case column already exists in seenFieldNames map, append the count with column name.
    if (count > 1) {
      cleanFieldNameBuilder.append(replacementChar).append(count);
    }
    return cleanFieldNameBuilder.toString();
  }

  private String applySheetTitleConvention(String title, int columnIndex) {
    if (!COLUMN_NAME.matcher(title).matches()) {
      String defaultColumnName = ColumnAddressConverter.getColumnName(columnIndex + 1);
      LOG.warn(String.format("Original column name '%s' doesn't satisfy column name requirements '%s', " +
                               "the default column name '%s' will be used.", title, COLUMN_NAME.pattern(),
                             defaultColumnName));
      return defaultColumnName;
    }
    return title;
  }

  private Schema getDataCellSchema(List<CellData> dataRow, int index, String headerName) {
    Schema dataSchema = Schema.of(Schema.Type.STRING);
    if (dataRow != null && dataRow.size() > index) {
      CellData dataCell = dataRow.get(index);
      dataSchema = getCellSchema(dataCell);
    } else {
      LOG.warn(String.format("There is empty data cell for '%s' column during data types defining.",
        headerName));
    }
    return dataSchema;
  }

  /**
   * Returns the int.
   * @return The int
   */
  public int getActualFirstDataRow() {
    int firstDataRow = 1;
    if (isExtractMetadata() && getLastHeaderRow() > 0) {
      firstDataRow = getLastHeaderRow() + 1;
    }
    if (!getColumnNamesSelection().equals(HeaderSelection.NO_COLUMN_NAMES)) {
      // check for headers number
      int headerRows = 1;
      if (dataSchemaInfo.values().stream().anyMatch(si -> si.getSubColumns().size() > 0)) {
        headerRows = 2;
      }
      firstDataRow = Math.max(firstDataRow, getColumnNamesRow() + headerRows);
    }
    return firstDataRow;
  }

  /**
   * Returns the int.
   * @return The int
   */
  public int getActualLastDataRow(int recordsInSheet) {
    int lastDataRow = getAutoDetectRowsAndColumns() ? recordsInSheet : getLastDataRow();
    if (isExtractMetadata() && getFirstFooterRow() > 0) {
      lastDataRow = Math.min(lastDataRow, getFirstFooterRow() - 1);
    }
    return lastDataRow;
  }

  private Schema getCellSchema(CellData cellData) {
    if (cellData == null || cellData.size() == 0) {
      return Schema.of(Schema.Type.STRING);
    }
    ExtendedValue value = cellData.getEffectiveValue();
    if (value != null) {
      if (value.getNumberValue() != null) {
        CellFormat userEnteredFormat = cellData.getUserEnteredFormat();
        if (userEnteredFormat != null && userEnteredFormat.getNumberFormat() != null) {
          String type = userEnteredFormat.getNumberFormat().getType();
          switch (type) {
            case "DATE":
              return Schema.of(Schema.LogicalType.DATE);
            case "TIME":
              return Schema.of(Schema.Type.LONG);
            case "DATE_TIME":
              return Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS);
          }
        }
        if (getFormatting().equals(Formatting.VALUES_ONLY)) {
          return Schema.of(Schema.Type.DOUBLE);
        } else {
          return Schema.of(Schema.Type.STRING);
        }
      } else if (value.getBoolValue() != null) {
        return Schema.of(Schema.Type.BOOLEAN);
      }
    }
    return Schema.of(Schema.Type.STRING);
  }

  private LinkedHashMap<Integer, ColumnComplexSchemaInfo> defaultGeneratedHeaders(int length) {
    LinkedHashMap<Integer, ColumnComplexSchemaInfo> headers = new LinkedHashMap<>();
    for (int i = 1; i <= length; i++) {
      headers.put(i - 1, new ColumnComplexSchemaInfo(ColumnAddressConverter.getColumnName(i),
        Schema.of(Schema.Type.STRING)));
    }
    return headers;
  }

  private void validateMetadata(FailureCollector collector) {
    if (!containsMacro(EXTRACT_METADATA) && !containsMacro(METADATA_CELLS)
      && !containsMacro(FIRST_HEADER_ROW) && !containsMacro(LAST_HEADER_ROW)
      && !containsMacro(FIRST_FOOTER_ROW) && !containsMacro(LAST_FOOTER_ROW) && extractMetadata) {

      if (getFirstHeaderRow() == 0 && getLastHeaderRow() == 0
        && getFirstFooterRow() == 0 && getLastFooterRow() == 0) {
        collector.addFailure("No header or footer rows specified.", null)
          .withConfigProperty(FIRST_HEADER_ROW)
          .withConfigProperty(LAST_HEADER_ROW)
          .withConfigProperty(FIRST_FOOTER_ROW)
          .withConfigProperty(LAST_FOOTER_ROW);
        return;
      }
      if ((getFirstHeaderRow() == 0) != (getLastHeaderRow() == 0)) {
        collector.addFailure("Both first and last rows for header should be either specified or not.", null)
          .withConfigProperty(FIRST_HEADER_ROW)
          .withConfigProperty(LAST_HEADER_ROW);
        return;
      }
      if ((getFirstFooterRow() == 0) != (getLastFooterRow() == 0)) {
        collector.addFailure("Both first and last rows for footer should be either specified or not.", null)
          .withConfigProperty(FIRST_FOOTER_ROW)
          .withConfigProperty(LAST_FOOTER_ROW);
        return;
      }
      if (getFirstHeaderRow() > getLastHeaderRow()) {
        collector.addFailure("First row header index cannot be less than last row header index.", null)
          .withConfigProperty(FIRST_HEADER_ROW)
          .withConfigProperty(LAST_HEADER_ROW);
        return;
      }
      if (getFirstFooterRow() > getLastFooterRow()) {
        collector.addFailure("First row footer index cannot be less than last row footer index.", null)
          .withConfigProperty(FIRST_FOOTER_ROW)
          .withConfigProperty(LAST_FOOTER_ROW);
        return;
      }
      // we should have at least one data row
      if (getLastHeaderRow() + 1 >= getFirstFooterRow()) {
        collector.addFailure("Header and footer are intersected or there are no data rows between them.",
          null)
          .withConfigProperty(LAST_HEADER_ROW)
          .withConfigProperty(FIRST_FOOTER_ROW);
        return;
      }

      if (!containsMacro(COLUMN_NAMES_SELECTION) && !containsMacro(CUSTOM_COLUMN_NAMES_ROW)
        && !getColumnNamesSelection().equals(HeaderSelection.NO_COLUMN_NAMES)) {
        int headerRowNumber = getColumnNamesRow();
        int headerRowsCount = 1;
        if (dataSchemaInfo.values().stream().anyMatch(si -> si.getSubColumns().size() > 0)) {
          headerRowsCount = 2;
        }
        if (getFirstHeaderRow() <= headerRowNumber + headerRowsCount - 1 && getLastHeaderRow() >= headerRowNumber) {
          collector.addFailure("Metadata header rows coincides with column header row (or rows).", null)
            .withConfigProperty(FIRST_HEADER_ROW).withConfigProperty(LAST_HEADER_ROW);
        }
        if (getFirstFooterRow() <= headerRowNumber + headerRowsCount - 1 && getLastFooterRow() >= headerRowNumber) {
          collector.addFailure("Metadata footer rows coincides with column header row (or rows).", null)
            .withConfigProperty(FIRST_FOOTER_ROW).withConfigProperty(LAST_FOOTER_ROW);
        }
      }

      if (!containsMacro(METADATA_FIELD_NAME)) {
        List<String> columnNames = dataSchemaInfo.values().stream().map(c -> c.getHeaderTitle())
          .collect(Collectors.toList());
        if (columnNames.contains(metadataFieldName)) {
          collector.addFailure(String.format("Metadata record name '%s' coincides with one of the column names.",
            metadataFieldName), null).withConfigProperty(METADATA_FIELD_NAME);
        }
      }

      validateMetadataCells(collector);
    }
  }

  private Map<String, String> validateMetadataCells(FailureCollector collector) {
    Map<String, String> pairs = metadataInputToMap(getMetadataCells());
    Set<String> keys = new HashSet<>();
    Set<String> values = new HashSet<>();
    for (Map.Entry<String, String> pairEntry : pairs.entrySet()) {
      String keyAddress = pairEntry.getKey();
      String valueAddress = pairEntry.getValue();
      if (validateMetadataAddress(collector, keyAddress) & validateMetadataAddress(collector, valueAddress)) {
        if (keys.contains(keyAddress)) {
          collector.addFailure(String.format("Duplicate key address '%s'.", keyAddress), null)
            .withConfigProperty(METADATA_CELLS);
        }
        keys.add(keyAddress);
        if (values.contains(valueAddress)) {
          collector.addFailure(String.format("Duplicate value address '%s'.", valueAddress), null)
            .withConfigProperty(METADATA_CELLS);
        }
        values.add(valueAddress);
      }
    }
    return pairs;
  }

  private boolean validateMetadataAddress(FailureCollector collector, String address) {
    Matcher m = CELL_ADDRESS.matcher(address);
    if (m.find()) {
      Integer row = Integer.parseInt(m.group(2));
      if ((row < getFirstHeaderRow() || row > getLastHeaderRow()) ==
        (row < getFirstFooterRow() || row > getLastFooterRow())) {
        collector.addFailure(String.format("Metadata cell '%s' is out of header or footer rows.", address),
          null)
          .withConfigProperty(METADATA_CELLS);
        return false;
      }
    } else {
      collector.addFailure(String.format("Invalid cell address '%s'.", address), null)
        .withConfigProperty(METADATA_CELLS);
      return false;
    }
    return true;
  }

  private Map<String, String> metadataInputToMap(String input) {
    return Arrays.stream(input.split(",")).map(p -> p.split(":"))
      .filter(p -> p.length == 2)
      .collect(Collectors.toMap(p -> p[0], p -> p[1]));
  }

  public Formatting getFormatting() {
    return Formatting.fromValue(formatting);
  }

  public boolean isSkipEmptyData() {
    return skipEmptyData;
  }

  public HeaderSelection getColumnNamesSelection() {
    return HeaderSelection.fromValue(columnNamesSelection);
  }

  /**
   * Returns the int.
   * @return The int
   */
  public int getColumnNamesRow() {
    int firstRowIndex = 1;
    if (getColumnNamesSelection().equals(HeaderSelection.FIRST_ROW_AS_COLUMNS)) {
      return firstRowIndex;
    }
    return customColumnNamesRow == null ? firstRowIndex : customColumnNamesRow;
  }

  public boolean isExtractMetadata() {
    return extractMetadata;
  }

  public int getFirstHeaderRow() {
    return firstHeaderRow == null ? 0 : firstHeaderRow;
  }

  public int getLastHeaderRow() {
    return lastHeaderRow == null ? 0 : lastHeaderRow;
  }

  public int getFirstFooterRow() {
    return firstFooterRow == null ? 0 : firstFooterRow;
  }

  public int getLastFooterRow() {
    return lastFooterRow == null ? 0 : lastFooterRow;
  }

  @Nullable
  public boolean getAutoDetectRowsAndColumns() {
    return Boolean.TRUE.equals(autoDetectRowsAndColumns);
  }

  @Nullable
  public boolean getColumnNameCleansingEnabled() {
    return Boolean.TRUE.equals(columnNameCleansingEnabled);
  }

  public Integer getLastDataColumn() {
    return lastDataColumn == null ? 0 : Integer.parseInt(lastDataColumn);
  }

  public Integer getLastDataRow() {
    return lastDataRow == null ? 0 : Integer.parseInt(lastDataRow);
  }

  public String getMetadataCells() {
    return metadataCells == null ? "" : metadataCells;
  }

  public String getMetadataFieldName() {
    return metadataFieldName;
  }

  public SheetsToPull getSheetsToPull() {
    return SheetsToPull.fromValue(sheetsToPull);
  }

  @Nullable
  public List<String> getSheetsIdentifiers() {
    return Arrays.asList(sheetsIdentifiers.split(","));
  }

  /**
   * Returns the map which have key is Integer and value is map.
   * @return The Map which have key is Integer and value is map
   */
  public Map<Integer, Map<String, List<String>>> getHeaderTitlesRow() {
    Map<Integer, Map<String, List<String>>> titles = new HashMap<>();
    for (Map.Entry<Integer, ColumnComplexSchemaInfo> entry : dataSchemaInfo.entrySet()) {
      int index = entry.getKey();
      ColumnComplexSchemaInfo schemaInfo = entry.getValue();
      titles.put(index, new HashMap<>());
      if (schemaInfo.getSubColumns().isEmpty()) {
        titles.get(index).put(schemaInfo.getHeaderTitle(), Collections.emptyList());
      } else {
        titles.get(index).put(schemaInfo.getHeaderTitle(), schemaInfo.getSubColumns().stream()
          .map(sc -> sc.getHeaderTitle()).collect(Collectors.toList()));
      }
    }

    return titles;
  }

  /**
   * Returns the list of MetadataKeyValueAddress.
   * @return the List of MetadataKeyValueAddress
   */
  public List<MetadataKeyValueAddress> getMetadataCoordinates() {
    List<MetadataKeyValueAddress> metadataCoordinates = new ArrayList<>();
    if (extractMetadata) {
      Map<String, String> keyValuePairs = metadataInputToMap(getMetadataCells());

      for (Map.Entry<String, String> pair : keyValuePairs.entrySet()) {
        metadataCoordinates.add(new MetadataKeyValueAddress(toCoordinate(pair.getKey()),
          toCoordinate(pair.getValue())));
      }
    }
    return metadataCoordinates;
  }

  private CellCoordinate toCoordinate(String address) {
    Matcher m = CELL_ADDRESS.matcher(address);
    if (m.find()) {
      return new CellCoordinate(Integer.parseInt(m.group(2)), ColumnAddressConverter.getNumberOfColumn(m.group(1)));
    }
    throw new IllegalArgumentException(String.format("Cannot to parse '%s' cell address.", address));
  }

  public Integer getReadBufferSize() {
    return readBufferSize == null ? 100 : readBufferSize;
  }

  public Boolean getAddNameFields() {
    return addNameFields != null && addNameFields;
  }

  @Nullable
  public String getSpreadsheetFieldName() {
    return spreadsheetFieldName;
  }

  @Nullable
  public String getSheetFieldName() {
    return sheetFieldName;
  }

  private static GoogleSheetsSourceConfig of(String referenceName) {
    return new GoogleSheetsSourceConfig(referenceName);
  }

  public void setSheetsToPull(String sheetsToPull) {
    this.sheetsToPull = sheetsToPull;
  }

  public void setSheetsIdentifiers(String sheetsIdentifiers) {
    this.sheetsIdentifiers = sheetsIdentifiers;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public void setFormatting(String formatting) {
    this.formatting = formatting;
  }

  public void setSkipEmptyData(Boolean skipEmptyData) {
    this.skipEmptyData = skipEmptyData;
  }

  public void setColumnNamesSelection(String columnNamesSelection) {
    this.columnNamesSelection = columnNamesSelection;
  }

  public void setCustomColumnNamesRow(Integer customColumnNamesRow) {
    this.customColumnNamesRow = customColumnNamesRow;
  }

  public void setMetadataFieldName(String metadataFieldName) {
    this.metadataFieldName = metadataFieldName;
  }

  public void setExtractMetadata(Boolean extractMetadata) {
    this.extractMetadata = extractMetadata;
  }

  public void setFirstHeaderRow(Integer firstHeaderRow) {
    this.firstHeaderRow = firstHeaderRow;
  }

  public void setLastHeaderRow(Integer lastHeaderRow) {
    this.lastHeaderRow = lastHeaderRow;
  }

  public void setFirstFooterRow(Integer firstFooterRow) {
    this.firstFooterRow = firstFooterRow;
  }

  public void setLastFooterRow(Integer lastFooterRow) {
    this.lastFooterRow = lastFooterRow;
  }

  public void setAutoDetectRowsAndColumns(boolean autoDetectRowsAndColumns) {
    this.autoDetectRowsAndColumns = autoDetectRowsAndColumns;
  }

  public void setLastDataColumn(String lastDataColumn) {
    this.lastDataColumn = lastDataColumn;
  }

  public void setLastDataRow(String lastDataRow) {
    this.lastDataRow = lastDataRow;
  }

  public void setMetadataCells(String metadataCells) {
    this.metadataCells = metadataCells;
  }

  public void setReadBufferSize(Integer bufferSize) {
    this.readBufferSize = bufferSize;
  }

  public void setAddNameFields(Boolean addNameFields) {
    this.addNameFields = addNameFields;
  }

  public void setSpreadsheetFieldName(String spreadsheetFieldName) {
    this.spreadsheetFieldName = spreadsheetFieldName;
  }

  public void setSheetFieldName(String sheetFieldName) {
    this.sheetFieldName = sheetFieldName;
  }

  public void setFilter(String filter) {
    this.filter = filter;
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

  public void setColumnNameCleansingEnabled(@Nullable Boolean columnNameCleansingEnabled) {
    this.columnNameCleansingEnabled = columnNameCleansingEnabled;
  }

  public static GoogleSheetsSourceConfig of(JsonObject properties) throws IOException {

    GoogleSheetsSourceConfig googleSheetsSourceConfig = GoogleSheetsSourceConfig
      .of(properties.get(GoogleSheetsSourceConfig.REFERENCE_NAME).getAsString());

    if (properties.has(GoogleSheetsSourceConfig.SHEETS_TO_PULL)) {
      googleSheetsSourceConfig.setSheetsToPull(properties.get(GoogleSheetsSourceConfig.SHEETS_TO_PULL).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.SHEETS_IDENTIFIERS)) {
      googleSheetsSourceConfig.setSheetsIdentifiers(
        properties.get(GoogleSheetsSourceConfig.SHEETS_IDENTIFIERS).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.SCHEMA)) {
      googleSheetsSourceConfig.setSchema(
        properties.get(GoogleSheetsSourceConfig.SCHEMA).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.FORMATTING)) {
      googleSheetsSourceConfig.setFormatting(
        properties.get(GoogleSheetsSourceConfig.FORMATTING).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.SKIP_EMPTY_DATA)) {
      googleSheetsSourceConfig.setSkipEmptyData(
        Boolean.valueOf(properties.get(GoogleSheetsSourceConfig.SKIP_EMPTY_DATA).getAsString()));
    }

    if (properties.has(GoogleSheetsSourceConfig.COLUMN_NAMES_SELECTION)) {
      googleSheetsSourceConfig.setColumnNamesSelection(
        properties.get(GoogleSheetsSourceConfig.COLUMN_NAMES_SELECTION).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.CUSTOM_COLUMN_NAMES_ROW)) {
      googleSheetsSourceConfig.setCustomColumnNamesRow(
        Integer.valueOf(properties.get(GoogleSheetsSourceConfig.CUSTOM_COLUMN_NAMES_ROW).getAsString()));
    }

    if (properties.has(GoogleSheetsSourceConfig.METADATA_FIELD_NAME)) {
      googleSheetsSourceConfig.setMetadataFieldName(
        properties.get(GoogleSheetsSourceConfig.METADATA_FIELD_NAME).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.EXTRACT_METADATA)) {
      googleSheetsSourceConfig.setExtractMetadata(
        Boolean.valueOf(properties.get(GoogleSheetsSourceConfig.EXTRACT_METADATA).getAsString()));
    }

    if (properties.has(GoogleSheetsSourceConfig.FIRST_HEADER_ROW)) {
      googleSheetsSourceConfig.setFirstHeaderRow(
        Integer.valueOf(properties.get(GoogleSheetsSourceConfig.FIRST_HEADER_ROW).getAsString()));
    }

    if (properties.has(GoogleSheetsSourceConfig.LAST_HEADER_ROW)) {
      googleSheetsSourceConfig.setLastHeaderRow(
        Integer.valueOf(properties.get(GoogleSheetsSourceConfig.LAST_HEADER_ROW).getAsString()));
    }

    if (properties.has(GoogleSheetsSourceConfig.FIRST_FOOTER_ROW)) {
      googleSheetsSourceConfig.setFirstFooterRow(
        Integer.valueOf(properties.get(GoogleSheetsSourceConfig.FIRST_FOOTER_ROW).getAsString()));
    }

    if (properties.has(GoogleSheetsSourceConfig.LAST_FOOTER_ROW)) {
      googleSheetsSourceConfig.setLastFooterRow(
        Integer.valueOf(properties.get(GoogleSheetsSourceConfig.LAST_FOOTER_ROW).getAsString()));
    }

    if (properties.has(GoogleSheetsSourceConfig.LAST_DATA_COLUMN)) {
      googleSheetsSourceConfig.setLastDataColumn(
        properties.get(GoogleSheetsSourceConfig.LAST_DATA_COLUMN).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.LAST_DATA_ROW)) {
      googleSheetsSourceConfig.setLastDataRow(
        properties.get(GoogleSheetsSourceConfig.LAST_DATA_ROW).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.METADATA_CELLS)) {
      googleSheetsSourceConfig.setMetadataCells(
        properties.get(GoogleSheetsSourceConfig.METADATA_CELLS).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.READ_BUFFER_SIZE)) {
      googleSheetsSourceConfig.setReadBufferSize(
        Integer.valueOf(properties.get(GoogleSheetsSourceConfig.READ_BUFFER_SIZE).getAsString()));
    }

    if (properties.has(GoogleSheetsSourceConfig.ADD_NAME_FIELDS)) {
      googleSheetsSourceConfig.setAddNameFields(
        Boolean.valueOf(properties.get(GoogleSheetsSourceConfig.ADD_NAME_FIELDS).getAsString()));
    }

    if (properties.has(GoogleSheetsSourceConfig.SPREADSHEET_FIELD_NAME)) {
      googleSheetsSourceConfig.setSpreadsheetFieldName(
        properties.get(GoogleSheetsSourceConfig.SPREADSHEET_FIELD_NAME).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.SHEET_FIELD_NAME)) {
      googleSheetsSourceConfig.setSheetFieldName(
        properties.get(GoogleSheetsSourceConfig.SHEET_FIELD_NAME).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.FILTER)) {
      googleSheetsSourceConfig.setFilter(
        properties.get(GoogleSheetsSourceConfig.FILTER).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.MODIFICATION_DATE_RANGE)) {
      googleSheetsSourceConfig.setModificationDateRange(
        properties.get(GoogleSheetsSourceConfig.MODIFICATION_DATE_RANGE).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.START_DATE)) {
      googleSheetsSourceConfig.setStartDate(
        properties.get(GoogleSheetsSourceConfig.START_DATE).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.END_DATE)) {
      googleSheetsSourceConfig.setEndDate(
        properties.get(GoogleSheetsSourceConfig.END_DATE).getAsString());
    }
    
    if (properties.has(GoogleSheetsSourceConfig.AUTH_TYPE)) {
      googleSheetsSourceConfig.setAuthType(
        properties.get(GoogleSheetsSourceConfig.AUTH_TYPE).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.NAME_SERVICE_ACCOUNT_TYPE)) {
      googleSheetsSourceConfig.setServiceAccountType(
        properties.get(GoogleSheetsSourceConfig.NAME_SERVICE_ACCOUNT_TYPE).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.NAME_SERVICE_ACCOUNT_JSON)) {
      googleSheetsSourceConfig.setServiceAccountJson(
        properties.get(GoogleSheetsSourceConfig.NAME_SERVICE_ACCOUNT_JSON).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.ACCOUNT_FILE_PATH)) {
      googleSheetsSourceConfig.setAccountFilePath(
        properties.get(GoogleSheetsSourceConfig.ACCOUNT_FILE_PATH).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.CLIENT_ID)) {
      googleSheetsSourceConfig.setClientId(properties.get(GoogleSheetsSourceConfig.CLIENT_ID).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.CLIENT_SECRET)) {
      googleSheetsSourceConfig.setClientSecret(properties.get(GoogleSheetsSourceConfig.CLIENT_SECRET).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.REFRESH_TOKEN)) {
      googleSheetsSourceConfig.setRefreshToken(properties.get(GoogleSheetsSourceConfig.REFRESH_TOKEN).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.DIRECTORY_IDENTIFIER)) {
      googleSheetsSourceConfig.setDirectoryIdentifier(
        properties.get(GoogleSheetsSourceConfig.DIRECTORY_IDENTIFIER).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.OAUTH_METHOD)) {
      googleSheetsSourceConfig.setoAuthMethod(
        properties.get(GoogleSheetsSourceConfig.OAUTH_METHOD).getAsString());
    }

    if (properties.has(GoogleSheetsSourceConfig.ACCESS_TOKEN)) {
      googleSheetsSourceConfig.setAccessToken(
        properties.get(GoogleSheetsSourceConfig.ACCESS_TOKEN).getAsString());
    }
    if (properties.has(GoogleSheetsSourceConfig.FILE_IDENTIFIER)) {
      googleSheetsSourceConfig.setFileIdentifier(
        properties.get(GoogleSheetsSourceConfig.FILE_IDENTIFIER).getAsString());
    }
    if (properties.has(GoogleSheetsSourceConfig.IDENTIFIER_TYPE)) {
      googleSheetsSourceConfig.setIdentifierType(
        properties.get(GoogleSheetsSourceConfig.IDENTIFIER_TYPE).getAsString());
    }
    if (properties.has(GoogleSheetsSourceConfig.AUTO_DETECT_ROWS_AND_COLUMNS)) {
      googleSheetsSourceConfig.setAutoDetectRowsAndColumns(
        properties.get(GoogleSheetsSourceConfig.AUTO_DETECT_ROWS_AND_COLUMNS).getAsBoolean());
    }

    if (properties.has(GoogleSheetsSourceConfig.CLEANSE_COLUMN_NAMES)) {
      googleSheetsSourceConfig.setColumnNameCleansingEnabled(
        properties.get(GoogleSheetsSourceConfig.CLEANSE_COLUMN_NAMES).getAsBoolean());
    }

    return googleSheetsSourceConfig;
  }
}
