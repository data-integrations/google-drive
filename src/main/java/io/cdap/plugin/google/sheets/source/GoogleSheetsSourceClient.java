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

package io.cdap.plugin.google.sheets.source;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.GridData;
import com.google.api.services.sheets.v4.model.GridRange;
import com.google.api.services.sheets.v4.model.RowData;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import io.cdap.plugin.google.common.APIRequestRetryer;
import io.cdap.plugin.google.sheets.common.GoogleSheetsClient;
import io.cdap.plugin.google.sheets.source.utils.CellCoordinate;
import io.cdap.plugin.google.sheets.source.utils.ColumnAddressConverter;
import io.cdap.plugin.google.sheets.source.utils.ComplexMultiValueColumn;
import io.cdap.plugin.google.sheets.source.utils.MergesForNumeredRows;
import io.cdap.plugin.google.sheets.source.utils.MetadataKeyValueAddress;
import io.cdap.plugin.google.sheets.source.utils.MultipleRowRecord;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Client for getting data via Google Sheets API.
 */
public class GoogleSheetsSourceClient extends GoogleSheetsClient<GoogleSheetsSourceConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleSheetsSourceClient.class);

  public GoogleSheetsSourceClient(GoogleSheetsSourceConfig config) throws IOException {
    super(config);
  }

  @Override
  protected List<String> getRequiredScopes() {
    return Collections.singletonList(SheetsScopes.SPREADSHEETS_READONLY);
  }

  public List<com.google.api.services.sheets.v4.model.Sheet> getSheets(String spreadsheetId)
    throws ExecutionException, RetryException {
    Retryer<List<com.google.api.services.sheets.v4.model.Sheet>> sheetsRetryer = APIRequestRetryer.getRetryer(config,
      String.format("Get spreadsheet, id: '%s'.", spreadsheetId));
    return sheetsRetryer.call(() -> {
      Spreadsheet spreadsheet = service.spreadsheets().get(spreadsheetId).execute();
      return spreadsheet.getSheets();
    });
  }

  public List<String> getSheetsTitles(String spreadsheetId, List<Integer> indexes)
    throws ExecutionException, RetryException {
    Retryer<List<String>> sheetTitlesRetryer = APIRequestRetryer.getRetryer(config,
      String.format("Get sheet titles, spreadsheet id: '%s'.", spreadsheetId));
    return sheetTitlesRetryer.call(() -> {
      Spreadsheet spreadsheet = service.spreadsheets().get(spreadsheetId).execute();
      return spreadsheet.getSheets().stream().filter(s -> indexes.contains(s.getProperties().getIndex()))
        .map(s -> s.getProperties().getTitle()).collect(Collectors.toList());
    });
  }

  public List<String> getSheetsTitles(String spreadsheetId) throws ExecutionException, RetryException {
    Retryer<List<String>> sheetsTitlesRetryer = APIRequestRetryer.getRetryer(config,
      String.format("Get sheet titles, spreadsheet id: '%s'.", spreadsheetId));
    return sheetsTitlesRetryer.call(() -> {
      Spreadsheet spreadsheet = service.spreadsheets().get(spreadsheetId).execute();
      return spreadsheet.getSheets().stream().map(s -> s.getProperties().getTitle()).collect(Collectors.toList());
    });
  }

  /**
   * Method that retrieves multiple subsequent rows from sheet. Also it separates merged cells with replacing
   * empty values with value in main cell of merge.
   * Metadata info is retrieved if needed.
   *
   * @param spreadsheetId id of the spreadsheet.
   * @param sheetTitle title of sheet.
   * @param rowNumber number of start row to read.
   * @param length number of rows to read.
   * @param resolvedHeaders complex header.
   * @param metadataCoordinates coordinates of metadata cells.
   * @return multiple rows mapped to complex headers.
   * @throws ExecutionException
   * @throws RetryException
   * @throws IOException
   */
  public MultipleRowRecord getContent(String spreadsheetId, String sheetTitle, int rowNumber, int length,
                                      Map<Integer, Map<String, List<String>>> resolvedHeaders,
                                      List<MetadataKeyValueAddress> metadataCoordinates)
    throws ExecutionException, RetryException, IOException {

    Sheets.Spreadsheets.Get contentRequest =
      prepareContentRequest(spreadsheetId, sheetTitle, rowNumber, length, metadataCoordinates);

    Retryer<Spreadsheet> contentRetryer = APIRequestRetryer.getRetryer(config,
      String.format("Get content, spreadsheet id: '%s', sheet title: '%s', row number: '%d'.",
        spreadsheetId, sheetTitle, rowNumber));
    Spreadsheet spreadsheet = contentRetryer.call(() -> contentRequest.execute());
    checkSingleSheetRetrieved(spreadsheet);

    Sheet resultSheet = spreadsheet.getSheets().get(0);
    List<GridData> grids = resultSheet.getData();

    // process merged data cells
    List<GridRange> mergeRanges = resultSheet.getMerges();
    if (CollectionUtils.isNotEmpty(mergeRanges)) {

      // get all not available head cell in current data range
      List<String> rangesToCall = getMissedHeadCellRanges(sheetTitle, grids, mergeRanges);

      // make API call for not available head cells
      Map<GridRange, CellData> retriedHeads = getMissedCells(rangesToCall, spreadsheetId);
      replaceMergeCells(grids, mergeRanges, length, retriedHeads);
    }

    Map<String, ComplexMultiValueColumn> headers = new HashMap<>();
    Map<String, String> metadata = new HashMap<>();
    for (GridData gridData : grids) {
      List<RowData> rows = gridData.getRowData();
      int startRow = gridData.getStartRow() == null ? 0 : gridData.getStartRow();
      startRow++;
      if (startRow == rowNumber) {
        headers = parseData(resolvedHeaders, rows);
      } else if (startRow == config.getFirstHeaderRow() || startRow == config.getFirstFooterRow()) {
        if (CollectionUtils.isNotEmpty(rows)) {
          // retrieve header and footer metadata
          metadata.putAll(parseMetadata(metadataCoordinates, rows, startRow));
        }
      } else {
        throw new IllegalStateException(String.format("Range with invalid start row '%d'.", startRow));
      }
    }

    return new MultipleRowRecord(spreadsheet.getProperties().getTitle(), sheetTitle, metadata, headers,
      spreadsheet.getSheets().get(0).getMerges());
  }

  /**
   * Method that prepares get request for retrieving of content and metadata cells.
   *
   * @param spreadsheetId id of the spreadsheet.
   * @param sheetTitle title of the sheet.
   * @param rowNumber number of start row to read.
   * @param length number of rows to read.
   * @param metadataCoordinates coordinates of metadata cells.
   * @return {@link Sheets.Spreadsheets.Get} request.
   * @throws IOException
   */
  private Sheets.Spreadsheets.Get prepareContentRequest(String spreadsheetId, String sheetTitle, int rowNumber,
                                                        int length, List<MetadataKeyValueAddress> metadataCoordinates)
    throws IOException {
    String dataRange = String.format("%s!%d:%d", sheetTitle, rowNumber, rowNumber + length - 1);
    String headerRange = null;
    String footerRange = null;
    if (config.isExtractMetadata() && CollectionUtils.isNotEmpty(metadataCoordinates)) {
      if (config.getFirstHeaderRow() > 0) {
        headerRange = String.format("%s!%d:%d", sheetTitle,
          config.getFirstHeaderRow(), config.getLastHeaderRow());
      }
      if (config.getFirstFooterRow() > 0) {
        footerRange = String.format("%s!%d:%d", sheetTitle,
          config.getFirstFooterRow(), config.getLastFooterRow());
      }
    }

    Sheets.Spreadsheets.Get request = service.spreadsheets().get(spreadsheetId);
    List<String> ranges = new ArrayList<>();
    ranges.add(dataRange);
    if (headerRange != null) {
      ranges.add(headerRange);
    }
    if (footerRange != null) {
      ranges.add(footerRange);
    }
    request.setRanges(ranges);
    request.setIncludeGridData(true);

    return request;
  }

  /**
   * Method that prepares range values for all head cells from merge ranges that are out of data rows.
   *
   * @param sheetTitle source sheet title.
   * @param dataGrids data rows.
   * @param mergeRanges ranges of merged cells.
   * @return ranges in A1 notation for merge heads that are out of data rows.
   */
  private List<String> getMissedHeadCellRanges(String sheetTitle, List<GridData> dataGrids,
                                               List<GridRange> mergeRanges) {
    List<String> rangesToCall = new ArrayList<>();
    for (GridData gridData : dataGrids) {
      int startRow = gridData.getStartRow() == null ? 0 : gridData.getStartRow();
      for (GridRange range : mergeRanges) {
        if (!isHeadAvailableInDataRange(range, startRow)) {
          rangesToCall.add(String.format("%s!%s%d:%s%d",
            sheetTitle,
            ColumnAddressConverter.getColumnName(range.getStartColumnIndex() + 1),
            range.getStartRowIndex() + 1,
            ColumnAddressConverter.getColumnName(range.getStartColumnIndex() + 1),
            range.getStartRowIndex() + 1));
        }
      }
    }
    return rangesToCall;
  }

  private boolean isHeadAvailableInDataRange(GridRange mergeRange, int dataStartRow) {
    return mergeRange.getStartRowIndex() >= dataStartRow;
  }

  /**
   * Method that calls Sheets API for required single cells.
   *
   * @param rangesToCall list of ranges for single cells.
   * @param spreadsheetId id of the spreadsheet.
   * @return map of cell ranges to cell values.
   * @throws ExecutionException
   * @throws RetryException
   * @throws IOException
   */
  private Map<GridRange, CellData> getMissedCells(List<String> rangesToCall, String spreadsheetId)
    throws ExecutionException, RetryException, IOException {
    Map<GridRange, CellData> retriedHeads = new HashMap<>();
    if (!rangesToCall.isEmpty()) {
      Sheets.Spreadsheets.Get headCellsRequest = service.spreadsheets().get(spreadsheetId);
      headCellsRequest.setRanges(rangesToCall);
      headCellsRequest.setIncludeGridData(true);
      Retryer<Spreadsheet> headCellsRetryer = APIRequestRetryer.getRetryer(config,
        "Get additional cells for merge resolving.");
      Spreadsheet headesSpreadsheet = headCellsRetryer.call(() -> headCellsRequest.execute());
      checkSingleSheetRetrieved(headesSpreadsheet);


      Sheet headsSheet = headesSpreadsheet.getSheets().get(0);
      for (GridData gridData : headsSheet.getData()) {
        int headRow = gridData.getStartRow() == null ? 0 : gridData.getStartRow();
        int headColumn = gridData.getStartColumn() == null ? 0 : gridData.getStartColumn();
        CellData headCell = null;
        if (gridData.getRowData() != null && gridData.getRowData().size() > 0
          && gridData.getRowData().get(0).getValues() != null
          && gridData.getRowData().get(0).getValues().size() > 0) {
          headCell = gridData.getRowData().get(0).getValues().get(0);
        }
        retriedHeads.put(new GridRange().setStartRowIndex(headRow).setEndRowIndex(headRow + 1)
            .setStartColumnIndex(headColumn).setEndColumnIndex(headColumn + 1),
          headCell);
      }
    }
    return retriedHeads;
  }

  /**
   * Method that replaces all cells inside all merges with value for merge (from head cell of merge).
   *
   * @param dataGrids data to process.
   * @param mergeRanges ranges for merged cells.
   * @param expectedDataRowsNumber number of rows should be in data.
   * @param missedHeadCells values for head cells that are out of data range.
   */
  private void replaceMergeCells(List<GridData> dataGrids, List<GridRange> mergeRanges, int expectedDataRowsNumber,
                                 Map<GridRange, CellData> missedHeadCells) {
    for (GridData gridData : dataGrids) {
      List<RowData> rows = gridData.getRowData();
      int startRow = gridData.getStartRow() == null ? 0 : gridData.getStartRow();
      for (GridRange range : mergeRanges) {
        // get head value
        CellData headCell;
        if (isHeadAvailableInDataRange(range, startRow)) {
          int headCellRowIndex = range.getStartRowIndex() - startRow;
          if (rows == null || rows.size() <= headCellRowIndex) {
            continue;
          }
          if (rows.get(headCellRowIndex).getValues().size() <= range.getStartColumnIndex()) {
            continue;
          }

          headCell = rows.get(headCellRowIndex).getValues().get(range.getStartColumnIndex());
        } else {
          // get cell value from additional API request results
          headCell = missedHeadCells.get(new GridRange()
            .setStartRowIndex(range.getStartRowIndex())
            .setEndRowIndex(range.getStartRowIndex() + 1)
            .setStartColumnIndex(range.getStartColumnIndex())
            .setEndColumnIndex(range.getStartColumnIndex() + 1));
        }

        // skip if head cell is empty
        if (headCell == null || headCell.size() == 0) {
          continue;
        }

        // replace with head value all cells from merge range, which are placed inside retrieved cells
        replaceCell(range, headCell, rows, startRow, expectedDataRowsNumber);
      }
    }
  }

  /**
   * Method that replaces all cells from a single merge with required value. If needed new rows/columns are adding.
   *
   * @param mergeRange range for merged cells.
   * @param headCell value of head cell of merge range.
   * @param dataRows data to process.
   * @param dataStartRow index of the first row of the data relative to the start of sheet.
   * @param expectedDataRowsNumber number of rows should be in data.
   */
  private void replaceCell(GridRange mergeRange, CellData headCell, List<RowData> dataRows, int dataStartRow,
                           int expectedDataRowsNumber) {
    for (int i = mergeRange.getStartRowIndex(); i < mergeRange.getEndRowIndex(); i++) {
      int cellRowIndex = i - dataStartRow;
      if (cellRowIndex < 0 || cellRowIndex >= expectedDataRowsNumber) {
        continue;
      }
      for (int j = mergeRange.getStartColumnIndex(); j < mergeRange.getEndColumnIndex(); j++) {
        if (dataRows == null) {
          dataRows = new ArrayList<>();
        }
        while (dataRows.size() <= cellRowIndex) {
          dataRows.add(new RowData());
        }
        if (dataRows.get(cellRowIndex).getValues() == null) {
          dataRows.get(cellRowIndex).setValues(new ArrayList<>());
        }
        while (dataRows.get(cellRowIndex).getValues().size() <= j) {
          dataRows.get(cellRowIndex).getValues().add(new CellData());
        }
        dataRows.get(cellRowIndex).getValues().set(j, headCell);
      }
    }
  }

  /**
   * Method that retrieves metadata info from data rows.
   *
   * @param metadataCoordinates coordinates of metadata cells.
   * @param rows data rows to process.
   * @param startRow index of the first row of the data relative to the start of sheet.
   * @return
   */
  private Map<String, String> parseMetadata(List<MetadataKeyValueAddress> metadataCoordinates, List<RowData> rows,
                                            int startRow) {
    Map<String, String> metadata = new HashMap<>();
    for (MetadataKeyValueAddress metadataCoordinate : metadataCoordinates) {
      CellCoordinate nameCoordinate = metadataCoordinate.getNameCoordinate();
      CellCoordinate valueCoordinate = metadataCoordinate.getValueCoordinate();
      String name = getCellValue(nameCoordinate, rows, startRow);
      String value = getCellValue(valueCoordinate, rows, startRow);
      if (StringUtils.isNotEmpty(name)) {
        metadata.put(name, value);
      }
    }
    return metadata;
  }

  /**
   * Method that retrieves all values for all headers.
   *
   * @param resolvedHeaders required headers to populate.
   * @param rows data to process.
   * @return populated complex headers.
   */
  private Map<String, ComplexMultiValueColumn> parseData(Map<Integer, Map<String, List<String>>> resolvedHeaders,
                                                         List<RowData> rows) {
    Map<String, ComplexMultiValueColumn> headers = new HashMap<>();
    for (Map.Entry<Integer, Map<String, List<String>>> headerEntry : resolvedHeaders.entrySet()) {
      String headerName = headerEntry.getValue().keySet().iterator().next();
      List<String> subHeaderNames = headerEntry.getValue().get(headerName);
      int columnIndex = headerEntry.getKey();
      headers.put(headerName, new ComplexMultiValueColumn());
      if (CollectionUtils.isEmpty(subHeaderNames)) {
        scanRowsAndPopulateHeader(rows, headers.get(headerName), columnIndex);
      } else {
        Map<String, ComplexMultiValueColumn> subHeaders = new HashMap<>();
        for (int i = 0; i < subHeaderNames.size(); i++) {
          String subHeaderName = subHeaderNames.get(i);
          subHeaders.put(subHeaderName, new ComplexMultiValueColumn());
          scanRowsAndPopulateHeader(rows, subHeaders.get(subHeaderName), columnIndex + i);
        }
        headers.get(headerName).setSubColumns(subHeaders);
      }
    }
    return headers;
  }

  /**
   * Method that go through all rows and collects value for specified header.
   *
   * @param rows data to process.
   * @param header header to populate with values.
   * @param columnIndex index of column relative to start of the sheet.
   */
  private void scanRowsAndPopulateHeader(List<RowData> rows, ComplexMultiValueColumn header, int columnIndex) {
    if (rows == null) {
      header.addData(null);
      return;
    }
    for (RowData rowData : rows) {
      if (rowData == null) {
        header.addData(null);
      } else {
        List<CellData> cells = rowData.getValues();
        if (cells == null || cells.size() <= columnIndex) {
          header.addData(null);
        } else {
          header.addData(cells.get(columnIndex));
        }
      }
    }
  }

  /**
   * Method that retrieves cell value according to cell's relative coordinate.
   *
   * @param coordinate cell's relative coordinate.
   * @param rows data to process.
   * @param startRow index of the first row of the data relative to the start of sheet.
   * @return string value;
   */
  private String getCellValue(CellCoordinate coordinate, List<RowData> rows, int startRow) {
    int rowIndexInList = coordinate.getRowNumber() - startRow;
    if (rows.size() > rowIndexInList && rowIndexInList >= 0) {
      RowData rowData = rows.get(rowIndexInList);
      int columnIndex = coordinate.getColumnNumber() - 1;
      if (rowData.getValues().size() > columnIndex && columnIndex >= 0) {
        return rowData.getValues().get(columnIndex).getFormattedValue();
      }
    }
    return "";
  }

  /**
   * Method that retrieves data for separate rows with related merge ranges.
   *
   * @param spreadsheetId id of the spreadsheet.
   * @param sheetTitle title of the sheet.
   * @param rowNumbers list of the rows required to retrieve.
   * @return wrapper for rows with merge ranges.
   * @throws IOException
   */
  public MergesForNumeredRows getSingleRows(String spreadsheetId, String sheetTitle,
                                            Set<Integer> rowNumbers) throws IOException {
    Map<Integer, List<CellData>> result = new HashMap<>();

    Sheets.Spreadsheets.Get request = service.spreadsheets().get(spreadsheetId);
    List<String> ranges = rowNumbers.stream().map(n -> String.format("%s!%2$d:%2$d", sheetTitle, n))
      .collect(Collectors.toList());
    request.setRanges(ranges);
    request.setIncludeGridData(true);
    Spreadsheet response = request.execute();
    checkSingleSheetRetrieved(response);

    List<GridData> grids = response.getSheets().get(0).getData();
    List<GridRange> merges = response.getSheets().get(0).getMerges();
    for (GridData gridData : grids) {
      List<RowData> rows = gridData.getRowData();
      int startRow = gridData.getStartRow() == null ? 0 : gridData.getStartRow();
      startRow++;
      for (Integer rowNumber : rowNumbers) {
        if (startRow == rowNumber) {
          if (rows == null) {
            result.put(rowNumber, null);
          } else if (rows.size() > 1) {
            throw new RuntimeException(String.format("Excess rows during data retrieving."));
          } else {
            result.put(rowNumber, rows.get(0).getValues());
          }
        }

      }
    }
    return new MergesForNumeredRows(merges, result);
  }

  private void checkSingleSheetRetrieved(Spreadsheet spreadsheet) {
    if (CollectionUtils.isEmpty(spreadsheet.getSheets()) || spreadsheet.getSheets().size() > 1) {
      throw new RuntimeException(String.format("Invalid number of sheets were returned: '%d'.",
        spreadsheet.getSheets() == null ? 0 : spreadsheet.getSheets().size()));
    }
  }
}
