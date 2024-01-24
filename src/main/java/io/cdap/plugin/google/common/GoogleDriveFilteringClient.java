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

package io.cdap.plugin.google.common;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.common.base.Strings;
import io.cdap.plugin.google.common.utils.DateRange;
import io.cdap.plugin.google.common.utils.ExportedType;
import io.cdap.plugin.google.common.utils.ModifiedDateRangeUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Base Google Drive Class with files search functionality.
 *
 * @param <C> configuration.
 */
public class GoogleDriveFilteringClient<C extends GoogleFilteringSourceConfig> extends GoogleDriveClient<C> {
  public static final String MODIFIED_TIME_TERM = "modifiedTime";
  public static final String DRIVE_FOLDER_MIME = "application/vnd.google-apps.folder";
  public static final String DRIVE_DOCS_MIME_PREFIX = "application/vnd.google-apps.";
  public static final String DRIVE_DOCUMENTS_MIME = "application/vnd.google-apps.document";
  public static final String DRIVE_SPREADSHEETS_MIME = "application/vnd.google-apps.spreadsheet";
  public static final String DRIVE_DRAWINGS_MIME = "application/vnd.google-apps.drawing";
  public static final String DRIVE_PRESENTATIONS_MIME = "application/vnd.google-apps.presentation";
  public static final String DRIVE_APPS_SCRIPTS_MIME = "application/vnd.google-apps.script";

  public GoogleDriveFilteringClient(C config) throws IOException {
    super(config);
  }

  public List<File> getFilesSummary(List<ExportedType> exportedTypes) throws ExecutionException, RetryException {
    return getFilesSummary(exportedTypes, 0);
  }

  /**
   * Returns the list of file.
   * @param exportedTypes the exported types are provided with
   * @param filesNumber the number of files is provided
   * @return The list of file
   * @throws ExecutionException if there was an error getting the column information for the execution
   * @throws RetryException if there was an error getting the column information for the retry
   */
  public List<File> getFilesSummary(List<ExportedType> exportedTypes, int filesNumber)
      throws ExecutionException, RetryException {
    Retryer<List<File>> filesSummaryRetryer = APIRequestRetryer.getRetryer(
      String.format("Get files summary, files: '%d'.", filesNumber));
    return filesSummaryRetryer.call(() -> {
      List<File> files = new ArrayList<>();
      String nextToken = "";
      int retrievedFiles = 0;
      int actualFilesNumber = filesNumber;
      if (config.getFileIdentifier() != null) {
        files.add(service.files().get(config.getFileIdentifier()).setSupportsAllDrives(true).execute());
        return files;
      }
      Drive.Files.List request = service.files().list()
        .setSupportsAllDrives(true)
        .setIncludeItemsFromAllDrives(true)
        .setQ(generateFilter(exportedTypes))
        .setFields("nextPageToken, files(id, size)");
      if (actualFilesNumber > 0) {
        request.setPageSize(actualFilesNumber);
      } else {
        actualFilesNumber = 0;
      }
      while (nextToken != null && (actualFilesNumber == 0 || retrievedFiles < actualFilesNumber)) {
        FileList result = request.execute();
        files.addAll(result.getFiles());
        nextToken = result.getNextPageToken();
        request.setPageToken(nextToken);
        retrievedFiles += result.size();
      }
      return actualFilesNumber == 0 || files.size() <= actualFilesNumber ?
          files :
          files.subList(0, actualFilesNumber);

    });
  }

  private String generateFilter(List<ExportedType> exportedTypes) throws InterruptedException {
    StringBuilder sb = new StringBuilder();

    // prepare parent
    sb.append("'");
    sb.append(config.getDirectoryIdentifier());
    sb.append("' in parents");

    // prepare query for non folders
    sb.append(" and mimeType != '");
    sb.append(DRIVE_FOLDER_MIME);
    sb.append("'");

    if (!exportedTypes.isEmpty()) {
      sb.append(" and (");
      for (ExportedType exportedType : exportedTypes) {
        if (exportedType.equals(ExportedType.BINARY)) {
          sb.append(" not mimeType contains '");
          sb.append(exportedType.getRelatedMIME());
          sb.append("' or");
        } else {
          sb.append(" mimeType = '");
          sb.append(exportedType.getRelatedMIME());
          sb.append("' or");
        }
      }
      // delete last 'or'
      sb.delete(sb.length() - 3, sb.length());
      sb.append(")");
    }

    String filter = config.getFilter();
    if (!Strings.isNullOrEmpty(filter)) {
      sb.append(" and ");
      sb.append(filter);
    }

    DateRange modifiedDateRange = ModifiedDateRangeUtils.getDataRange(config.getModificationDateRangeType(),
        config.getStartDate(), config.getEndDate());
    if (modifiedDateRange != null) {
      sb.append(" and ");
      sb.append(ModifiedDateRangeUtils.getFilterValue(modifiedDateRange));
    }

    return sb.toString();
  }
}
