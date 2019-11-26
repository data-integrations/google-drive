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

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.common.base.Strings;
import io.cdap.plugin.google.drive.source.utils.DateRange;
import io.cdap.plugin.google.drive.source.utils.ExportedType;
import io.cdap.plugin.google.drive.source.utils.ModifiedDateRangeUtils;

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
  public static final String DRIVE_FOLDER_MIME = "application/vnd.google-apps.folder";

  public GoogleDriveFilteringClient(C config) throws IOException {
    super(config);
  }

  public List<File> getFilesSummary(List<ExportedType> exportedTypes) throws ExecutionException, RetryException {
    return getFilesSummary(exportedTypes, 0);
  }

  public List<File> getFilesSummary(List<ExportedType> exportedTypes, int filesNumber)
      throws ExecutionException, RetryException {
    Retryer<List<File>> filesSummaryRetryer = APIRequestRetryer.getRetryer(config,
        String.format("Get files summary, files: '%d'.", filesNumber));
    return filesSummaryRetryer.call(() -> {
      List<File> files = new ArrayList<>();
      String nextToken = "";
      int retrievedFiles = 0;
      int actualFilesNumber = filesNumber;
      Drive.Files.List request = service.files().list()
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
