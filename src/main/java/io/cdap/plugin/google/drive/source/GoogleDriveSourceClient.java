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

package io.cdap.plugin.google.drive.source;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import io.cdap.plugin.google.common.APIRequestRetryer;
import io.cdap.plugin.google.common.GoogleDriveFilteringClient;
import io.cdap.plugin.google.drive.common.FileFromFolder;
import io.cdap.plugin.google.drive.source.utils.ExportedType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Client for getting data via Google Drive API.
 */
public class GoogleDriveSourceClient extends GoogleDriveFilteringClient<GoogleDriveSourceConfig> {

  public static final String MODIFIED_TIME_TERM = "modifiedTime";

  public static final String DRIVE_DOCS_MIME_PREFIX = "application/vnd.google-apps.";
  public static final String DRIVE_DOCUMENTS_MIME = "application/vnd.google-apps.document";
  public static final String DRIVE_SPREADSHEETS_MIME = "application/vnd.google-apps.spreadsheet";
  public static final String DRIVE_DRAWINGS_MIME = "application/vnd.google-apps.drawing";
  public static final String DRIVE_PRESENTATIONS_MIME = "application/vnd.google-apps.presentation";
  public static final String DRIVE_APPS_SCRIPTS_MIME = "application/vnd.google-apps.script";

  public static final String DEFAULT_APPS_SCRIPTS_EXPORT_MIME = "application/vnd.google-apps.script+json";

  private static final String RANGE_PATTERN = "bytes=%d-%d";

  public GoogleDriveSourceClient(GoogleDriveSourceConfig config) throws IOException {
    super(config);
  }

  @Override
  protected List<String> getRequiredScopes() {
    return Collections.singletonList(DriveScopes.DRIVE_READONLY);
  }

  public FileFromFolder getFile(String fileId) throws IOException, ExecutionException, RetryException {
    return getFilePartition(fileId, null, null);
  }

  public FileFromFolder getFilePartition(String fileId, Long bytesFrom, Long bytesTo)
      throws IOException, ExecutionException, RetryException {
    Retryer<FileFromFolder> fileFromFolderRetryer = APIRequestRetryer.getRetryer(config,
        String.format("File retrieving, id: '%s'.", fileId));
    return fileFromFolderRetryer.call(() -> {
      FileFromFolder fileFromFolder;

      Drive.Files.Get request = service.files().get(fileId).setFields("*");
      File currentFile = request.execute();

      String mimeType = currentFile.getMimeType();
      long offset = bytesFrom == null ? 0L : bytesFrom;
      if (!mimeType.startsWith(DRIVE_DOCS_MIME_PREFIX)) {
        OutputStream outputStream = new ByteArrayOutputStream();
        Drive.Files.Get get = service.files().get(currentFile.getId());

        if (bytesFrom != null && bytesTo != null) {
          get.getMediaHttpDownloader().setDirectDownloadEnabled(true);
          get.getRequestHeaders().setRange(String.format(RANGE_PATTERN, bytesFrom, bytesTo));
        }

        get.executeMediaAndDownloadTo(outputStream);
        fileFromFolder =
            new FileFromFolder(((ByteArrayOutputStream) outputStream).toByteArray(), offset, currentFile);
      } else if (mimeType.equals(DRIVE_DOCUMENTS_MIME)) {
        fileFromFolder = exportGoogleFormatFile(service, currentFile, config.getDocsExportingFormat());
      } else if (mimeType.equals(DRIVE_SPREADSHEETS_MIME)) {
        fileFromFolder = exportGoogleFormatFile(service, currentFile, config.getSheetsExportingFormat());
      } else if (mimeType.equals(DRIVE_DRAWINGS_MIME)) {
        fileFromFolder = exportGoogleFormatFile(service, currentFile, config.getDrawingsExportingFormat());
      } else if (mimeType.equals(DRIVE_PRESENTATIONS_MIME)) {
        fileFromFolder = exportGoogleFormatFile(service, currentFile, config.getPresentationsExportingFormat());
      } else if (mimeType.equals(DRIVE_APPS_SCRIPTS_MIME)) {
        fileFromFolder = exportGoogleFormatFile(service, currentFile, DEFAULT_APPS_SCRIPTS_EXPORT_MIME);
      } else {
        fileFromFolder =
            new FileFromFolder(new byte[]{}, offset, currentFile);
      }
      return fileFromFolder;
    });
  }

  // We should separate binary and Google Drive formats between two requests
  public List<File> getFilesSummary() throws ExecutionException, RetryException {
    List<ExportedType> exportedTypes = new ArrayList<>(config.getFileTypesToPull());

    // Google API doesn't support query requests with both binary and Google formats simultaneously.
    List<List<ExportedType>> exportedTypeGroups = separateFileTypesBetweenGroups(exportedTypes);

    List<File> files = new ArrayList<>();
    for (List<ExportedType> group : exportedTypeGroups) {
      if (!group.isEmpty()) {
        files.addAll(getFilesSummary(group));
      }
    }
    return files;
  }

  private List<List<ExportedType>> separateFileTypesBetweenGroups(List<ExportedType> exportedTypes) {
    List<List<ExportedType>> exportedTypeGroups = new ArrayList<>();
    if (exportedTypes.contains(ExportedType.BINARY)) {
      exportedTypeGroups.add(Collections.singletonList(ExportedType.BINARY));

      exportedTypes.remove(ExportedType.BINARY);
      exportedTypeGroups.add(exportedTypes);
    } else {
      exportedTypeGroups.add(exportedTypes);
    }
    return exportedTypeGroups;
  }

  // Google Drive API does not support partitioning for exporting Google Docs
  private FileFromFolder exportGoogleFormatFile(Drive service, File currentFile, String exportFormat)
      throws ExecutionException, RetryException {
    Retryer<FileFromFolder> fileFromFolderRetryer = APIRequestRetryer.getRetryer(config,
        String.format("File exporting, id: '%s', export format: '%s'.", currentFile.getId(), exportFormat));
    return fileFromFolderRetryer.call(() -> {
      OutputStream outputStream = new ByteArrayOutputStream();
      service.files().export(currentFile.getId(), exportFormat).executeMediaAndDownloadTo(outputStream);
      byte[] content = ((ByteArrayOutputStream) outputStream).toByteArray();
      currentFile.setMimeType(exportFormat);
      currentFile.setSize((long) content.length);
      return new FileFromFolder(content, 0L, currentFile);
    });
  }
}
