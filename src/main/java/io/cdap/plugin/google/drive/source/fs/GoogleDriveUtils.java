/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.plugin.google.drive.source.fs;

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.common.base.Strings;
import io.cdap.plugin.google.drive.source.GoogleDriveSourceConfig;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for Google Drive operations.
 */
public class GoogleDriveUtils {
  /**
   * A set of MIME types representing Google-native files in Google Drive that are not
   * directly downloadable (e.g., Google Docs, Sheets, Slides, etc.).
   *
   * This set is used to filter out these types when listing files, ensuring only
   * downloadable files such as CSV, JSON, etc., are returned.
   *
   * Common Google-native MIME types:
   * <ul>
   *   <li>Google Docs: {@code application/vnd.google-apps.document}</li>
   *   <li>Google Sheets: {@code application/vnd.google-apps.spreadsheet}</li>
   *   <li>Google Slides: {@code application/vnd.google-apps.presentation}</li>
   *   <li>Google Forms: {@code application/vnd.google-apps.form}</li>
   *   <li>Google Drawings: {@code application/vnd.google-apps.drawing}</li>
   *   <li>Google Apps Scripts: {@code application/vnd.google-apps.script}</li>
   *   <li>Google Maps: {@code application/vnd.google-apps.map}</li>
   * </ul>
   *
   * @see <a href="https://developers.google.com/drive/api/v3/mime-types">Google Drive MIME types</a>
   */
  private static final Set<String> GOOGLE_NATIVE_MIME_TYPES = new HashSet<>(Arrays.asList(
    "application/vnd.google-apps.document",
    "application/vnd.google-apps.spreadsheet",
    "application/vnd.google-apps.presentation",
    "application/vnd.google-apps.form",
    "application/vnd.google-apps.drawing",
    "application/vnd.google-apps.script",
    "application/vnd.google-apps.map"
  ));
  private static final String FIELDS_TO_RETURN = "id, name, mimeType, size, modifiedTime";
  private static final String GOOGLE_DRIVE_FOLDER_MIME_TYPE = "application/vnd.google-apps.folder";

  /**
   * Retrieves the file ID for a given file or directory in Google Drive using its name.
   *
   * @param filePath The {@link Path} whose corresponding Google Drive file ID is to be retrieved.
   * @return The file ID as a {@link String}, or {@code null} if the file is not found.
   */
  public static String getFileId(Path filePath) {
    return filePath.getParent().getName(); // name is file name and parent is the file id
  }

  /**
   * Lists the files and directories under a specified Google Drive directory and returns
   * an array of {@link FileStatus} objects representing their metadata.
   *
   * For files, the actual size is returned. For directories, the size is set to 0.
   * All entries are marked with default permission, and placeholder owner and group values.
   *
   * @param driveService The authenticated {@link Drive} service instance used to interact with the Drive API.
   * @param dirPath      The Hadoop {@link Path} representing the target directory in Google Drive.
   * @return An array of {@link FileStatus} objects for all files and subdirectories found in the given directory.
   * @throws IOException If the directory is not found or the Drive API request fails.
   */
  public static FileStatus[] listStatus(Drive driveService, Path dirPath, String filter) throws IOException {
    List<FileStatus> fileStatuses = new ArrayList<>();

    String dirId = dirPath.getName();
    if (dirId == null) {
      throw new IOException("Directory not found: " + dirPath);
    }

    // Query Google Drive for files in the directory
    StringBuilder query = new StringBuilder("'" + dirId + "' in parents and trashed = false");
    query.append(" and ").append(getGoogleNativeExclusionQuery());
    if (!Strings.isNullOrEmpty(filter)) {
      query.append(" and ").append(filter);
    }

    FileList result = driveService.files().list()
      .setQ(query.toString())
      .setFields("files(id, name, mimeType, size, modifiedTime)")
      .setSupportsAllDrives(true)
      .setSupportsTeamDrives(true)
      .setIncludeItemsFromAllDrives(true)
      .execute();

    for (File file : result.getFiles()) {
      boolean isDirectory = GOOGLE_DRIVE_FOLDER_MIME_TYPE.equals(file.getMimeType());

      Path filePathWithFilePrefix;
      if (isDirectory) {
        filePathWithFilePrefix = new Path(String.format("%s://%s%s/%s",
                    GoogleDriveSourceConfig.GOOGLE_DRIVE_SCHEMA,
                    GoogleDriveSourceConfig.GOOGLE_DRIVE_AUTHORITY,
                    GoogleDriveSourceConfig.GOOGLE_DRIVE_FOLDER_PATH_PREFIX,
                    file.getId()));
      } else {
        filePathWithFilePrefix = new Path(String.format("%s://%s%s/%s/%s",
                    GoogleDriveSourceConfig.GOOGLE_DRIVE_SCHEMA,
                    GoogleDriveSourceConfig.GOOGLE_DRIVE_AUTHORITY,
                    GoogleDriveSourceConfig.GOOGLE_DRIVE_FILE_PATH_PREFIX,
                    file.getId(), file.getName()));
      }
      FileStatus fileStatus = new FileStatus(
        isDirectory ? 0 : file.getSize(),
        isDirectory,
        1,
        file.getSize() == null ? 0 : file.getSize(),
        file.getModifiedTime().getValue(),
        0,
        FsPermission.getDefault(),
        "owner",
        "group",
        filePathWithFilePrefix
      );

      fileStatuses.add(fileStatus);
    }

    return fileStatuses.toArray(new FileStatus[0]);
  }

  /**
   * Retrieves the status of a specific file or directory in Google Drive.
   * @param driveService The authenticated {@link Drive} service instance used to interact with the Drive API.
   * @param path The Hadoop {@link Path} representing the target file or directory.
   * @param isDir Indicates whether the path is a directory.
   * @param objectId The Google Drive file ID corresponding to the path.
   */
  public static FileStatus listObjectStatus(Drive driveService, Path path, boolean isDir, String objectId)
      throws IOException {
    File fileSummary = driveService.files()
        .get(objectId)
        .setFields(FIELDS_TO_RETURN)
        .setSupportsAllDrives(true)
        .execute();

    Path pathWithFileName = path;
    // if file name not equal to default file name, then we need to change the path with correct file name
    if (!isDir && !fileSummary.getName().equals(GoogleDriveSourceConfig.GOOGLE_DRIVE_DEFAULT_FILENAME) &&
        path.getName().equals(GoogleDriveSourceConfig.GOOGLE_DRIVE_DEFAULT_FILENAME)) {
      pathWithFileName = new Path(path.getParent(), fileSummary.getName());
    }
    return new FileStatus(
        fileSummary.getSize() == null ? 0 : fileSummary.getSize(),
        isDir,
        1,
        fileSummary.getSize() == null ? 0 : fileSummary.getSize(),
        fileSummary.getModifiedTime().getValue(),
        0,
        FsPermission.getDefault(),
        "owner",
        "group",
        pathWithFileName
    );
  }

  /**
   * Builds a Google Drive API query string that excludes all Google-native file types
   * defined in {@link #GOOGLE_NATIVE_MIME_TYPES}.
   *
   * The generated string will be used in the Drive `files().list().setQ(...)` query to
   * filter out files such as Google Docs, Sheets, Slides, etc., which are not
   * directly downloadable.
   *
   * Example output:
   * <pre>
   *   mimeType != 'application/vnd.google-apps.document' and
   *   mimeType != 'application/vnd.google-apps.spreadsheet' and
   *   ...
   * </pre>
   *
   * @return A query string that excludes all Google-native MIME types using "and" conditions.
   */
  private static String getGoogleNativeExclusionQuery() {
    return GOOGLE_NATIVE_MIME_TYPES.stream()
      .map(mime -> "mimeType != '" + mime + "'")
      .collect(Collectors.joining(" and "));
  }
}
