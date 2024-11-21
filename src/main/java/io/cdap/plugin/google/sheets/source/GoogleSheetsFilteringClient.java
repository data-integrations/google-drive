/*
 * Copyright © 2024 Cask Data, Inc.
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

import com.google.api.services.drive.model.File;
import io.cdap.plugin.google.common.GoogleDriveFilteringClient;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Client for getting File information via Google Sheets API.
 */
public class GoogleSheetsFilteringClient extends GoogleDriveFilteringClient<GoogleSheetsSourceConfig> {

  public GoogleSheetsFilteringClient(GoogleSheetsSourceConfig config) throws IOException {
    super(config);
  }

  @Override
  protected File getFilesSummaryByFileId() throws IOException, ExecutionException {
    File file = service.files().get(config.getFileIdentifier()).setSupportsAllDrives(true).execute();
    if (!file.getMimeType().equalsIgnoreCase(DRIVE_SPREADSHEETS_MIME)) {
      throw new ExecutionException(
        String.format("File with id: '%s' has a MIME_TYPE '%s' and is not a Google Sheets File.",
                      file.getMimeType(),
                      config.getFileIdentifier()), null);
    }
    return file;
  }
}
