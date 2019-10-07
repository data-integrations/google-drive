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

package io.cdap.plugin.google.drive.sink;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.services.drive.model.File;
import io.cdap.plugin.google.drive.common.FileFromFolder;
import io.cdap.plugin.google.drive.common.GoogleDriveClient;

import java.io.IOException;
import java.util.Collections;

/**
 * Client for writing data via Google Drive API.
 */
public class GoogleDriveSinkClient extends GoogleDriveClient<GoogleDriveSinkConfig> {

  public GoogleDriveSinkClient(GoogleDriveSinkConfig config) {
    super(config);
  }

  @Override
  protected String getRequiredScope() {
    return FULL_PERMISSIONS_SCOPE;
  }

  public void createFile(FileFromFolder fileFromFolder) throws IOException {
    String folderId = config.getDirectoryIdentifier();

    File fileToWrite = new File();

    fileToWrite.setName(fileFromFolder.getFile().getName());
    fileToWrite.setMimeType(fileFromFolder.getFile().getMimeType());
    fileToWrite.setParents(Collections.singletonList(folderId));
    ByteArrayContent fileContent = new ByteArrayContent(null, fileFromFolder.getContent());
    service.files().create(fileToWrite, fileContent).execute();
  }
}
