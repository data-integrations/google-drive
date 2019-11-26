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

import com.github.rholder.retry.RetryException;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import io.cdap.plugin.google.common.APIRequestRetryer;
import io.cdap.plugin.google.common.GoogleDriveClient;
import io.cdap.plugin.google.drive.common.FileFromFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Client for writing data via Google Drive API.
 */
public class GoogleDriveSinkClient extends GoogleDriveClient<GoogleDriveSinkConfig> {

  public GoogleDriveSinkClient(GoogleDriveSinkConfig config) throws IOException {
    super(config);
  }

  public void createFile(FileFromFolder fileFromFolder) throws ExecutionException, RetryException {
    APIRequestRetryer.getRetryer(config,
      String.format("Creating of file with name '%s'.", fileFromFolder.getFile().getName()))
      .call(() -> {
        String folderId = config.getDirectoryIdentifier();

        File fileToWrite = new File();

        fileToWrite.setName(fileFromFolder.getFile().getName());
        fileToWrite.setMimeType(fileFromFolder.getFile().getMimeType());
        fileToWrite.setParents(Collections.singletonList(folderId));
        ByteArrayContent fileContent = new ByteArrayContent(fileFromFolder.getFile().getMimeType(),
          fileFromFolder.getContent());
        service.files().create(fileToWrite, fileContent).execute();
        return null;
      });
  }

  @Override
  protected List<String> getRequiredScopes() {
    return Collections.singletonList(DriveScopes.DRIVE);
  }
}
