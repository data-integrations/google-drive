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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import io.cdap.plugin.google.common.exceptions.InvalidPropertyType;
import io.cdap.plugin.google.common.exceptions.InvalidPropertyTypeException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;

/**
 * Base client for working with Google Drive API
 *
 * @param <C> configuration
 */
public abstract class GoogleDriveClient<C extends GoogleDriveBaseConfig> {
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static final String ROOT_FOLDER_ID = "root";
  protected static final String FULL_PERMISSIONS_SCOPE = "https://www.googleapis.com/auth/drive";
  protected Drive service;
  protected final C config;
  private NetHttpTransport httpTransport;

  public GoogleDriveClient(C config) {
    this.config = config;
    try {
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    } catch (Exception e) {
      throw new RuntimeException("There was issue during communicating with Google Drive API.", e);
    }
    service = new Drive.Builder(httpTransport, JSON_FACTORY, getCredentials())
      .build();
  }

  private Credential getCredentials() {
    GoogleCredential credential;

    // TODO fix authentication after OAuth2 will be provided by cdap
    // So for now plugins require user or service account json
    // start of workaround
    try {
      AuthType authType = config.getAuthType();
      switch (authType) {
        case OAUTH2:
          credential = new GoogleCredential.Builder()
            .setTransport(httpTransport)
            .setJsonFactory(JSON_FACTORY)
            .setClientSecrets(config.getClientId(),
                              config.getClientSecret())
            .build();
          credential.setRefreshToken(config.getRefreshToken());
          break;
        case SERVICE_ACCOUNT:
          String accountFilePath = config.getAccountFilePath();
          if (GoogleDriveBaseConfig.AUTO_DETECT_VALUE.equals(accountFilePath)) {
            credential = GoogleCredential.getApplicationDefault();
          } else {
            credential = GoogleCredential.fromStream(new FileInputStream(accountFilePath));
          }
          break;
        default:
          throw new InvalidPropertyTypeException(InvalidPropertyType.AUTH_TYPE, authType.toString());
      }

      return credential.createScoped(Collections.singleton(getRequiredScope()));
    } catch (IOException e) {
      throw new RuntimeException(String.format("There was issue while loading account file: %s", e.getMessage()), e);
    }
    // end of workaround
  }

  protected abstract String getRequiredScope();

  public void checkRootFolder() throws IOException {
    service.files().get(ROOT_FOLDER_ID).execute();
  }

  public void isFolderAccessible(String folderId) throws IOException {
    service.files().get(folderId).execute();
  }
}
