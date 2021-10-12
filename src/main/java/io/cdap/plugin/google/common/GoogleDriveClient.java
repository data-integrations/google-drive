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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.common.base.Strings;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

/**
 * Base client for working with Google Drive API.
 *
 * @param <C> configuration.
 */
public class GoogleDriveClient<C extends GoogleAuthBaseConfig> {
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static final String ROOT_FOLDER_ID = "root";
  protected Drive service;
  protected final C config;
  protected NetHttpTransport httpTransport;

  /**
   * Constructor for GoogleDriveClient object.
   *
   * @param config the google auth base config is provided
   * @throws IOException on issues with file reading
   */
  public GoogleDriveClient(C config) throws IOException {
    this.config = config;
    try {
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    } catch (Exception e) {
      throw new RuntimeException("There was issue during communicating with Google Drive API.", e);
    }
    service = new Drive.Builder(httpTransport, JSON_FACTORY, getCredentials())
      .build();
  }

  /**
   * Provides credential object for Google APIs that supports this credential type.
   * @return filled credential.
   * @throws IOException on issues with service account file reading.
   */
  protected Credential getCredentials() throws IOException {
    GoogleCredential credential;

    // TODO fix authentication after OAuth2 will be provided by cdap
    // So for now plugins require user or service account json
    // start of workaround
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
        if (config.isServiceAccountJson()) {
          InputStream jsonInputStream = new ByteArrayInputStream(config.getServiceAccountJson().getBytes());
          credential = GoogleCredential.fromStream(jsonInputStream);
        } else if (config.isServiceAccountFilePath() && !Strings.isNullOrEmpty(config.getServiceAccountFilePath())) {
          credential = GoogleCredential.fromStream(new FileInputStream(config.getServiceAccountFilePath()));
        } else {
          credential = GoogleCredential.getApplicationDefault(httpTransport, JSON_FACTORY);
        }
        break;
      default:
        throw new IllegalStateException(String.format("Untreated value '%s' for authentication type.", authType));
    }

    return credential.createScoped(getRequiredScopes());
    // end of workaround
  }

  protected List<String> getRequiredScopes() {
    return Collections.singletonList(DriveScopes.DRIVE_READONLY);
  }

  public void checkRootFolder() throws IOException {
    service.files().get(ROOT_FOLDER_ID).execute();
  }

  public void isFolderAccessible(String folderId) throws IOException {
    service.files().get(folderId).execute();
  }
}
