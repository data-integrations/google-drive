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
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Strings;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

/**
 * Base client for working with Google Drive API.
 *
 * @param <C> configuration.
 */
public class GoogleDriveClient<C extends GoogleAuthBaseConfig> {
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  protected final Drive service;
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
    this.service = getDriveClient();
  }

  /**
   * Generates drive client for Google Drive API based on the authentication type.
   * @return {@link Drive} client.
   * @throws IOException on issues with service account file reading.
   */
  protected Drive getDriveClient() throws IOException {
    Drive drive;
    AuthType authType = config.getAuthType();
    switch (authType) {
      case OAUTH2:
        drive = new Drive.Builder(httpTransport, JSON_FACTORY, getOAuth2Credential()).build();
        break;
      case SERVICE_ACCOUNT:
        drive =
            new Drive.Builder(httpTransport, JSON_FACTORY, getServiceAccountCredential()).build();
        break;
      default:
        throw new IllegalStateException(
            String.format("Untreated value '%s' for authentication type.", authType));
    }
    return drive;
  }

  protected Credential getOAuth2Credential() {
    GoogleCredential credential;
    GoogleCredential.Builder builder = new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(JSON_FACTORY);
    if (OAuthMethod.ACCESS_TOKEN.equals(config.getOAuthMethod())) {
      credential = builder.build();
      credential.createScoped(getRequiredScopes()).setAccessToken(config.getAccessToken());
    } else {
      credential = builder.setClientSecrets(config.getClientId(),
                                            config.getClientSecret()).build();
      credential.createScoped(getRequiredScopes()).setRefreshToken(config.getRefreshToken());
    }
    return credential;
  }

  protected HttpCredentialsAdapter getServiceAccountCredential() throws IOException {
    GoogleCredentials googleCredentials;
    List<String> scopes = getRequiredScopes();
    if (Boolean.TRUE.equals(config.isServiceAccountJson())) {
      InputStream jsonInputStream = new ByteArrayInputStream(
          config.getServiceAccountJson().getBytes(StandardCharsets.UTF_8));
      googleCredentials = GoogleCredentials.fromStream(jsonInputStream).createScoped(scopes);
    } else if (Boolean.TRUE.equals(config.isServiceAccountFilePath()) && !Strings.isNullOrEmpty(
        config.getServiceAccountFilePath())) {
      googleCredentials =
          GoogleCredentials.fromStream(
              new FileInputStream(config.getServiceAccountFilePath())).createScoped(scopes);
    } else {
      googleCredentials =
          GoogleCredentials.getApplicationDefault().createScoped(scopes);
    }
    return new HttpCredentialsAdapter(googleCredentials);
  }

  protected List<String> getRequiredScopes() {
    return Collections.singletonList(DriveScopes.DRIVE_READONLY);
  }

  public void isFolderAccessible(String folderId) throws IOException {
    service.files().get(folderId).setSupportsAllDrives(true).execute();
  }

  public void isFileAccessible(String fileId) throws IOException {
    service.files().get(fileId).setSupportsAllDrives(true).execute();
  }
}
