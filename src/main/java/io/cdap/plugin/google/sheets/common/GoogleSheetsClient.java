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

package io.cdap.plugin.google.sheets.common;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.sheets.v4.Sheets;
import io.cdap.plugin.google.common.AuthType;
import io.cdap.plugin.google.common.GoogleAuthBaseConfig;
import io.cdap.plugin.google.common.GoogleDriveClient;

import java.io.IOException;
import java.util.List;

/**
 * Base client for working with Google Sheets API.
 *
 * @param <C> configuration.
 */
public abstract class GoogleSheetsClient<C extends GoogleAuthBaseConfig> extends GoogleDriveClient<C> {
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  protected final Sheets service;
  protected final Drive drive;

  /**
   * Constructor for GoogleSheetsClient object.
   * @param config the google auth base config is provided
   * @throws IOException on issues with file reading
   */
  public GoogleSheetsClient(C config) throws IOException {
    super(config);
    this.drive = getDriveClient();
    this.service = getSheetsClient();
  }

  /**
   * Generates sheets client for Google Sheets API based on the authentication type.
   * @return {@link Sheets} client.
   * @throws IOException on issues with service account file reading.
   */
  protected Sheets getSheetsClient() throws IOException {
    Sheets sheets;
    AuthType authType = config.getAuthType();
    switch (authType) {
      case OAUTH2:
        sheets = new Sheets.Builder(httpTransport, JSON_FACTORY, getOAuth2Credential()).build();
        break;
      case SERVICE_ACCOUNT:
        sheets =
            new Sheets.Builder(httpTransport, JSON_FACTORY, getServiceAccountCredential()).build();
        break;
      default:
        throw new IllegalStateException(
            String.format("Untreated value '%s' for authentication type.", authType));
    }
    return sheets;
  }

  protected abstract List<String> getRequiredScopes();
}
