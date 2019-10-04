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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.google.common.exceptions.InvalidPropertyTypeException;

import java.io.File;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Base Google Drive batch config. Contains common configuration properties and methods.
 */
public abstract class GoogleDriveBaseConfig extends PluginConfig {
  public static final String AUTO_DETECT_VALUE = "auto-detect";
  public static final String REFERENCE_NAME = "referenceName";
  public static final String AUTH_TYPE = "authType";
  public static final String AUTH_TYPE_LABEL = "Auth type";
  public static final String CLIENT_ID = "clientId";
  public static final String CLIENT_ID_LABEL = "Client ID";
  public static final String CLIENT_SECRET = "clientSecret";
  public static final String CLIENT_SECRET_LABEL = "Client secret";
  public static final String REFRESH_TOKEN = "refreshToken";
  public static final String REFRESH_TOKEN_LABEL = "Refresh token";
  public static final String ACCOUNT_FILE_PATH = "accountFilePath";
  public static final String DIRECTORY_IDENTIFIER = "directoryIdentifier";

  private static final String IS_SET_FAILURE_MESSAGE_PATTERN = "'%s' property is empty or macro is not available";

  @Name(REFERENCE_NAME)
  @Description("Reference Name")
  protected String referenceName;

  // TODO remove these properties after OAuth2 will be provided by cdap
  // start of workaround
  @Name(AUTH_TYPE)
  @Description("Type of authentication used to access Google API. \n" +
    "OAuth2 and Service account types are available. Default is OAuth2.")
  @Macro
  protected String authType;

  @Nullable
  @Name(CLIENT_ID)
  @Description("OAuth2 client id. " +
    "Is shown only when 'OAuth2' auth type is selected for 'Authentication type' property.")
  @Macro
  protected String clientId;

  @Nullable
  @Name(CLIENT_SECRET)
  @Description("OAuth2 client secret. " +
    "Is shown only when 'OAuth2' auth type is selected for 'Authentication type' property.")
  @Macro
  protected String clientSecret;

  @Nullable
  @Name(REFRESH_TOKEN)
  @Description("OAuth2 refresh token. " +
    "Is shown only when 'OAuth2' auth type is selected for 'Authentication type' property.")
  @Macro
  protected String refreshToken;

  @Nullable
  @Name(ACCOUNT_FILE_PATH)
  @Description("Path on the local file system of the service account key used for authorization. " +
    "Is shown only when 'Service account' auth type is selected for 'Authentication type' property. " +
    "Can be set to 'auto-detect' when running on a Dataproc cluster. " +
    "When running on other clusters, the file must be present on every node in the cluster." +
    "Service account json can be generated on Google Cloud " +
    "Service Account page (https://console.cloud.google.com/iam-admin/serviceaccounts)")
  @Macro
  protected String accountFilePath;
  // end of workaround

  @Name(DIRECTORY_IDENTIFIER)
  @Description("ID of target directory, the last part of the URL.")
  @Macro
  protected String directoryIdentifier;

  public void validate(FailureCollector collector) {
    IdUtils.validateReferenceName(referenceName, collector);

    if (validateAuthType(collector)) {
      AuthType authType = getAuthType();
      boolean propertiesAreValid;
      switch (authType) {
        case OAUTH2:
          propertiesAreValid = validateOAuth2Properties(collector);
          break;
        case SERVICE_ACCOUNT:
          propertiesAreValid = validateAccountFilePath(collector);
          break;
        default:
          throw new InvalidPropertyTypeException(GoogleDriveBaseConfig.AUTH_TYPE_LABEL, authType.toString());
      }
      if (propertiesAreValid) {
        try {
          GoogleDriveClient client = getDriveClient();

          // validate auth
          validateCredentials(collector, client);

          // validate directory
          validateDirectoryIdentifier(collector, client);

        } catch (Exception e) {
          collector.addFailure(
            String.format("Exception during authentication/directory properties check: %s", e.getMessage()),
            "Check message and reconfigure the plugin")
            .withStacktrace(e.getStackTrace());
        }
      }
    }
  }

  protected abstract GoogleDriveClient getDriveClient();

  private boolean validateAuthType(FailureCollector collector) {
    if (!containsMacro(AUTH_TYPE)) {
      try {
        getAuthType();
        return true;
      } catch (InvalidPropertyTypeException e) {
        collector.addFailure(e.getMessage(), null)
          .withConfigProperty(AUTH_TYPE);
        return false;
      }
    }
    return false;
  }

  private boolean validateOAuth2Properties(FailureCollector collector) {
    return checkPropertyIsSet(collector, clientId, CLIENT_ID, CLIENT_ID_LABEL)
      & checkPropertyIsSet(collector, clientSecret, CLIENT_SECRET, CLIENT_SECRET_LABEL)
      & checkPropertyIsSet(collector, refreshToken, REFRESH_TOKEN, REFRESH_TOKEN_LABEL);
  }

  private boolean validateAccountFilePath(FailureCollector collector) {
    if (!containsMacro(ACCOUNT_FILE_PATH)) {
      if (!AUTO_DETECT_VALUE.equals(accountFilePath) && !new File(accountFilePath).exists()) {
        collector.addFailure("Account file is not available",
                             "Provide path to existing account file")
          .withConfigProperty(ACCOUNT_FILE_PATH);
      } else {
        return true;
      }
    }
    return false;
  }

  private void validateCredentials(FailureCollector collector, GoogleDriveClient driveClient) throws IOException {
    try {
      driveClient.checkRootFolder();
    } catch (GoogleJsonResponseException e) {
      collector.addFailure(e.getMessage(), "Provide valid credentials")
        .withConfigProperty(ACCOUNT_FILE_PATH)
        .withStacktrace(e.getStackTrace());
    }
  }

  private void validateDirectoryIdentifier(FailureCollector collector, GoogleDriveClient driveClient)
    throws IOException {
    if (!containsMacro(DIRECTORY_IDENTIFIER)) {
      try {
        driveClient.isFolderAccessible(directoryIdentifier);
      } catch (GoogleJsonResponseException e) {
        collector.addFailure(e.getMessage(), "Provide an existing folder identifier")
          .withConfigProperty(DIRECTORY_IDENTIFIER)
          .withStacktrace(e.getStackTrace());
      }
    }
  }

  protected boolean checkPropertyIsSet(FailureCollector collector, String propertyValue, String propertyName,
                                       String propertyLabel) {
    if (!containsMacro(propertyName)) {
      if (Strings.isNullOrEmpty(propertyValue)) {
        collector.addFailure(String.format(IS_SET_FAILURE_MESSAGE_PATTERN, propertyLabel), null)
          .withConfigProperty(propertyName);
      } else {
        return true;
      }
    }
    return false;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getDirectoryIdentifier() {
    return directoryIdentifier;
  }

  public AuthType getAuthType() {
    return AuthType.fromValue(authType);
  }

  @Nullable
  public String getAccountFilePath() {
    return accountFilePath;
  }

  @Nullable
  public String getClientId() {
    return clientId;
  }

  @Nullable
  public String getClientSecret() {
    return clientSecret;
  }

  @Nullable
  public String getRefreshToken() {
    return refreshToken;
  }
}
