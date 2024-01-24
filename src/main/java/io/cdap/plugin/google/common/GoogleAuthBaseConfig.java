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
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Base Google batch config. Contains common auth configuration properties and methods.
 */
public abstract class GoogleAuthBaseConfig extends PluginConfig {
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

  public static final String ACCESS_TOKEN = "accessToken";
  public static final String ACCESS_TOKEN_LABEL = "Access token";

  public static final String OAUTH_METHOD = "oauthMethod";
  public static final String ACCOUNT_FILE_PATH = "accountFilePath";
  public static final String DIRECTORY_IDENTIFIER = "directoryIdentifier";
  public static final String FILE_IDENTIFIER = "fileIdentifier";
  public static final String NAME_SERVICE_ACCOUNT_TYPE = "serviceAccountType";
  public static final String NAME_SERVICE_ACCOUNT_JSON = "serviceAccountJSON";
  public static final String SERVICE_ACCOUNT_FILE_PATH = "filePath";
  public static final String SERVICE_ACCOUNT_JSON = "JSON";
  public static final String SCHEMA = "schema";

  private static final String IS_SET_FAILURE_MESSAGE_PATTERN = "'%s' property is empty or macro is not available.";

  @Name(REFERENCE_NAME)
  @Description("Reference Name.")
  private String referenceName;

  // TODO remove these properties after OAuth2 will be provided by cdap
  // start of workaround
  @Name(AUTH_TYPE)
  @Description("Type of authentication used to access Google API. \n" +
    "OAuth2 and Service account types are available. When using service account type make sure that the Google Drive " +
    "Folder is shared to the service account email used with the required permission.")
  private String authType;

  @Name(NAME_SERVICE_ACCOUNT_TYPE)
  @Description("Service account type, file path where the service account is located or the JSON content of the " +
    "service account.")
  @Nullable
  @Macro
  protected String serviceAccountType;

  @Macro
  @Nullable
  @Name(OAUTH_METHOD)
  @Description("The method used to get OAuth access tokens. "
    + "The oauth access token can be directly provided, "
    + "or a client id, client secret, and refresh token can be provided.")
  private String oauthMethod;

  @Nullable
  @Macro
  @Name(CLIENT_ID)
  @Description("OAuth2 client id.")
  private String clientId;

  @Nullable
  @Macro
  @Name(CLIENT_SECRET)
  @Description("OAuth2 client secret.")
  private String clientSecret;

  @Nullable
  @Macro
  @Name(REFRESH_TOKEN)
  @Description("OAuth2 refresh token.")
  private String refreshToken;

  @Nullable
  @Macro
  @Name(ACCESS_TOKEN)
  @Description("Short lived access token for connect.")
  private String accessToken;

  @Nullable
  @Macro
  @Name(ACCOUNT_FILE_PATH)
  @Description("Path on the local file system of the service account key used for authorization. " +
    "Can be set to 'auto-detect' for getting service account from system variable. " +
    "The file/system variable must be present on every node in the cluster. " +
    "Service account json can be generated on Google Cloud " +
    "Service Account page (https://console.cloud.google.com/iam-admin/serviceaccounts).")
  protected String accountFilePath;
  // end of workaround

  @Name(NAME_SERVICE_ACCOUNT_JSON)
  @Description("Content of the service account file.")
  @Nullable
  @Macro
  protected String serviceAccountJson;

  @Nullable
  @Macro
  @Name(DIRECTORY_IDENTIFIER)
  @Description("Identifier of the folder. This comes after “folders/” in the URL. For example, if the URL was " +
    "“https://drive.google.com/drive/folders/1dyUEebJaFnWa3Z4n0BFMVAXQ7mfUH11g?resourcekey=0-XVijrJSp3E3gkdJp20MpCQ”, "
    + "then the Directory Identifier would be “1dyUEebJaFnWa3Z4n0BFMVAXQ7mfUH11g”.")
  private String directoryIdentifier;

  @Nullable
  @Macro
  @Name(FILE_IDENTIFIER)
  @Description("Identifier of the file. This comes after “file/d/ or spreadsheets/d/ or document/d/” in the URL. " +
    "For example, if the URL was “https://drive.google.com/file/d/16npTpL3ozkAzB5kLQ-oQD3IlTZhnnh2w1/view”, "
    + "then the File Identifier would be “16npTpL3ozkAzB5kLQ-oQD3IlTZhnnh2w1”.")
  private String fileIdentifier;

  /**
   * Returns the ValidationResult.
   *
   * @param collector the failure collector is provided
   * @return The ValidationResult
   */
  public ValidationResult validate(FailureCollector collector) {
    IdUtils.validateReferenceName(referenceName, collector);
    checkIfDirectoryOrFileIdentifierExists(collector);
    ValidationResult validationResult = new ValidationResult();
    if (validateAuthType(collector)) {
      AuthType authType = getAuthType();
      boolean propertiesAreValid;
      switch (authType) {
        case OAUTH2:
          propertiesAreValid = validateOAuth2Properties(collector);
          break;
        case SERVICE_ACCOUNT:
          propertiesAreValid = validateServiceAccount(collector);
          break;
        default:
          collector.addFailure(String.format("'%s' is not processed value.", authType.toString()), null)
            .withConfigProperty(AUTH_TYPE);
          return validationResult;
      }
      if (propertiesAreValid) {
        try {
          GoogleDriveClient client = new GoogleDriveClient(this);

          // check directory or file access
          if (isDirectoryOrFileAccessible(collector, client)) {
            validationResult.setDirectoryOrFileAccessible(true);
          }
        } catch (Exception e) {
          collector.addFailure(
            String.format("Exception during authentication/directory properties check: %s.", e.getMessage()),
            "Check message and reconfigure the plugin.")
            .withStacktrace(e.getStackTrace());
        }
      }
    }
    return validationResult;
  }

  private boolean validateAuthType(FailureCollector collector) {
    if (!containsMacro(AUTH_TYPE)) {
      try {
        getAuthType();
        return true;
      } catch (InvalidPropertyTypeException e) {
        collector.addFailure(e.getMessage(), Arrays.stream(AuthType.values()).map(v -> v.getValue())
            .collect(Collectors.joining()))
          .withConfigProperty(AUTH_TYPE);
        return false;
      }
    }
    return false;
  }

  private boolean validateOAuth2Properties(FailureCollector collector) {
    if (OAuthMethod.REFRESH_TOKEN.equals(getOAuthMethod())) {
      return checkPropertyIsSet(collector, clientId, CLIENT_ID, CLIENT_ID_LABEL)
        & checkPropertyIsSet(collector, clientSecret, CLIENT_SECRET, CLIENT_SECRET_LABEL)
        & checkPropertyIsSet(collector, refreshToken, REFRESH_TOKEN, REFRESH_TOKEN_LABEL);
    } else {
      return checkPropertyIsSet(collector, accessToken, ACCESS_TOKEN, ACCESS_TOKEN_LABEL);
    }
  }

  private boolean validateServiceAccount(FailureCollector collector) {
    if (containsMacro(ACCOUNT_FILE_PATH) || containsMacro(NAME_SERVICE_ACCOUNT_JSON)) {
      return false;
    }
    final Boolean serviceAccountFilePath = isServiceAccountFilePath();
    final Boolean serviceAccountJson = isServiceAccountJson();

    if (serviceAccountFilePath != null && serviceAccountFilePath) {
      if (!AUTO_DETECT_VALUE.equals(accountFilePath) && !new File(accountFilePath).exists()) {
        collector.addFailure("Service Account File Path is not available.",
                             "Please provide path to existing Service Account file.")
          .withConfigProperty(ACCOUNT_FILE_PATH);
      }
    }
    if (serviceAccountJson != null && serviceAccountJson) {
      if (!Optional.ofNullable(getServiceAccountJson()).isPresent()) {
        collector.addFailure("Service Account JSON can not be empty.",
                             "Please provide Service Account JSON.")
          .withConfigProperty(NAME_SERVICE_ACCOUNT_JSON);
      }
    }
    return collector.getValidationFailures().size() == 0;
  }

  private boolean isDirectoryOrFileAccessible(FailureCollector collector, GoogleDriveClient driveClient)
    throws IOException {

    if (directoryIdentifier != null && !containsMacro(DIRECTORY_IDENTIFIER)) {
      try {
        driveClient.isFolderAccessible(directoryIdentifier);
        return true;
      } catch (GoogleJsonResponseException e) {
        collector.addFailure(e.getDetails().getMessage(), "Provide an existing folder identifier.")
          .withConfigProperty(DIRECTORY_IDENTIFIER)
          .withStacktrace(e.getStackTrace());
      }
    }

    if (fileIdentifier != null && !containsMacro(FILE_IDENTIFIER)) {
      try {
        driveClient.isFileAccessible(fileIdentifier);
        return true;
      } catch (GoogleJsonResponseException e) {
        collector.addFailure(e.getDetails().getMessage(), "Provide an existing file identifier.")
          .withConfigProperty(FILE_IDENTIFIER)
          .withStacktrace(e.getStackTrace());
      }
    }
    return false;
  }

  protected void checkIfDirectoryOrFileIdentifierExists(FailureCollector collector) {
    if (directoryIdentifier == null && !containsMacro(DIRECTORY_IDENTIFIER) &&
      fileIdentifier == null && !containsMacro(FILE_IDENTIFIER)) {
      collector.addFailure("Both Directory Identifier and File Identifier can not be null.",
                           "Provide either Directory Identifier or File Identifier.")
        .withConfigProperty(DIRECTORY_IDENTIFIER)
        .withConfigProperty(FILE_IDENTIFIER);
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

  public String getFileIdentifier() {
    return fileIdentifier;
  }

  public AuthType getAuthType() {
    return AuthType.fromValue(authType);
  }

  public void setAuthType(String authType) {
    this.authType = authType;
  }

  public void setReferenceName(String referenceName) {
    this.referenceName = referenceName;
  }

  public void setServiceAccountType(String serviceAccountType) {
    this.serviceAccountType = serviceAccountType;
  }

  public void setServiceAccountJson(String serviceAccountJson) {
    this.serviceAccountJson = serviceAccountJson;
  }

  public void setAccountFilePath(String accountFilePath) {
    this.accountFilePath = accountFilePath;
  }

  public void setDirectoryIdentifier(String directoryIdentifier) {
    this.directoryIdentifier = directoryIdentifier;
  }
  public void setFileIdentifier(String fileIdentifier) {
    this.fileIdentifier = fileIdentifier;
  }

  public void setOauthMethod(String oauthMethod) {
    this.oauthMethod = oauthMethod;
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  public void setClientSecret(String clientSecret) {
    this.clientSecret = clientSecret;
  }

  public void setRefreshToken(String refreshToken) {
    this.refreshToken = refreshToken;
  }

  public void setAccessToken(String accessToken) {
    this.accessToken = accessToken;
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

  @Nullable
  public String getAccessToken() {
    return accessToken;
  }

  @Nullable
  public String getServiceAccountFilePath() {
    if (containsMacro(ACCOUNT_FILE_PATH) || Strings.isNullOrEmpty(accountFilePath)
      || AUTO_DETECT_VALUE.equals(accountFilePath)) {
      return null;
    }
    return accountFilePath;
  }

  @Nullable
  public String getServiceAccountJson() {
    if (containsMacro(NAME_SERVICE_ACCOUNT_JSON) || Strings.isNullOrEmpty(serviceAccountJson)) {
      return null;
    }
    return serviceAccountJson;
  }

  /**
   * @return Service Account Type, defaults to filePath.
   */
  @Nullable
  public String getServiceAccountType() {
    if (containsMacro(NAME_SERVICE_ACCOUNT_TYPE)) {
      return null;
    }
    return Strings.isNullOrEmpty(serviceAccountType) ? SERVICE_ACCOUNT_FILE_PATH : serviceAccountType;
  }

  @Nullable
  public Boolean isServiceAccountJson() {
    String serviceAccountType = getServiceAccountType();
    return Strings.isNullOrEmpty(serviceAccountType) ? null : serviceAccountType.equals(SERVICE_ACCOUNT_JSON);
  }

  @Nullable
  public Boolean isServiceAccountFilePath() {
    String serviceAccountType = getServiceAccountType();
    return Strings.isNullOrEmpty(serviceAccountType) ? null : serviceAccountType.equals(SERVICE_ACCOUNT_FILE_PATH);
  }

  public OAuthMethod getOAuthMethod() {
    if (oauthMethod == null) {
      return OAuthMethod.REFRESH_TOKEN;
    }
    try {
      return OAuthMethod.valueOf(oauthMethod.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid oauth method " + oauthMethod);
    }
  }
}
