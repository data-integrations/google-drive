/*
 * Copyright © 2021 Cask Data, Inc.
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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.drive.DriveScopes;
import com.google.common.base.Preconditions;
import io.cdap.cdap.api.common.Bytes;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Collections;

public class GenerateCredentials {

  protected static final String GCP_SERVICE_ACCOUNT_PATH = "google.application.credentials.path";
  protected static final String GCP_SERVICE_ACCOUNT_BASE64_ENCODED = "google.application.credentials.base64.encoded";

  public GoogleCredential getServiceAccountCredentials() throws IOException {
    // base64-encode the credentials, to avoid a commandline-parsing error, since the credentials have dashes in them
    String property = System.getProperty(GCP_SERVICE_ACCOUNT_BASE64_ENCODED);
    String serviceAccountCredentials;
    if (property != null) {
      serviceAccountCredentials = Bytes.toString(Base64.getDecoder().decode(property));
    } else {
      property = Preconditions.checkNotNull(System.getProperty(GCP_SERVICE_ACCOUNT_PATH),
                                            "The credentials file provided is null. " +
                                              "Please make sure the path is correct and the file exists.");

      serviceAccountCredentials = new String(Files.readAllBytes(Paths.get(property)), StandardCharsets.UTF_8);
    }

    GoogleCredential googleCredential = null;
    if (serviceAccountCredentials != null) {
      try (InputStream inputStream = new ByteArrayInputStream(
        serviceAccountCredentials.getBytes(StandardCharsets.UTF_8))) {
        googleCredential = GoogleCredential.fromStream(inputStream).createScoped(
          Collections.singletonList(DriveScopes.DRIVE));
      }
    }
    return googleCredential;
  }
}
