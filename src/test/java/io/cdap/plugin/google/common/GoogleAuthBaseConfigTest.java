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

import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.validation.DefaultFailureCollector;
import io.cdap.plugin.google.drive.source.GoogleDriveSourceConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class GoogleAuthBaseConfigTest {

  @Test
  public void testNullServiceAccountType() {
    GoogleDriveSourceConfig config = new GoogleDriveSourceConfig(null);
    config.serviceAccountType = null;
    Assert.assertEquals(config.getServiceAccountType(), GoogleAuthBaseConfig.SERVICE_ACCOUNT_FILE_PATH);
  }

  @Test
  public void testIsServiceAccountFilePath() {
    GoogleDriveSourceConfig config = new GoogleDriveSourceConfig(null);
    config.accountFilePath = "test-file-path";
    Assert.assertEquals(config.isServiceAccountFilePath(), true);
  }

  @Test
  public void testFilePathServiceAccountWithoutAccountType() {
    GoogleDriveSourceConfig config = new GoogleDriveSourceConfig(null);
    config.accountFilePath = "test-file-path";
    Assert.assertEquals(config.getServiceAccountType(), GoogleAuthBaseConfig.SERVICE_ACCOUNT_FILE_PATH);
  }

  @Test
  public void testValidationErrorFilePath() {
    GoogleDriveSourceConfig config = new GoogleDriveSourceConfig(null);
    config.setReferenceName("validationErrorFilePath");
    config.setServiceAccountJson("filePath");
    config.setAccountFilePath("test-file-path");
    config.setAuthType("serviceAccount");
    config.setModificationDateRange("today");
    config.getBodyFormat("string");
    config.setStartDate("today");
    FailureCollector collector = new DefaultFailureCollector("stageConfig", Collections.EMPTY_MAP);
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Service Account File Path is not available.",
                        collector.getValidationFailures().get(0).getMessage());
    Assert.assertEquals("accountFilePath",
                        collector.getValidationFailures().get(0).getCauses().get(0).getAttribute("stageConfig"));
  }

  @Test
  public void testValidationErrorJSON() {
    GoogleDriveSourceConfig config = new GoogleDriveSourceConfig(null);
    config.setReferenceName("validationErrorJSON");
    config.setServiceAccountType("JSON");
    config.setServiceAccountJson(null);
    config.setAuthType("serviceAccount");
    config.setModificationDateRange("today");
    config.getBodyFormat("string");
    config.setStartDate("today");
    FailureCollector collector = new DefaultFailureCollector("stageConfig", Collections.EMPTY_MAP);
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Service Account JSON can not be empty.",
                        collector.getValidationFailures().get(0).getMessage());
    Assert.assertEquals("serviceAccountJSON",
                        collector.getValidationFailures().get(0).getCauses().get(0).getAttribute("stageConfig"));
  }
}
