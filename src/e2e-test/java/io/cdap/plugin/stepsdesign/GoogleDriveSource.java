/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.plugin.stepsdesign;

import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfPluginPropertiesActions;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.actions.GoogleDriveActions;
import io.cdap.plugin.locators.GoogleDriveLocators;
import io.cucumber.java.en.Then;
import org.junit.Assert;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Google Drive Source Plugin related step design.
 */
public class GoogleDriveSource  {

  @Then("Toggle GoogleDrive source property skip header to true")
  public void toggleGoogleDriveSourcePropertySkipHeaderToTrue() {
    CdfGcsActions.skipHeader();
  }

  @Then("Validate the data transferred from BigQuery to BigQuery with actual And expected file for: {string}")
  public void validateTheDataFromBQToBQWithActualAndExpectedFileFor(String expectedFile) throws IOException,
    InterruptedException, URISyntaxException {
    boolean recordsMatched = BQValidationExistingTables.validateActualDataToExpectedData(
      PluginPropertyUtils.pluginProp("bqTargetTable"),
      PluginPropertyUtils.pluginProp(expectedFile));
    Assert.assertTrue("Value of records in actual and expected file is equal", recordsMatched);
  }

  @Then("Toggle GoogleDrive source property schema required to false")
  public void toggleGoogleDriveSourcePropertySchemaRequiredToFalse() {
   GoogleDriveActions.structuredSchemaRequired();

  }
}
