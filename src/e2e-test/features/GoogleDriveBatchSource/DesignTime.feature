# Copyright © 2025 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

@GoogleDrive_Source
Feature: Google Drive Batch Source - Design time Scenarios

  @BATCH-TS-GD-DSGN-01
  Scenario: Verify required fields missing validation for 'Reference Name' property
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "GoogleDriveSource" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Google Drive Source"
    Then Click on the Validate button
    Then Verify mandatory property error for below listed properties:
      | referenceName |

  @BATCH-TS-GD-DSGN-02
  Scenario: Verify validation message when user leaves Directory Identifer Property blank
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "GoogleDriveSource" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Google Drive Source"
    Then Enter input plugin property: "referenceName" with value: "refName"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "directoryIdentifier" is displaying an in-line error message: "blank.property.directoryidentifier"

  @BATCH-TS-GD-DSGN-03
  Scenario: Verify validation message when user provides invalid directory name
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "GoogleDriveSource" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Google Drive Source"
    Then Enter input plugin property: "referenceName" with value: "refName"
    Then Select radio button plugin property: "authType" with value: "serviceAccount"
    Then Enter input plugin property: "directoryIdentifier" with value: "invalidName"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "directoryIdentifier" is displaying an in-line error message: "invalid.property.directoryidentifier"

  @BATCH-TS-GD-DSGN-04
  Scenario: Verify validation message when user provides invalid file identifer name
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "GoogleDriveSource" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Google Drive Source"
    Then Enter input plugin property: "referenceName" with value: "refName"
    Then Select radio button plugin property: "authType" with value: "serviceAccount"
    Then Select radio button plugin property: "identifierType" with value: "FILE_IDENTIFIER"
    Then Enter input plugin property: "fileIdentifier" with value: "invalidName"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "fileIdentifier" is displaying an in-line error message: "invalid.property.fileidentifier"

  @BATCH-TS-GD-DSGN-05
  Scenario: Verify that user is able to get output schema for a valid filter
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "GoogleDriveSource" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Google Drive Source"
    Then Enter input plugin property: "referenceName" with value: "refName"
    Then Select radio button plugin property: "authType" with value: "serviceAccount"
    Then Enter input plugin property: "directoryIdentifier" with value: "directoryIdentifierValue"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Toggle GoogleDrive source property skip header to true
    Then Enter input plugin property: "filter" with value: "FilterValue"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "sample.schema"

  @BATCH-TS-GD-DSGN-06
  Scenario: Verify that user is able to get output schema for a valid file identifier
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "GoogleDriveSource" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Google Drive Source"
    Then Enter input plugin property: "referenceName" with value: "refName"
    Then Select radio button plugin property: "authType" with value: "serviceAccount"
    Then Select radio button plugin property: "identifierType" with value: "FILE_IDENTIFIER"
    Then Enter input plugin property: "fileIdentifier" with value: "fileIdentifierValue"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Toggle GoogleDrive source property skip header to true
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "Test.schema"


