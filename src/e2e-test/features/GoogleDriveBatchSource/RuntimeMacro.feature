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

Feature: Google Drive Batch Source - Runtime Scenarios

  @GoogleDrive @BATCH-TS-GD-MACRO-01 @BQ_SINK_TEST
  Scenario: Validate successful records transfer from GoogleDrive to BigQuery with macro arguments
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "GoogleDriveSource" from the plugins list as: "Source"
    Then Expand Plugin group in the LHS plugins list: "Sink"
    Then Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "Google Drive Source" and "BigQuery" to establish connection
    And Navigate to the properties page of plugin: "Google Drive Source"
    Then Enter input plugin property: "referenceName" with value: "refName"
    Then Select radio button plugin property: "authType" with value: "serviceAccount"
    Then Select radio button plugin property: "identifierType" with value: "FILE_IDENTIFIER"
    Then Click on the Macro button of Property: "fileIdentifier" and set the value to: "fileIdentifier"
    Then Click on the Macro button of Property: "format" and set the value to: "format"
    Then Click on the Macro button of Property: "skipHeader" and set the value to: "googleDriveSkipHeader"
    Then Select dropdown plugin property from output schema and set the value: "clear"
    Then Validate "GoogleDrive" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "projectId"
    And Replace input plugin property: "dataset" with value: "dataset"
    Then Override Service account details if set in environment variables
    And Enter input plugin property: "table" with value: "bqTargetTable"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Click on the Runtime Arguments Dropdown button
    Then Enter runtime argument value "fileIdentifierValue" for key "fileIdentifier"
    Then Enter runtime argument value "format" for key "format"
    Then Enter runtime argument value "googleDriveSkipHeaderTrue" for key "googleDriveSkipHeader"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the data transferred from GoogleDrive to BigQuery with actual And expected file for: "bqFileIdentifierOutputFile"