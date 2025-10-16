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

@GoogleDrive
Feature: Google Drive Batch Source - Runtime Scenarios

  @BATCH-TS-GD-RNTM-01 @BQ_SINK_TEST
  Scenario: Validate successful records transfer from GoogleDrive to BigQuery with file identifier using CSV format
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
    Then Enter input plugin property: "fileIdentifier" with value: "fileIdentifierValue"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Toggle GoogleDrive source property skip header to true
    Then Click on the Get Schema button
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
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the data transferred from GoogleDrive to BigQuery with actual And expected file for: "bqFileIdentifierOutputFile"

  @BATCH-TS-GD-RNTM-02 @BQ_SINK_TEST
  Scenario: Validate successful records transfer from GoogleDrive to BigQuery with Directory identifier and filter operation
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "GoogleDriveSource" from the plugins list as: "Source"
    Then Expand Plugin group in the LHS plugins list: "Sink"
    Then Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "Google Drive Source" and "BigQuery" to establish connection
    And Navigate to the properties page of plugin: "Google Drive Source"
    Then Enter input plugin property: "referenceName" with value: "refName"
    Then Select radio button plugin property: "authType" with value: "serviceAccount"
    Then Enter input plugin property: "directoryIdentifier" with value: "directoryIdentifierValue"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Toggle GoogleDrive source property skip header to true
    Then Enter input plugin property: "filter" with value: "FilterValue2"
    Then Click on the Get Schema button
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
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the data transferred from GoogleDrive to BigQuery with actual And expected file for: "bqFileIdentifierOutputFile"

  @BATCH-TS-GD-RNTM-03 @BQ_SINK_TEST
  Scenario: Validate successful records transfer from GoogleDrive to BigQuery with file identifier using TSV format
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
    Then Enter input plugin property: "fileIdentifier" with value: "tsvFileIdentifier"
    Then Select dropdown plugin property: "format" with option value: "tsv"
    Then Toggle GoogleDrive source property skip header to true
    Then Click on the Get Schema button
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
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the data transferred from GoogleDrive to BigQuery with actual And expected file for: "bqTsvFileIdentifierOutputFile"

  @BATCH-TS-GD-RNTM-04 @BQ_SINK_TEST
  Scenario: Validate successful records transfer from GoogleDrive to BigQuery with file identifier using Avro format
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
    Then Enter input plugin property: "fileIdentifier" with value: "avroFileIdentifier"
    Then Select dropdown plugin property: "format" with option value: "avro"
    Then Click on the Get Schema button
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
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the data transferred from GoogleDrive to BigQuery with actual And expected file for: "bqFileIdentifierOutputFile"

  @BATCH-TS-GD-RNTM-05 @BQ_SINK_TEST
  Scenario: Validate successful records transfer from GoogleDrive to BigQuery with file identifier using Text format
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
    Then Enter input plugin property: "fileIdentifier" with value: "textFileIdentifier"
    Then Select dropdown plugin property: "format" with option value: "text"
    Then Click on the Get Schema button
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
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the data transferred from GoogleDrive to BigQuery with actual And expected file for: "bqTextFileIdentifierOutputFile"

  @BATCH-TS-GD-RNTM-06 @BQ_SINK_TEST
  Scenario: Validate successful records transfer from GoogleDrive to BigQuery with file identifier using xls format
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
    Then Enter input plugin property: "fileIdentifier" with value: "xlsFileIdentifier"
    Then Select dropdown plugin property: "format" with option value: "xls"
    Then Click on the Get Schema button
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
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the data transferred from GoogleDrive to BigQuery with actual And expected file for: "bqXlsFileIdentifierOutputFile"

  @BATCH-TS-GD-RNTM-07 @BQ_SINK_TEST
  Scenario: Validate successful records transfer from GoogleDrive to BigQuery with Directory identifier and file types to pull as spreadsheets
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "GoogleDriveSource" from the plugins list as: "Source"
    Then Expand Plugin group in the LHS plugins list: "Sink"
    Then Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "Google Drive Source" and "BigQuery" to establish connection
    And Navigate to the properties page of plugin: "Google Drive Source"
    Then Enter input plugin property: "referenceName" with value: "refName"
    Then Select radio button plugin property: "authType" with value: "serviceAccount"
    Then Enter input plugin property: "directoryIdentifier" with value: "directoryIdentifierValue"
    Then Toggle GoogleDrive source property schema required to false
    Then Select dropdown plugin property: "fileTypesToPull" with option value: "spreadsheets"
    Then Press Escape Key
    Then Select dropdown plugin property: "fileTypesToPull" with option value: "binary"
    Then Press Escape Key
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
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the data transferred from GoogleDrive to BigQuery with actual And expected file for: "bqSpreadsheetsFileIdentifierOutputFile"

  @BATCH-TS-GD-RNTM-08 @BQ_SINK_TEST
  Scenario: Verify user should be able to preview the pipeline
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
    Then Enter input plugin property: "fileIdentifier" with value: "fileIdentifierValue"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Toggle GoogleDrive source property skip header to true
    Then Click on the Get Schema button
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
    Then Preview and run the pipeline
    And Wait till pipeline preview is in running state with a timeout of 500 seconds
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
