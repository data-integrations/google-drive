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

package io.cdap.plugin.google.drive.etl;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.SpreadsheetProperties;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.google.common.GenerateCredentials;
import io.cdap.plugin.google.sheets.source.GoogleSheetsSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ETLTestGoogleSheets extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", true);
  protected static Sheets service;
  protected static Drive drive;
  protected static String directoryIdentifier;
  protected static final String APPLICATION_NAME = "Google Sheets Test";
  protected static final Schema INPUT_SCHEMA = Schema.recordOf(
    "input-record",
    Schema.Field.of("body", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("offset", Schema.of(Schema.Type.LONG)));

  protected static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  protected static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();


  @BeforeClass
  public static void setupClient() throws Exception {

    //Generate credentials
    GenerateCredentials credentials = new GenerateCredentials();
    GoogleCredential googleCredential = credentials.getServiceAccountCredentials();

    final NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

    //Create necessary services
    service = new Sheets.Builder(httpTransport, JSON_FACTORY, googleCredential)
      .setApplicationName(APPLICATION_NAME)
      .build();

    drive = new Drive.Builder(httpTransport, JSON_FACTORY, googleCredential)
      .setApplicationName(APPLICATION_NAME)
      .build();

    //Create the directory
    File directoryMetadata = new File();
    directoryMetadata.setName("TestDirectory");
    directoryMetadata.setMimeType("application/vnd.google-apps.folder");
    File directory = drive.files().create(directoryMetadata)
      .setFields("id")
      .execute();

    directoryIdentifier = directory.getId();

    //Creating the spreadsheet
    Spreadsheet spreadsheet = new Spreadsheet()
      .setProperties(new SpreadsheetProperties()
                       .setTitle("TestSpreadSheet"));

    spreadsheet = service.spreadsheets().create(spreadsheet)
      .setFields("spreadsheetId")
      .execute();

    String spreadsheetId = spreadsheet.getSpreadsheetId();
    String range = "Sheet1!A:A";

    //Adding the rows
    List<Object> row = new ArrayList<>();
    row.add("TestRow");

    List<List<Object>> values = new ArrayList<>();
    values.add(row);

    ValueRange valueRange = new ValueRange();
    valueRange.setMajorDimension("ROWS");
    valueRange.setValues(values);

    //Appending the values
    service.spreadsheets().values().append(spreadsheetId, range, valueRange).setValueInputOption("RAW").execute();

    //Sending spreadsheet to created directory
    drive.files().update(spreadsheet.getSpreadsheetId(), null)
      .setAddParents(directoryIdentifier)
      .execute();

    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the artifact as its parent.
    // this will make our plugins available.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("google-sheets-plugins", "1.0.0"),
                      parentArtifact, GoogleSheetsSource.class);
  }

  @AfterClass
  public static void cleanUp() {
    try {
      drive.files().delete(directoryIdentifier).execute();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Test
  public void testGoogleSheetsWithMacros() throws Exception {

    ETLStage source = new ETLStage("GoogleSheetsETLTest", new ETLPlugin(GoogleSheetsSource.NAME,
                                                                        BatchSource.PLUGIN_TYPE,
                                                                        getSourceMinimalDefaultConfigs(),
                                                                        null));
    String outputDatasetName = "output-google_sheets_test";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ImmutableMap<String, String> runtimeProperties =
      ImmutableMap.of("serviceAccountType", "filePath",
                      "serviceAccountFilePathWithMacro", "auto-detect");

    ApplicationManager appManager = deployETL(etlConfig, "GoogleSheetsWithMacro");
    runETLOnce(appManager, runtimeProperties);

    DataSetManager<Table> dataset = getDataset(outputDatasetName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(dataset);

    Assert.assertEquals("Expected records", 999, outputRecords.size());
  }

  /**
   * Run the SmartWorkflow in the given ETL application for once and wait for the workflow's COMPLETED status
   * with 5 minutes timeout.
   *
   * @param appManager the ETL application to run
   * @param arguments  the arguments to be passed when running SmartWorkflow
   */
  protected WorkflowManager runETLOnce(ApplicationManager appManager, Map<String, String> arguments)
    throws TimeoutException, InterruptedException, ExecutionException {
    final WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    int numRuns = workflowManager.getHistory().size();
    workflowManager.start(arguments);
    Tasks.waitFor(numRuns + 1, () -> workflowManager.getHistory().size(), 20, TimeUnit.SECONDS);
    workflowManager.waitForStopped(5, TimeUnit.MINUTES);
    return workflowManager;
  }

  protected ApplicationManager deployETL(ETLBatchConfig etlConfig, String appName) throws Exception {
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    return deployApplication(appId, appRequest);
  }

  public Map<String, String> getSourceMinimalDefaultConfigs() {
    Map<String, String> sourceProps = new HashMap<>();
    sourceProps.put("referenceName", "google_sheets_with_macro");
    sourceProps.put("sheetsToPull", "all");
    sourceProps.put("bodyFormat", "string");
    sourceProps.put("formatting", "valuesOnly");
    sourceProps.put("metadataFieldName", "metadata");
    sourceProps.put("extractMetadata", "false");
    sourceProps.put("skipEmptyData", "false");
    sourceProps.put("maxRetryJitterWait", "100");
    sourceProps.put("schemaBodyFieldName", "bytes");
    sourceProps.put("schemaNameFieldName", "field");
    sourceProps.put("schemaMimeFieldName", "string");
    sourceProps.put("maxPartitionSize", "0");
    sourceProps.put("readBufferSize", "100");
    sourceProps.put("maxRetryWait", "200");
    sourceProps.put("maxRetryCount", "8");
    sourceProps.put("modificationDateRange", "today");
    sourceProps.put("directoryIdentifier", directoryIdentifier);
    sourceProps.put("authType", "serviceAccount");
    sourceProps.put("serviceAccountType", "${serviceAccountType}");
    sourceProps.put("accountFilePath", "${serviceAccountFilePathWithMacro}");
    sourceProps.put("schema", INPUT_SCHEMA.toString());
    sourceProps.put("columnNamesSelection", "firstRowAsColumns");
    sourceProps.put("firstHeaderRow", "1");
    sourceProps.put("lastDataColumn", "26");
    sourceProps.put("lastDataRow", "1000");
    sourceProps.put("addNameFields", "false");
    return sourceProps;
  }
}
