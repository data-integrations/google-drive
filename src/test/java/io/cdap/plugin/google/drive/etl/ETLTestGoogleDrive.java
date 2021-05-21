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
import com.google.api.client.http.FileContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.common.Bytes;
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
import io.cdap.plugin.google.drive.source.GoogleDriveSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ETLTestGoogleDrive extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", true);
  private static final String GCP_SERVICE_ACCOUNT_PATH = "google.application.credentials.path";
  private static final String GCP_SERVICE_ACCOUNT_BASE64_ENCODED = "google.application.credentials.base64.encoded";
  protected static Drive service;
  protected static String directoryIdentifier;
  protected static final String APPLICATION_NAME = "Google Drive Test";
  protected static final Schema INPUT_SCHEMA = Schema.recordOf(
    "input-record",
    Schema.Field.of("body", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("offset", Schema.of(Schema.Type.LONG)));

  protected static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  protected static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  @BeforeClass
  public static void setupClient() throws Exception {

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

    NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

    GoogleCredential googleCredential = null;
    if (serviceAccountCredentials != null) {
      try (InputStream inputStream = new ByteArrayInputStream(
        serviceAccountCredentials.getBytes(StandardCharsets.UTF_8))) {
        googleCredential = GoogleCredential.fromStream(inputStream).createScoped(
          Collections.singletonList(DriveScopes.DRIVE));
      }
    }

    service = new Drive.Builder(httpTransport, JSON_FACTORY, googleCredential)
      .setApplicationName(APPLICATION_NAME)
      .build();

    //Create the directory
    File directoryMetadata = new File();
    directoryMetadata.setName("TestDirectory");
    directoryMetadata.setMimeType("application/vnd.google-apps.folder");
    File directory = service.files().create(directoryMetadata)
      .setFields("id")
      .execute();

    directoryIdentifier = directory.getId();

    // Populate directory with a file
    File fileMetadata = new File();
    fileMetadata.setName("csvexample.csv");
    fileMetadata.setParents(Collections.singletonList(directoryIdentifier));
    java.io.File filePath = new java.io.File("src/test/resources/csvexample.csv");
    FileContent mediaContent = new FileContent("text/csv", filePath);
    service.files().create(fileMetadata, mediaContent)
      .setFields("id, parents")
      .execute();

    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the artifact as its parent.
    // this will make our plugins available.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("google-drive-plugins", "1.0.0"),
                      parentArtifact, GoogleDriveSource.class);
  }

  @AfterClass
  public static void cleanUp() {
    try {
      service.files().delete(directoryIdentifier).execute();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Test
  public void testGoogleDriveWithMacros() throws Exception {

    ETLStage source = new ETLStage("GoogleDriveETLTest", new ETLPlugin(GoogleDriveSource.NAME,
                                                                       BatchSource.PLUGIN_TYPE,
                                                                       getSourceMinimalDefaultConfigs(),
                                                                       null));
    String outputDatasetName = "output-google_drive_test";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ImmutableMap<String, String> runtimeProperties =
      ImmutableMap.of(
        "serviceAccountType", "filePath",
        "serviceAccountFilePathWithMacro", "auto-detect");

    ApplicationManager appManager = deployETL(etlConfig, "GoogleDriveWithMacro");
    runETLOnce(appManager, runtimeProperties);

    DataSetManager<Table> dataset = getDataset(outputDatasetName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(dataset);

    Assert.assertEquals("Expected records", 1, outputRecords.size());
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
    sourceProps.put("referenceName", "google_drive_with_macro");
    sourceProps.put("fileTypesToPull", "documents,binary,spreadsheets,drawings,presentations,appsScripts");
    sourceProps.put("bodyFormat", "string");
    sourceProps.put("sheetsExportingFormat", "text/plain");
    sourceProps.put("docsExportingFormat", "text/csv");
    sourceProps.put("drawingsExportingFormat", "image/svg+xml");
    sourceProps.put("presentationsExportingFormat", "text/plain");
    sourceProps.put("maxRetryJitterWait", "100");
    sourceProps.put("schemaBodyFieldName", "bytes");
    sourceProps.put("schemaNameFieldName", "field");
    sourceProps.put("schemaMimeFieldName", "string");
    sourceProps.put("maxPartitionSize", "0");
    sourceProps.put("maxRetryWait", "200");
    sourceProps.put("maxRetryCount", "8");
    sourceProps.put("modificationDateRange", "today");
    sourceProps.put("directoryIdentifier", directoryIdentifier);
    sourceProps.put("authType", "serviceAccount");
    sourceProps.put("serviceAccountType", "${serviceAccountType}");
    sourceProps.put("accountFilePath", "${serviceAccountFilePathWithMacro}");
    sourceProps.put("schema", INPUT_SCHEMA.toString());
    return sourceProps;
  }
}
