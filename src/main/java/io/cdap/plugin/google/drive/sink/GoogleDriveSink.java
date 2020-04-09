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

package io.cdap.plugin.google.drive.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.google.drive.common.FileFromFolder;

import java.util.stream.Collectors;

/**
 * Batch sink to writing multiple files to Google Drive directory.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(GoogleDriveSink.NAME)
@Description("Sink plugin to save files from the pipeline to Google Drive directory.")
public class GoogleDriveSink extends BatchSink<StructuredRecord, Void, FileFromFolder> {
  public static final String NAME = "GoogleDrive";

  private final GoogleDriveSinkConfig config;
  private StructuredRecordToFileFromFolderTransformer transformer;

  public GoogleDriveSink(GoogleDriveSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();

    config.validate(failureCollector, inputSchema);
  }

  @Override
  public void prepareRun(BatchSinkContext batchSinkContext) {
    Schema inputSchema = batchSinkContext.getInputSchema();

    FailureCollector failureCollector = batchSinkContext.getFailureCollector();
    config.validate(failureCollector, inputSchema);
    failureCollector.getOrThrowException();

    batchSinkContext.addOutput(Output.of(config.getReferenceName(), new GoogleDriveOutputFormatProvider(config)));

    LineageRecorder lineageRecorder = new LineageRecorder(batchSinkContext, config.getReferenceName());
    lineageRecorder.createExternalDataset(inputSchema);
    if (inputSchema.getFields() != null && !inputSchema.getFields().isEmpty()) {
      String operationDescription = "Wrote to Google Drive directory";
      lineageRecorder.recordWrite("Write", operationDescription,
                                  inputSchema.getFields().stream()
                                    .map(Schema.Field::getName)
                                    .collect(Collectors.toList()));
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    transformer = new StructuredRecordToFileFromFolderTransformer(config.getSchemaBodyFieldName(),
                                                                  config.getSchemaNameFieldName(),
                                                                  config.getSchemaMimeFieldName());
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Void, FileFromFolder>> emitter) {
    FileFromFolder fileFromFolder = transformer.transform(input);
    emitter.emit(new KeyValue<>(null, fileFromFolder));
  }
}
