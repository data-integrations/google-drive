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

package io.cdap.plugin.google.drive.source;

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.google.drive.common.FileFromFolder;
import org.apache.hadoop.io.NullWritable;

import java.util.stream.Collectors;

/**
 * Batch source to read multiple files from Google Drive directory.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(GoogleDriveSource.NAME)
@Description("Reads fileset from specified Google Drive directory.")
public class GoogleDriveSource extends BatchSource<NullWritable, FileFromFolder, StructuredRecord> {
  public static final String NAME = "GoogleDrive";

  private final GoogleDriveSourceConfig config;

  public GoogleDriveSource(GoogleDriveSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();

    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    FailureCollector failureCollector = context.getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();

    LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
    lineageRecorder.createExternalDataset(config.getSchema());
    lineageRecorder.recordRead("Read", "Reading Google Drive files",
                               Preconditions.checkNotNull(config.getSchema().getFields()).stream()
                                 .map(Schema.Field::getName)
                                 .collect(Collectors.toList()));

    context.setInput(Input.of(config.getReferenceName(), new GoogleDriveInputFormatProvider(config)));
  }

  @Override
  public void transform(KeyValue<NullWritable, FileFromFolder> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(FilesFromFolderTransformer.transform(input.getValue(), config.getSchema()));
  }
}
