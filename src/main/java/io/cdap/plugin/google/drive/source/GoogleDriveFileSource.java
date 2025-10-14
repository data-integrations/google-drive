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

package io.cdap.plugin.google.drive.source;

import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.format.plugin.AbstractFileSource;
import java.util.HashMap;
import java.util.Map;

/**
 * This class gives ability to read files from Google Drive with a structured schema.
 */
public class GoogleDriveFileSource extends AbstractFileSource<GoogleDriveSourceConfig> {
  public static final String NAME = "GoogleDrive";
  private final GoogleDriveSourceConfig config;

  public GoogleDriveFileSource(GoogleDriveSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSourceContext context) {
    FailureCollector collector = context == null ? null : context.getFailureCollector();
    return new HashMap<>(config.getFileSystemProperties(collector));
  }

  @Override
  protected boolean shouldGetSchema() {
    return !config.containsMacro(GoogleDriveSourceConfig.NAME_FORMAT)
        && !config.containsMacro(GoogleDriveSourceConfig.NAME_DELIMITER)
        && !config.containsMacro(GoogleDriveSourceConfig.NAME_FILE_SYSTEM_PROPERTIES)
        && !config.containsMacro(GoogleDriveSourceConfig.NAME_FILE_ENCODING);
  }
}
