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

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Map;

/**
 * InputFormatProvider used by cdap to provide configurations to mapreduce job.
 */
public class GoogleDriveInputFormatProvider implements InputFormatProvider {
  public static final String PROPERTY_CONFIG_JSON = "cdap.google.config";
  public static final Gson GSON = new GsonBuilder().create();

  private final Map<String, String> conf;

  /**
   * Constructor for GoogleDriveOutputFormatProvider object.
   * @param config the GoogleDriveSourceConfig is provided
   */
  public GoogleDriveInputFormatProvider(GoogleDriveSourceConfig config) {
    this.conf = new ImmutableMap.Builder<String, String>()
      .put(PROPERTY_CONFIG_JSON, GSON.toJson(config.getProperties()))
      .build();
  }

  public static GoogleDriveSourceConfig extractPropertiesFromConfig(Configuration config) throws IOException {
    String configJson = config.get(PROPERTY_CONFIG_JSON);
    JsonObject properties = GSON.fromJson(configJson, JsonObject.class)
      .getAsJsonObject(GoogleDriveSourceConfig.CONFIGURATION_PARSE_PROPERTY_NAME);
    return GoogleDriveSourceConfig.of(properties);
  }

  @Override
  public String getInputFormatClassName() {
    return GoogleDriveInputFormat.class.getName();
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return conf;
  }
}
