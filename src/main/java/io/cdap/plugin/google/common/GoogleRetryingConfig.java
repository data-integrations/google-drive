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

package io.cdap.plugin.google.common;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;

/**
 * Base Google retrying batch config.
 * Contains common configuration properties and methods for API requests retrying functionality.
 */
public class GoogleRetryingConfig extends GoogleAuthBaseConfig {
  public static final String MAX_RETRY_COUNT = "maxRetryCount";
  public static final String MAX_RETRY_WAIT = "maxRetryWait";
  public static final String MAX_RETRY_JITTER_WAIT = "maxRetryJitterWait";

  @Name(MAX_RETRY_COUNT)
  @Description("Maximum number of retry attempts.")
  @Macro
  protected Integer maxRetryCount;

  @Name(MAX_RETRY_WAIT)
  @Description("Maximum wait time for attempt in seconds. Initial wait time is one second and it grows exponentially.")
  @Macro
  protected Integer maxRetryWait;

  @Name(MAX_RETRY_JITTER_WAIT)
  @Description("Maximum additional wait time is milliseconds.")
  @Macro
  protected Integer maxRetryJitterWait;

  public ValidationResult validate(FailureCollector collector) {
    return super.validate(collector);
  }

  public Integer getMaxRetryCount() {
    return maxRetryCount;
  }

  public Integer getMaxRetryWait() {
    return maxRetryWait;
  }

  public Integer getMaxRetryJitterWait() {
    return maxRetryJitterWait;
  }
}
