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

package io.cdap.plugin.google.common;

import com.google.api.services.drive.model.File;

/**
 * Representation for file can be written to Google Drive system..
 */
public class FileFromFolder {
  private final byte[] content;
  private final long offset;
  private final File file;

  public FileFromFolder(byte[] content, long offset, File file) {
    this.content = content;
    this.offset = offset;
    this.file = file;
  }

  public FileFromFolder(byte[] content, File file) {
    this.content = content;
    this.file = file;
    this.offset = 0L;
  }

  public byte[] getContent() {
    return content;
  }

  public long getOffset() {
    return offset;
  }

  public File getFile() {
    return file;
  }
}
