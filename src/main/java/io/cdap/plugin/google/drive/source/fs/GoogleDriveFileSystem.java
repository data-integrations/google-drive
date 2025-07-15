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

package io.cdap.plugin.google.drive.source.fs;

import com.google.api.services.drive.Drive;
import com.google.common.base.Strings;
import io.cdap.plugin.google.common.GoogleDriveClient;
import io.cdap.plugin.google.drive.source.GoogleDriveInputFormatProvider;
import io.cdap.plugin.google.drive.source.GoogleDriveSourceConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

/**
 * A custom Hadoop FileSystem implementation for Google Drive.
 * This class provides methods to interact with Google Drive files and directories.
 */
public class GoogleDriveFileSystem extends FileSystem {
  private URI uri;
  private Path workingDir;
  private Drive driveService;
  private String filter;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    setConf(conf);
    this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());
    this.workingDir = new Path("/");

    GoogleDriveSourceConfig googleDriveSourceConfig = GoogleDriveInputFormatProvider.extractPropertiesFromConfig(conf);
    GoogleDriveClient<GoogleDriveSourceConfig> client = new GoogleDriveClient<>(googleDriveSourceConfig);

    // Initialize Google Drive service (using OAuth2 or service account)
    this.driveService = client.getDriveClient();

    // Initialize filter to be passed down to listStatus
    if (!Strings.isNullOrEmpty(googleDriveSourceConfig.getFilter())) {
      this.filter = googleDriveSourceConfig.getFilter();
    }
  }
  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return new GoogleDriveInputStream(new GoogleDriveInputStreamWrapper(driveService, f));
  }

  @Override
  public FSDataOutputStream create(Path f,
                                   FsPermission permission,
                                   boolean overwrite,
                                   int bufferSize,
                                   short replication,
                                   long blockSize,
                                   Progressable progress) throws IOException {
    throw new UnsupportedOperationException(
      "GDrive does not support: FSDataOutputStream create(Path, FsPermission, boolean, int, " +
        "short, long, Progressable)");
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    throw new UnsupportedOperationException(
      "GDrive does not support: FSDataOutputStream append(Path, int, Progressable)");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw  new UnsupportedOperationException("GDrive does not support: boolean rename(Path, Path)");
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    throw  new UnsupportedOperationException("GDrive does not support: boolean delete(Path, boolean)");
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    if (isPathDirectory(f)) {
      return GoogleDriveUtils.listStatus(driveService, f, filter);
    }
    return new FileStatus[]{GoogleDriveUtils.listObjectStatus(driveService, f, false, GoogleDriveUtils.getFileId(f))};
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    throw new UnsupportedOperationException("GDrive does not support: void setWorkingDirectory(Path)");
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new UnsupportedOperationException(
      "GDrive does not support: boolean mkdirs(Path, FsPermission)");
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    if (isPathDirectory(f)) {
      return GoogleDriveUtils.listObjectStatus(driveService, f, true, f.getName());
    }
    return GoogleDriveUtils.listObjectStatus(driveService, f, false, GoogleDriveUtils.getFileId(f));
  }

  private boolean isPathDirectory(Path path) {
    return path != null && path.toString().startsWith(String.format("%s://%s%s/",
        GoogleDriveSourceConfig.GOOGLE_DRIVE_SCHEMA,
        GoogleDriveSourceConfig.GOOGLE_DRIVE_AUTHORITY,
        GoogleDriveSourceConfig.GOOGLE_DRIVE_FOLDER_PATH_PREFIX));
  }

}
