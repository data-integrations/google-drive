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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.IOException;
import java.io.InputStream;

/**
 * A wrapper for InputStream that implements Seekable and PositionedReadable interfaces.
 * This is used to read files from Google Drive in a way that is compatible with Hadoop's file system APIs.
 */
public class GoogleDriveInputStreamWrapper extends InputStream implements Seekable, PositionedReadable {
  private final Drive driveService;
  private InputStream googleDriveStream;
  private long currentPos = 0;

  private final Path filePath;
  public GoogleDriveInputStreamWrapper(Drive driveService, Path filePath) throws IOException {
    this.filePath = filePath;
    this.driveService = driveService;
    this.googleDriveStream = getDriveInputStream(filePath, 0);
  }

  private InputStream getDriveInputStream(Path filePath, long pos) throws IOException {
    String fileId = GoogleDriveUtils.getFileId(filePath);
    if (fileId == null) {
      throw new IOException("File not found in Google Drive: " + filePath);
    }
    Drive.Files.Get get = driveService.files().get(fileId);
    get.getMediaHttpDownloader().setDirectDownloadEnabled(true);
    get.getRequestHeaders().setRange(String.format("bytes=%d-", pos));
    return get.executeMediaAsInputStream();
  }

  @Override
  public int read() throws IOException {
    int result = googleDriveStream.read();
    if (result != -1) {
      currentPos++;
    }
    return result;
  }

  @Override
  public void seek(long pos) throws IOException {
    googleDriveStream.close();
    googleDriveStream = getDriveInputStreamForPosition(pos);
    currentPos = pos;
  }

  private InputStream getDriveInputStreamForPosition(long pos) throws IOException {
    return getDriveInputStream(filePath, pos);
  }

  @Override
  public long getPos() throws IOException {
    return currentPos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    InputStream tempStream = getDriveInputStreamForPosition(position);
    int bytesRead = tempStream.read(buffer, offset, length);
    tempStream.close();
    return bytesRead;
  }

  @Override
  public void close() throws IOException {
    googleDriveStream.close();
    super.close();
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    if (offset < 0 || length < 0 || offset + length > buffer.length) {
      throw new IndexOutOfBoundsException("Invalid offset or length for readFully.");
    }
    int bytesRead = read(position, buffer, offset, length);
    if (bytesRead < length) {
      throw new IOException("Could not read the requested number of bytes from the stream.");
    }
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
   if (buffer == null) {
      throw new NullPointerException("Byte array cannot be null.");
    }
    readFully(position, buffer, 0, buffer.length);
  }
}
