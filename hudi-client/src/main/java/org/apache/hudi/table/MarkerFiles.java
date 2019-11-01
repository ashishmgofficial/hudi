/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.IOType;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Operates on marker files for a given write action (commit, delta commit, compaction).
 */
public class MarkerFiles {

  private static final Logger LOG = LogManager.getLogger(MarkerFiles.class);

  public static String stripMarkerSuffix(String path) {
    return path.substring(0, path.indexOf(HoodieTableMetaClient.MARKER_EXTN));
  }

  private final String instantTime;
  private final FileSystem fs;
  private final Path markerDirPath;
  private final String basePath;

  public MarkerFiles(FileSystem fs, String basePath, String markerFolderPath, String instantTime) {
    this.instantTime = instantTime;
    this.fs = fs;
    this.markerDirPath = new Path(markerFolderPath);
    this.basePath = basePath;
  }

  public MarkerFiles(HoodieTable<?> table, String instantTime) {
    this(table.getMetaClient().getFs(),
        table.getMetaClient().getBasePath(),
        table.getMetaClient().getMarkerFolderPath(instantTime),
        instantTime);
  }

  public void quietDeleteMarkerDir() {
    try {
      deleteMarkerDir();
    } catch (HoodieIOException ioe) {
      LOG.warn("Error deleting marker directory for instant " + instantTime, ioe);
    }
  }

  /**
   * Delete Marker directory corresponding to an instant.
   */
  public boolean deleteMarkerDir() {
    try {
      boolean result = fs.delete(markerDirPath, true);
      if (result) {
        LOG.info("Removing marker directory at " + markerDirPath);
      } else {
        LOG.info("No marker directory to delete at " + markerDirPath);
      }
      return result;
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  public boolean doesMarkerDirExist() throws IOException {
    return fs.exists(markerDirPath);
  }

  public List<String> createdAndMergedDataPaths() throws IOException {
    List<String> dataFiles = new LinkedList<>();
    FSUtils.processFiles(fs, markerDirPath.toString(), (status) -> {
      String pathStr = status.getPath().toString();
      if (pathStr.contains(HoodieTableMetaClient.MARKER_EXTN) && !pathStr.endsWith(IOType.APPEND.name())) {
        dataFiles.add(translateMarkerToDataPath(pathStr));
      }
      return true;
    }, false);
    return dataFiles;
  }

  private String translateMarkerToDataPath(String markerPath) {
    String rPath = stripMarkerFolderPrefix(markerPath);
    return MarkerFiles.stripMarkerSuffix(rPath);
  }

  public List<String> allMarkerFilePaths() throws IOException {
    List<String> markerFiles = new ArrayList<>();
    FSUtils.processFiles(fs, markerDirPath.toString(), fileStatus -> {
      markerFiles.add(stripMarkerFolderPrefix(fileStatus.getPath().toString()));
      return true;
    }, false);
    return markerFiles;
  }

  private String stripMarkerFolderPrefix(String fullMarkerPath) {
    ValidationUtils.checkArgument(fullMarkerPath.contains(HoodieTableMetaClient.MARKER_EXTN));
    String markerRootPath = Path.getPathWithoutSchemeAndAuthority(
        new Path(String.format("%s/%s/%s", basePath, HoodieTableMetaClient.TEMPFOLDER_NAME, instantTime))).toString();
    int begin = fullMarkerPath.indexOf(markerRootPath);
    ValidationUtils.checkArgument(begin >= 0,
        "Not in marker dir. Marker Path=" + fullMarkerPath + ", Expected Marker Root=" + markerRootPath);
    return fullMarkerPath.substring(begin + markerRootPath.length() + 1);
  }

  /**
   * The marker path will be <base-path>/.hoodie/.temp/<instant_ts>/2019/04/25/filename.marker.writeIOType.
   */
  public Path create(String partitionPath, String dataFileName, IOType type) {
    Path path = FSUtils.getPartitionPath(markerDirPath, partitionPath);
    try {
      fs.mkdirs(path); // create a new partition as needed.
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + path, e);
    }
    String markerFileName = String.format("%s%s.%s", dataFileName, HoodieTableMetaClient.MARKER_EXTN, type.name());
    Path markerPath = new Path(path, markerFileName);
    try {
      LOG.info("Creating Marker Path=" + markerPath);
      fs.create(markerPath, false).close();
    } catch (IOException e) {
      throw new HoodieException("Failed to create marker file " + markerPath, e);
    }
    return markerPath;
  }

}