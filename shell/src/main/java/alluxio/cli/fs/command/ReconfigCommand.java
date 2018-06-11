/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.cli.fs.command;

import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

/**
 * Send reconfig request to master.
 */
public final class ReconfigCommand extends AbstractFileSystemCommand {

  public ReconfigCommand(FileSystem fileSystem) {
    super(fileSystem);
  }

  @Override
  protected int getNumOfArgs() {
    return 0;
  }

  @Override
  public String getCommandName() {
    return "reconfig";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    mFileSystem.reconfig();
    return 0;
  }

  @Override
  public String getUsage() {
    return "reconfig";
  }

  @Override
  public String getDescription() {
    return "Request cluster to reload configuration.";
  }
}
