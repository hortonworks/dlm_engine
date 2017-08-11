/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package org.apache.hadoop.tools.mapred;

import org.apache.hadoop.tools.util.RetriableCommand;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This class extends Retriable command to implement the creation of directories
 * with retries on failure.
 */
public class RetriableDirectoryCreateCommand extends RetriableCommand {

  /**
   * Constructor, taking a description of the action.
   * @param description Verbose description of the copy operation.
   */
  public RetriableDirectoryCreateCommand(String description) {
    super(description);
  }

  /**
   * Implementation of RetriableCommand::doExecute().
   * This implements the actual mkdirs() functionality.
   * @param arguments Argument-list to the command.
   * @return Boolean. True, if the directory could be created successfully.
   * @throws Exception IOException, on failure to create the directory.
   */
  @Override
  protected Object doExecute(Object... arguments) throws Exception {
    assert arguments.length == 2 : "Unexpected argument list.";
    Path target = (Path)arguments[0];
    Mapper.Context context = (Mapper.Context)arguments[1];

    FileSystem targetFS = target.getFileSystem(context.getConfiguration());
    return targetFS.mkdirs(target);
  }
}
