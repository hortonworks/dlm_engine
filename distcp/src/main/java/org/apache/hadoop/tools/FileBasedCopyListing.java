/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.Credentials;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * FileBasedCopyListing implements the CopyListing interface,
 * to create the copy-listing for DistCp,
 * by iterating over all source paths mentioned in a specified input-file.
 */
public class FileBasedCopyListing extends CopyListing {

  private final CopyListing globbedListing;
  /**
   * Constructor, to initialize base-class.
   * @param configuration The input Configuration object.
   * @param credentials - Credentials object on which the FS delegation tokens are cached. If null
   * delegation token caching is skipped
   */
  public FileBasedCopyListing(Configuration configuration, Credentials credentials) {
    super(configuration, credentials);
    globbedListing = new GlobbedCopyListing(getConf(), credentials);
  }

  /** {@inheritDoc} */
  @Override
  protected void validatePaths(DistCpOptions options)
      throws IOException, InvalidInputException {
  }

  /**
   * Implementation of CopyListing::buildListing().
   *   Iterates over all source paths mentioned in the input-file.
   * @param pathToListFile Path on HDFS where the listing file is written.
   * @param options Input Options for DistCp (indicating source/target paths.)
   * @throws IOException
   */
  @Override
  public void doBuildListing(Path pathToListFile, DistCpOptions options) throws IOException {
    DistCpOptions newOption = new DistCpOptions(options);
    newOption.setSourcePaths(fetchFileList(options.getSourceFileListing()));
    globbedListing.buildListing(pathToListFile, newOption);
  }

  private List<Path> fetchFileList(Path sourceListing) throws IOException {
    List<Path> result = new ArrayList<Path>();
    FileSystem fs = sourceListing.getFileSystem(getConf());
    BufferedReader input = null;
    try {
      input = new BufferedReader(new InputStreamReader(fs.open(sourceListing),
          Charset.forName("UTF-8")));
      String line = input.readLine();
      while (line != null) {
        result.add(new Path(line));
        line = input.readLine();
      }
    } finally {
      IOUtils.closeStream(input);
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  protected long getBytesToCopy() {
    return globbedListing.getBytesToCopy();
  }

  /** {@inheritDoc} */
  @Override
  protected long getNumberOfPaths() {
    return globbedListing.getNumberOfPaths();
  }
}
