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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

/**
 * A CopyFilter which compares Java Regex Patterns to each Path to determine
 * whether a file should be copied.
 */
public class RegexCopyFilter extends CopyFilter {

  private static final Log LOG = LogFactory.getLog(RegexCopyFilter.class);
  private File filtersFile;
  private List<Pattern> filters;

  /**
   * Constructor, sets up a File object to read filter patterns from and
   * the List to store the patterns.
   */
  protected RegexCopyFilter(String filtersFilename) {
    filtersFile = new File(filtersFilename);
    filters = new ArrayList<>();
  }

  /**
   * Loads a list of filter patterns for use in shouldCopy.
   */
  @Override
  public void initialize() {
    BufferedReader reader = null;
    try {
      InputStream is = new FileInputStream(filtersFile);
      reader = new BufferedReader(new InputStreamReader(is,
          Charset.forName("UTF-8")));
      String line;
      while ((line = reader.readLine()) != null) {
        Pattern pattern = Pattern.compile(line);
        filters.add(pattern);
      }
    } catch (FileNotFoundException notFound) {
      LOG.error("Can't find filters file " + filtersFile);
    } catch (IOException cantRead) {
      LOG.error("An error occurred while attempting to read from " +
          filtersFile);
    } finally {
      IOUtils.cleanup(LOG, reader);
    }
  }

  /**
   * Sets the list of filters to exclude files from copy.
   * Simplifies testing of the filters feature.
   *
   * @param filtersList a list of Patterns to be excluded
   */
  @VisibleForTesting
  protected final void setFilters(List<Pattern> filtersList) {
    this.filters = filtersList;
  }

  @Override
  public boolean shouldCopy(Path path) {
    for (Pattern filter : filters) {
      if (filter.matcher(path.toString()).matches()) {
        return false;
      }
    }
    return true;
  }
}
