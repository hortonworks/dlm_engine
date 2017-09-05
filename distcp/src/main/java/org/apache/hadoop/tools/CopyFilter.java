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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.lang.reflect.Constructor;

/**
 * Interface for excluding files from DistCp.
 *
 */
public abstract class CopyFilter {

  static final Log LOG = LogFactory.getLog(CopyFilter.class);

  /**
   * Default initialize method does nothing.
   */
  public void initialize() {}

  /**
   * Predicate to determine if a file can be excluded from copy.
   *
   * @param path a Path to be considered for copying
   * @return boolean, true to copy, false to exclude
   */
  public abstract boolean shouldCopy(Path path);

  /**
   * Public factory method which returns the appropriate implementation of
   * CopyFilter.
   *
   * @param conf DistCp configuration
   * @return An instance of the appropriate CopyFilter
   */
  public static CopyFilter getCopyFilter(Configuration conf) {
    String filtersClassName = conf.get(DistCpConstants.CONF_LABEL_FILTERS_CLASS);

    if (filtersClassName != null) {
      try {
        Class<? extends CopyFilter> filtersClass = conf.getClassByName(filtersClassName).asSubclass(CopyFilter.class);
        filtersClassName = filtersClass.getName();
        Constructor<? extends CopyFilter> constructor = filtersClass.getDeclaredConstructor(Configuration.class);
        return constructor.newInstance(conf);
      } catch (Exception e) {
        LOG.error("Unable to instantiate " + filtersClassName, e);
      }
    }
    return getDefaultCopyFilter(conf);
  }

  private static CopyFilter getDefaultCopyFilter(Configuration conf) {
    String filtersFilename = conf.get(DistCpConstants.CONF_LABEL_FILTERS_FILE);

    if (filtersFilename == null) {
      return new TrueCopyFilter();
    } else {
      String filterFilename = conf.get(
          DistCpConstants.CONF_LABEL_FILTERS_FILE);
      return new RegexCopyFilter(filterFilename);
    }
  }
}
