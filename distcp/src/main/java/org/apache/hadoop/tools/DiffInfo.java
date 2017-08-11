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

import java.util.Comparator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;

/**
 * Information presenting a rename/delete op derived from a snapshot diff entry.
 * This includes the source file/dir of the rename/delete op, and the target
 * file/dir of a rename op.
 */
class DiffInfo {
  static final Comparator<DiffInfo> sourceComparator = new Comparator<DiffInfo>() {
    @Override
    public int compare(DiffInfo d1, DiffInfo d2) {
      return d2.source.compareTo(d1.source);
    }
  };

  static final Comparator<DiffInfo> targetComparator = new Comparator<DiffInfo>() {
    @Override
    public int compare(DiffInfo d1, DiffInfo d2) {
      return d1.target == null ? ((d2.target == null)? 0 : -1) :
        (d2.target ==  null ? 1 : d1.target.compareTo(d2.target));
    }
  };

  /** The source file/dir of the rename or deletion op */
  private Path source;
  /** The target file/dir of the rename op. Null means the op is deletion. */
  private Path target;

  private SnapshotDiffReport.DiffType type;
  /**
   * The intermediate file/dir for the op. For a rename or a delete op,
   * we first rename the source to this tmp file/dir.
   */
  private Path tmp;

  DiffInfo(final Path source, final Path target,
      SnapshotDiffReport.DiffType type) {
    assert source != null;
    this.source = source;
    this.target= target;
    this.type = type;
  }

  void setSource(final Path source) {
    this.source = source;
  }

  Path getSource() {
    return source;
  }

  void setTarget(final Path target) {
    this.target = target;
  }

  Path getTarget() {
    return target;
  }

  public void setType(final SnapshotDiffReport.DiffType type){
    this.type = type;
  }

  public SnapshotDiffReport.DiffType getType(){
    return type;
  }

  void setTmp(Path tmp) {
    this.tmp = tmp;
  }

  Path getTmp() {
    return tmp;
  }

  @Override
  public String toString() {
    return type + ": src=" + String.valueOf(source) + " tgt="
        + String.valueOf(target) + " tmp=" + String.valueOf(tmp);
  }
}
