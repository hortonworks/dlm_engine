package com.hortonworks.beacon.util;

import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class FSUtils {
    private FSUtils() {
    }

    private static final Logger LOG = LoggerFactory.getLogger(FSUtils.class);

    public static Configuration conf = new Configuration();

    public static Configuration getConf() {
        return conf;
    }

    public static void setConf(Configuration conf) {
        FSUtils.conf = conf;
    }

    public static boolean isHCFS(Path filePath) throws BeaconException {
        if (filePath == null) {
            throw new BeaconException("filePath cannot be empty");
        }

        String scheme;
        try {
            FileSystem f = FileSystem.get(filePath.toUri(), getConf());
            scheme = f.getScheme();
            if (StringUtils.isBlank(scheme)) {
                throw new BeaconException("Cannot get valid scheme for " + filePath);
            }
        } catch (IOException e) {
            throw new BeaconException(e);
        }

        boolean inTestMode = BeaconConfig.getInstance().getEngine().getInTestMode();
        LOG.info("inTestMode: {}", inTestMode);
        if (inTestMode) {
            return ((scheme.toLowerCase().contains("hdfs") || scheme.toLowerCase().contains("file")) ? false : true);
        }

        return (scheme.toLowerCase().contains("hdfs") ? false : true);
    }
}