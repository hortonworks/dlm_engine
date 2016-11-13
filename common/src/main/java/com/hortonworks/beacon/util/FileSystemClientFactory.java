package com.hortonworks.beacon.util;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.security.SecurityUtil;
import com.hortonworks.beacon.config.BeaconConfig;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

/**
 * A factory implementation to dole out FileSystem handles based on the logged in user.
 */
public final class FileSystemClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemClientFactory.class);
    private static final FileSystemClientFactory INSTANCE = new FileSystemClientFactory();

    public static final String MR_JT_ADDRESS_KEY = "mapreduce.jobtracker.address";
    public static final String YARN_RM_ADDRESS_KEY = "yarn.resourcemanager.address";
    public static final String FS_DEFAULT_NAME_KEY = CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;

    /**
     * Constant for the configuration property that indicates the Name node principal.
     */
    public static final String NN_PRINCIPAL = "dfs.namenode.kerberos.principal";

    private FileSystemClientFactory() {
    }

    public static FileSystemClientFactory get() {
        return INSTANCE;
    }

    public FileSystem createBeaconFileSystem(final URI uri) throws BeaconException {
        Validate.notNull(uri, "uri cannot be null");

        try {
            Configuration conf = new Configuration();
            if (UserGroupInformation.isSecurityEnabled()) {
                BeaconConfig config = BeaconConfig.getInstance();
                conf.set(SecurityUtil.NN_PRINCIPAL, config.getEngine().getPrincipal());
            }

            return createFileSystem(UserGroupInformation.getLoginUser(), uri, conf);
        } catch (IOException e) {
            throw new BeaconException("Exception while getting FileSystem for: " + uri, e);
        }
    }

    /**
     * Return a FileSystem created with the authenticated proxy user for the specified conf.
     *
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return FileSystem created with the provided proxyUser/group.
     * @throws BeaconException
     *          if the filesystem could not be created.
     */
    public FileSystem createProxiedFileSystem(final Configuration conf)
            throws BeaconException {
        Validate.notNull(conf, "configuration cannot be null");

        String nameNode = getNameNode(conf);
        try {
            return createProxiedFileSystem(new URI(nameNode), conf);
        } catch (URISyntaxException e) {
            throw new BeaconException("Exception while getting FileSystem for: " + nameNode, e);
        }
    }

    /**
     * Return a DistributedFileSystem created with the authenticated proxy user for the specified conf.
     *
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return DistributedFileSystem created with the provided proxyUser/group.
     * @throws BeaconException
     *          if the filesystem could not be created.
     */
    public DistributedFileSystem createDistributedProxiedFileSystem(final Configuration conf) throws BeaconException {
        Validate.notNull(conf, "configuration cannot be null");

        String nameNode = getNameNode(conf);
        try {
            return createDistributedFileSystem(UserGroupInformation.getCurrentUser(), new URI(nameNode), conf);
        } catch (URISyntaxException e) {
            throw new BeaconException("Exception while getting Distributed FileSystem for: " + nameNode, e);
        } catch (IOException e) {
            throw new BeaconException("Exception while getting Distributed FileSystem: ", e);
        }
    }

    private static String getNameNode(Configuration conf) {
        return conf.get(FS_DEFAULT_NAME_KEY);
    }

    /**
     * This method is called from with in a workflow execution context.
     *
     * @param uri uri
     * @return file system handle
     * @throws BeaconException
     */
    public FileSystem createProxiedFileSystem(final URI uri) throws BeaconException {
        return createProxiedFileSystem(uri, new Configuration());
    }

    public FileSystem createProxiedFileSystem(final URI uri,
                                              final Configuration conf) throws BeaconException {
        Validate.notNull(uri, "uri cannot be null");

        try {
            return createFileSystem(UserGroupInformation.getCurrentUser(), uri, conf);
        } catch (IOException e) {
            throw new BeaconException("Exception while getting FileSystem", e);
        }
    }

    /**
     * Return a FileSystem created with the provided user for the specified URI.
     *
     * @param ugi user group information
     * @param uri  file system URI.
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return FileSystem created with the provided user/group.
     * @throws BeaconException
     *          if the filesystem could not be created.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public FileSystem createFileSystem(UserGroupInformation ugi, final URI uri,
                                       final Configuration conf) throws BeaconException {
        validateInputs(ugi, uri, conf);

        try {
            // prevent beacon impersonating beacon, no need to use doas
            final String proxyUserName = ugi.getShortUserName();
            if (proxyUserName.equals(UserGroupInformation.getLoginUser().getShortUserName())) {
                LOG.info("Creating FS for the login user {}, impersonation not required",
                        proxyUserName);
                return FileSystem.get(uri, conf);
            }

            LOG.info("Creating FS impersonating user {}", proxyUserName);
            return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                public FileSystem run() throws Exception {
                    return FileSystem.get(uri, conf);
                }
            });
        } catch (InterruptedException ex) {
            throw new BeaconException("Exception creating FileSystem:" + ex.getMessage(), ex);
        } catch (IOException ex) {
            throw new BeaconException("Exception creating FileSystem:" + ex.getMessage(), ex);
        }
    }

    /**
     * Return a DistributedFileSystem created with the provided user for the specified URI.
     *
     * @param ugi user group information
     * @param uri  file system URI.
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return DistributedFileSystem created with the provided user/group.
     * @throws BeaconException
     *          if the filesystem could not be created.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public DistributedFileSystem createDistributedFileSystem(UserGroupInformation ugi, final URI uri,
                                                             final Configuration conf) throws BeaconException {
        validateInputs(ugi, uri, conf);
        FileSystem returnFs;
        try {
            // prevent beacon impersonating beacon, no need to use doas
            final String proxyUserName = ugi.getShortUserName();
            if (proxyUserName.equals(UserGroupInformation.getLoginUser().getShortUserName())) {
                LOG.info("Creating Distributed FS for the login user {}, impersonation not required",
                        proxyUserName);
                returnFs = DistributedFileSystem.get(uri, conf);
            } else {
                LOG.info("Creating FS impersonating user {}", proxyUserName);
                returnFs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                    public FileSystem run() throws Exception {
                        return DistributedFileSystem.get(uri, conf);
                    }
                });
            }

            return (DistributedFileSystem) returnFs;
        } catch (InterruptedException ex) {
            throw new BeaconException("Exception creating FileSystem:" + ex.getMessage(), ex);
        } catch (IOException ex) {
            throw new BeaconException("Exception creating FileSystem:" + ex.getMessage(), ex);
        }
    }

    public static void mkdirs(FileSystem fs, Path path,
                              FsPermission permission) throws IOException {
        if (!FileSystem.mkdirs(fs, path, permission)) {
            throw new IOException("mkdir failed for " + path);
        }
    }

    private void validateInputs(UserGroupInformation ugi, final URI uri,
                                final Configuration conf) throws BeaconException {
        Validate.notNull(ugi, "ugi cannot be null");
        Validate.notNull(conf, "configuration cannot be null");

        try {
            if (UserGroupInformation.isSecurityEnabled()) {
                LOG.debug("Revalidating Auth Token with auth method {}",
                        UserGroupInformation.getLoginUser().getAuthenticationMethod().name());
                UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
            }
        } catch (IOException ioe) {
            throw new BeaconException("Exception while getting FileSystem. Unable to check TGT for user "
                    + ugi.getShortUserName(), ioe);
        }

        validateNameNode(uri, conf);
    }

    private void validateNameNode(URI uri, Configuration conf) throws BeaconException {
        String nameNode = uri.getAuthority();
        if (StringUtils.isBlank(nameNode)) {
            nameNode = getNameNode(conf);
            if (StringUtils.isNotBlank(nameNode)) {
                try {
                    new URI(nameNode).getAuthority();
                } catch (URISyntaxException ex) {
                    throw new BeaconException("Exception while getting FileSystem", ex);
                }
            }
        }
    }
}