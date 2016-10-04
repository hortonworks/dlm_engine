package com.hortonworks.beacon.entity.store;

import com.esotericsoftware.yamlbeans.YamlReader;
import com.esotericsoftware.yamlbeans.YamlWriter;
import com.hortonworks.beacon.entity.Acl;
import com.hortonworks.beacon.entity.Entity;
import com.hortonworks.beacon.entity.EntityType;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.exceptions.EntityAlreadyExistsException;
import com.hortonworks.beacon.exceptions.StoreAccessException;
import com.hortonworks.beacon.service.BeaconService;
import com.hortonworks.beacon.util.FileSystemClientFactory;
import com.hortonworks.beacon.util.config.BeaconConfig;
import org.apache.commons.codec.CharEncoding;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by sramesh on 9/30/16.
 */
public final class ConfigurationStore implements BeaconService {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationStore.class);
    private static final EntityType[] ENTITY_LOAD_ORDER = new EntityType[] {
            EntityType.CLUSTER, EntityType.REPLICATIONPOLICY,};

    private static final String LOAD_ENTITIES_THREADS = "config.store.num.threads.load.entities";
    private static final String TIMEOUT_MINS_LOAD_ENTITIES = "config.store.start.timeout.minutes";
    private int numThreads;
    private int restoreTimeOutInMins;
    private BeaconConfig config = new BeaconConfig();

    private static final FsPermission STORE_PERMISSION =
            new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);
    private static final Logger AUDIT = LoggerFactory.getLogger("AUDIT");
    private static final String UTF_8 = CharEncoding.UTF_8;

    private ThreadLocal<Entity> updatesInProgress = new ThreadLocal<Entity>();

    private final Map<EntityType, ConcurrentHashMap<String, Entity>> dictionary
            = new HashMap<EntityType, ConcurrentHashMap<String, Entity>>();

    private static final Entity NULL = new Entity() {
        @Override
        public String getName() {
            return "NULL";
        }

        @Override
        public String getTags() { return null; }

        @Override
        public Acl getAcl() {
            return null;
        }
    };

    private static final ConfigurationStore STORE = new ConfigurationStore();

    public static ConfigurationStore get() {
        return STORE;
    }
    private FileSystem fs;
    private String storePath;

    public FileSystem getFs() {
        return fs;
    }

    public String getStorePath() {
        return storePath;
    }

    private ConfigurationStore() {
        for (EntityType type : EntityType.values()) {
            dictionary.put(type, new ConcurrentHashMap<String, Entity>());
        }

        storePath = config.getConfigStoreUri();
        fs = initializeFileSystem();
    }

    /**
     * Falcon owns this dir on HDFS which no one has permissions to read.
     *
     * @return FileSystem handle
     */
    private FileSystem initializeFileSystem() {
        try {
            Path storeUri = new Path(storePath);
            FileSystem fileSystem =
                    FileSystemClientFactory.get().createBeaconFileSystem(storeUri.toUri());
            if (!fileSystem.exists(storeUri)) {
                LOG.info("Creating configuration store directory: {}", storePath);
                // set permissions so config store dir is owned by falcon alone
                FileSystemClientFactory.mkdirs(fileSystem, storeUri, STORE_PERMISSION);
            }

            return fileSystem;
        } catch (Exception e) {
            throw new RuntimeException("Unable to bring up config store for path: " + storePath, e);
        }
    }

//    private File initializeFileSystem() {
//        try {
//            FileSystem fileSystem = new File(storePath.toUri());
//            if (!fileSystem.exists()) {
//                LOG.info("Creating configuration store directory: {}", storePath);
//                // set permissions so config store dir is owned by falcon alone
////                FileSystemClientFactory.mkdirs(fileSystem, storePath, STORE_PERMISSION);
//            }
//
//            return fileSystem;
//        } catch (Exception e) {
//            throw new RuntimeException("Unable to bring up config store for path: " + storePath, e);
//        }
//    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }

    @Override
    public void init() throws BeaconException {
        try {
            numThreads = Integer.parseInt(config.get().getProperty(LOAD_ENTITIES_THREADS, "100"));
            LOG.info("Number of threads used to restore entities: {}", restoreTimeOutInMins);
        } catch (NumberFormatException nfe) {
            throw new BeaconException("Invalid value specified for start up property \""
                    + LOAD_ENTITIES_THREADS + "\".Please provide an integer value");
        }
        try {
            restoreTimeOutInMins = Integer.parseInt(config.get().
                    getProperty(TIMEOUT_MINS_LOAD_ENTITIES, "30"));
            LOG.info("TimeOut to load Entities is taken as {} mins", restoreTimeOutInMins);
        } catch (NumberFormatException nfe) {
            throw new BeaconException("Invalid value specified for start up property \""
                    + TIMEOUT_MINS_LOAD_ENTITIES + "\".Please provide an integer value");
        }

        for (final EntityType type : ENTITY_LOAD_ORDER) {
            loadEntity(type);
        }
    }

    private void loadEntity(final EntityType type) throws BeaconException {
        try {
            final ConcurrentHashMap<String, Entity> entityMap = dictionary.get(type);
            FileStatus[] files = fs.globStatus(new Path(storePath, type.name() + Path.SEPARATOR + "*"));
            if (files != null && files.length != 0) {

                final ExecutorService service = Executors.newFixedThreadPool(numThreads);
                for (final FileStatus file : files) {
                    service.execute( new Runnable() {
                        public void run() {
                            try {
                                String fileName = file.getPath().getName();
                                String encodedEntityName = fileName.substring(0, fileName.length() - 4); // drop
                                // ".yml"
                                String entityName = URLDecoder.decode(encodedEntityName, UTF_8);
                                Entity entity = restore(type, entityName);
                                LOG.info("Restored configuration {}/{}", type, entityName);
                                entityMap.put(entityName, entity);
                            } catch (IOException | BeaconException e) {
                                LOG.error("Unable to restore entity of", file);
                            }
                        }
                    });
                }
                service.shutdown();
                if (service.awaitTermination(restoreTimeOutInMins, TimeUnit.MINUTES)) {
                    LOG.info("Restored Configurations for entity type: {} ", type.name());
                } else {
                    LOG.warn("Timed out while waiting for all threads to finish while restoring entities "
                            + "for type: {}", type.name());
                }
                // Checking if all entities were loaded
                if (entityMap.size() != files.length) {
                    throw new BeaconException("Unable to restore configurations for entity type " + type.name());
                }
            }
        } catch (IOException e) {
            throw new BeaconException("Unable to restore configurations", e);
        } catch (InterruptedException e) {
            throw new BeaconException("Failed to restore configurations in 10 minutes for entity type " + type.name());
        }
    }

    public synchronized void publish(EntityType type, Entity entity) throws BeaconException {
        try {
            if (get(type, entity.getName()) == null) {
                persist(type, entity);
                dictionary.get(type).put(entity.getName(), entity);
            } else {
                throw new EntityAlreadyExistsException(
                        entity.toShortString() + " already registered with configuration store. "
                                + "Can't be submitted again. Try removing before submitting.");
            }
        } catch (IOException e) {
            throw new StoreAccessException(e);
        }
//        AUDIT.info(type + "/" + entity.getName() + " is published into config store");
    }

    /**
     * @param type - Entity type that is being retrieved
     * @param name - Name as it appears in the entity xml definition
     * @param <T>  - Actual Entity object type
     * @return - Entity object from internal dictionary, If the object is not
     *         loaded in memory yet, it will retrieve it from persistent store
     *         just in time. On startup all the entities will be added to the
     *         dictionary with null reference.
     * @throws com.hortonworks.beacon.exceptions.BeaconException
     */
    @SuppressWarnings("unchecked")
    public <T extends Entity> T get(EntityType type, String name) throws BeaconException {
        ConcurrentHashMap<String, Entity> entityMap = dictionary.get(type);
        if (entityMap.containsKey(name)) {
            if (updatesInProgress.get() != null && updatesInProgress.get().getEntityType() == type
                    && updatesInProgress.get().getName().equals(name)) {
                return (T) updatesInProgress.get();
            }
            T entity = (T) entityMap.get(name);
            if (entity == NULL) { // Object equality being checked
                try {
                    entity = this.restore(type, name);
                } catch (IOException e) {
                    throw new StoreAccessException(e);
                }
                LOG.info("Restored configuration {}/{}", type, name);
                entityMap.put(name, entity);
                return entity;
            } else {
                return entity;
            }
        } else {
            return null;
        }
    }

    public Collection<String> getEntities(EntityType type) {
        return Collections.unmodifiableCollection(dictionary.get(type).keySet());
    }

    public void cleanupUpdateInit() {
        updatesInProgress.set(null);
    }

    @Override
    public void destroy() throws BeaconException {

    }


    private void persist(EntityType type, Entity entity) throws IOException, BeaconException {
        final String filename = storePath.toString() + type + Path.SEPARATOR + URLEncoder.encode(entity.getName(), UTF_8) + ".yml";
        FileWriter writer = new FileWriter(filename);
        YamlWriter yamlWriter = new YamlWriter(writer);

        try {
            yamlWriter.write(entity);
            LOG.info("Persisted configuration {}/{}", type, entity.getName());
        } finally {
            yamlWriter.close();
        }
    }

    private synchronized <T extends Entity> T restore(EntityType type, String name)
            throws IOException, BeaconException {

        final String filename = storePath.toString() + type + Path.SEPARATOR + URLEncoder.encode(name, UTF_8) + ".yml";
        FileReader reader = new FileReader(filename);
        YamlReader yamlReader = new YamlReader(reader);

        try {
            return (T) yamlReader.read(type.getEntityClass());
        } finally {
            yamlReader.close();
        }
    }
}
