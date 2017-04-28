/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.beacon.entity.store;

import com.hortonworks.beacon.client.entity.Entity;
import com.hortonworks.beacon.client.entity.EntityType;
import com.hortonworks.beacon.config.BeaconConfig;
import com.hortonworks.beacon.entity.exceptions.EntityAlreadyExistsException;
import com.hortonworks.beacon.entity.exceptions.StoreAccessException;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.service.BeaconService;
import com.hortonworks.beacon.util.FileSystemClientFactory;
import org.apache.commons.codec.CharEncoding;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Persistent store for Beacon entity resources.
 */
public final class ConfigurationStoreService implements BeaconService {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationStoreService.class);
    public static final String SERVICE_NAME = ConfigurationStoreService.class.getName();
    private static final EntityType[] ENTITY_LOAD_ORDER = new EntityType[]{
        EntityType.CLUSTER, EntityType.REPLICATIONPOLICY, };

    private BeaconConfig config = BeaconConfig.getInstance();

    private static final FsPermission STORE_PERMISSION =
            new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);
    private static final Logger AUDIT = LoggerFactory.getLogger("AUDIT");
    private static final String UTF_8 = CharEncoding.UTF_8;

    private ThreadLocal<Entity> updatesInProgress = new ThreadLocal<>();

    private final Map<EntityType, ConcurrentHashMap<String, Entity>> dictionary
            = new HashMap<>();

    private static final Entity NULL = new Entity() {
        @Override
        public String getName() {
            return "NULL";
        }

        @Override
        public String getTags() {
            return null;
        }
    };

    private FileSystem fs;
    private String storePath;

    public ConfigurationStoreService() {
        for (EntityType type : EntityType.values()) {
            dictionary.put(type, new ConcurrentHashMap<String, Entity>());
        }

        storePath = config.getEngine().getConfigStoreUri();
        fs = initializeFileSystem();
    }

    /**
     * Beacon owns this dir on HDFS which no one has permissions to read.
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
                FileSystemClientFactory.mkdirs(fileSystem, storeUri, STORE_PERMISSION);
            }

            return fileSystem;
        } catch (Exception e) {
            throw new RuntimeException("Unable to bring up config store for path: " + storePath, e);
        }
    }

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws BeaconException {
        LOG.info("Number of threads used to restore entities: {}", config.getEngine().getLoadNumThreads());

        LOG.info("TimeOut to load Entities is taken as {} mins", config.getEngine().getLoadTimeout());

        for (final EntityType type : ENTITY_LOAD_ORDER) {
            loadEntity(type);
        }
    }

    @Override
    public void destroy() throws BeaconException {

    }

    private void loadEntity(final EntityType type) throws BeaconException {
        try {
            final ConcurrentHashMap<String, Entity> entityMap = dictionary.get(type);
            FileStatus[] files = fs.globStatus(new Path(storePath, type.name() + Path.SEPARATOR + "*"));
            if (files != null && files.length != 0) {

                final ExecutorService service = Executors.newFixedThreadPool(config.getEngine().getLoadNumThreads());
                for (final FileStatus file : files) {
                    service.execute(new Runnable() {
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
                if (service.awaitTermination(config.getEngine().getLoadTimeout(), TimeUnit.SECONDS)) {
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
            if (getEntity(type, entity.getName()) == null) {
                persist(type, entity);
                dictionary.get(type).put(entity.getName(), entity);
            } else {
                throw new EntityAlreadyExistsException(
                        entity.toShortString() + " already registered with configuration store. "
                                + "Can't be submitted again. Try removing before submitting."
                );
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
     * loaded in memory yet, it will retrieve it from persistent store
     * just in time. On startup all the entities will be added to the
     * dictionary with null reference.
     * @throws com.hortonworks.beacon.exceptions.BeaconException
     */
    @SuppressWarnings("unchecked")
    public <T extends Entity> T getEntity(EntityType type, String name) throws BeaconException {
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

    /**
     * Remove an entity which is already stored in the config store.
     *
     * @param type - Entity type being removed
     * @param name - Name of the entity object being removed
     * @return - True is remove is successful, false if request entity doesn't
     * exist
     * @throws com.hortonworks.beacon.exceptions.BeaconException
     */
    public synchronized boolean remove(EntityType type, String name) throws IOException, BeaconException {
        Map<String, Entity> entityMap = dictionary.get(type);
        if (entityMap.containsKey(name)) {
            final String filename = getEntityFilePath(type, name);
            fs.delete(new Path(filename), false);
            entityMap.remove(name);
            AUDIT.info(type + " " + name + " is removed from config store");
            return true;
        }
        return false;
    }

    public synchronized void initiateUpdate(Entity entity) throws BeaconException {
        if (getEntity(entity.getEntityType(), entity.getName()) == null || updatesInProgress.get() != null) {
            throw new BeaconException(
                    "An update for " + entity.toShortString() + " is already in progress or doesn't exist");
        }
        updatesInProgress.set(entity);
    }

    public synchronized void update(EntityType type, Entity entity) throws BeaconException {
        if (updatesInProgress.get() == entity) {
            updateInternal(type, entity);
        } else {
            throw new BeaconException(entity.toShortString() + " is not initialized for update");
        }
    }

    public void cleanupUpdateInit() {
        updatesInProgress.set(null);
    }

    private void persist(EntityType type, Entity entity) throws IOException, BeaconException {
        final String filename = getEntityFilePath(entity.getEntityType(), entity.getName());
        FSDataOutputStream fdos = fs.create(new Path(filename));
        Yaml yaml = new Yaml();
        OutputStreamWriter writer = new OutputStreamWriter(fdos);
        try {
            yaml.dump(entity, writer);
            LOG.info("Persisted configuration {}/{}", type, entity.getName());
        } finally {
            writer.close();
        }
    }

    private synchronized <T extends Entity> T restore(EntityType type, String name)
            throws IOException, BeaconException {

        final String filename = getEntityFilePath(type, name);
        FSDataInputStream fdis = fs.open(new Path(filename));
        InputStreamReader reader = new InputStreamReader(fdis);
        Yaml yaml = new Yaml();

        try {
            return (T) yaml.load(reader);
        } finally {
            reader.close();
        }
    }

    private synchronized void updateInternal(EntityType type, Entity entity) throws BeaconException {
        try {
            if (getEntity(type, entity.getName()) != null) {
                ConcurrentHashMap<String, Entity> entityMap = dictionary.get(type);
                persist(type, entity);
                entityMap.put(entity.getName(), entity);
            } else {
                throw new NoSuchElementException(entity.getName() + " (" + type + ") not found");
            }
        } catch (IOException e) {
            throw new StoreAccessException(e);
        }
    }

    private String getEntityFilePath(final EntityType type, final String entityName) throws IOException {
        return storePath.toString() + type + Path.SEPARATOR + URLEncoder.encode(entityName, UTF_8) + ".yml";
    }
}
