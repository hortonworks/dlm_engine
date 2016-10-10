package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.entity.Acl;
import com.hortonworks.beacon.entity.Entity;
import com.hortonworks.beacon.entity.EntityType;
import com.hortonworks.beacon.entity.exceptions.EntityNotRegisteredException;
import com.hortonworks.beacon.entity.store.ConfigurationStore;
import com.hortonworks.beacon.exceptions.BeaconException;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Created by sramesh on 9/29/16.
 */
public final class EntityHelper {
    private EntityHelper() {
    }

    public static Properties getCustomProperties(final Properties properties, final Set<String> entityElements) {
        Properties customProperties = new Properties();
        for (Map.Entry<Object, Object> property : properties.entrySet()) {
            if (!entityElements.contains(property.getKey().toString())) {
                customProperties.put(property.getKey(), property.getValue());
            }

        }
        return customProperties;
    }

    public static Acl buildACL(final String aclOwner,
                               final String aclGroup,
                               final String aclPermission) {
        Acl acl = new Acl();
        acl.setOwner(aclOwner);
        acl.setGroup(aclGroup);
        acl.setPermission(aclPermission);

        return acl;
    }


    public static List<String> getTags(Entity entity) {
        String rawTags = entity.getTags();

//        switch (entity.getEntityType()) {
//            case REPLICATIONPOLICY:
//                rawTags = ((ReplicationPolicy) entity).getTags();
//                break;
//
//            case CLUSTER:
//                rawTags = ((Cluster) entity).getTags();
//                break;
//
//            default:
//                break;
//        }

        List<String> tags = new ArrayList<String>();
        if (!StringUtils.isEmpty(rawTags)) {
            for(String tag : rawTags.split(",")) {
                tags.add(tag.trim());
            }
        }

        return tags;
    }

    public static <T extends Entity> T getEntity(String type, String entityName) throws BeaconException {
        EntityType entityType;
        try {
            entityType = EntityType.getEnum(type);
        } catch (IllegalArgumentException e) {
            throw new BeaconException("Invalid entity type: " + type, e);
        }
        return getEntity(entityType, entityName);
    }

    public static <T extends Entity> T getEntity(EntityType type, String entityName) throws BeaconException {
        ConfigurationStore configStore = ConfigurationStore.get();
        T entity = configStore.get(type, entityName);
        if (entity == null) {
            throw new EntityNotRegisteredException(entityName + " (" + type + ") not found");
        }
        return entity;
    }
}
