package com.hortonworks.beacon.entity;

/**
 * Created by sramesh on 9/30/16.
 */
public abstract class Entity {
    public abstract String getName();

    public abstract String getTags();

    public abstract ACL getACL();

    public EntityType getEntityType() {
        for (EntityType type : EntityType.values()) {
            if (type.getEntityClass().equals(getClass())) {
                return type;
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !o.getClass().equals(this.getClass())) {
            return false;
        }

        Entity entity = (Entity) o;

        String name = getName();
        return !(name != null ? !name.equals(entity.getName()) : entity.getName() != null);
    }

    @Override
    public int hashCode() {
        String clazz = this.getClass().getName();

        String name = getName();
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + clazz.hashCode();
        return result;
    }

    public String toShortString() {
        return "(" + getEntityType().name().toLowerCase() + ") " + getName();
    }
}
