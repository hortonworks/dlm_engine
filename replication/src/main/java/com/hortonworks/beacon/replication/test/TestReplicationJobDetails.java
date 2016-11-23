package com.hortonworks.beacon.replication.test;

import com.hortonworks.beacon.replication.ReplicationJobDetails;
import com.hortonworks.beacon.util.DateUtil;

import java.util.Date;
import java.util.Properties;

public class TestReplicationJobDetails extends ReplicationJobDetails {

    int sleepTime;

    public int getSleepTime() {
        return sleepTime;
    }

    public void setSleepTime(int sleepTime) {
        this.sleepTime = sleepTime;
    }

    public TestReplicationJobDetails() {
    }

    public TestReplicationJobDetails(String name, String type, int frequency, Date startTime, Date endTime, int sleepTime) {
        super(name, type, frequency, startTime, endTime);
        this.sleepTime = sleepTime;
    }

    @Override
    public TestReplicationJobDetails setReplicationJobDetails(Properties properties) {
        return new TestReplicationJobDetails(properties.getProperty(TestDRProperties.JOB_NAME.getName()),
                properties.getProperty(TestDRProperties.JOB_TYPE.getName()),
                Integer.parseInt(properties.getProperty(TestDRProperties.JOB_FREQUENCY.getName())),
                DateUtil.parseDate(properties.getProperty(TestDRProperties.START_TIME.getName())),
                DateUtil.parseDate(properties.getProperty(TestDRProperties.START_TIME.getName())),
                Integer.parseInt(properties.getProperty(TestDRProperties.SLEEP_TIME.getName()))
        );
    }

    @Override
    public void validateReplicationProperties(Properties properties) {
        for (TestDRProperties option : TestDRProperties.values()) {
            if (properties.get(option.getName()) == null && option.isRequired()) {
                throw new IllegalArgumentException("Missing DR property for Test Replication : " + option.getName());
            }
        }
    }
}