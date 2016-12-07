package com.hortonworks.beacon.replication.test;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.DRReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;

import java.util.Properties;

public class TestDRImpl implements DRReplication {

    private Properties properties = null;

    public TestDRImpl(ReplicationJobDetails details) {
        this.properties = details.getProperties();
    }

    @Override
    public void establishConnection() {
    }

    @Override
    public void performReplication() throws BeaconException {
        try {
            int sleepTime = Integer.parseInt(properties.getProperty("sleepTime"));
            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }
        } catch (Exception e) {
            throw new BeaconException(e);
        }
    }
}