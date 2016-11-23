package com.hortonworks.beacon.replication.test;

import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.replication.DRReplication;
import com.hortonworks.beacon.replication.ReplicationJobDetails;

public class TestDRImpl implements DRReplication {

    private TestReplicationJobDetails details;

    public TestDRImpl(ReplicationJobDetails details) {
        this.details = (TestReplicationJobDetails) details;
    }

    @Override
    public void establishConnection() {
    }

    @Override
    public void performReplication() throws BeaconException {
        try {
            int sleepTime = details.getSleepTime();
            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }
        } catch (Exception e) {
            throw new BeaconException(e);
        }
    }
}