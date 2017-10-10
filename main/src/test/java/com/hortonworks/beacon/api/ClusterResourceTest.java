/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.api;

import com.hortonworks.beacon.XTestCase;
import com.hortonworks.beacon.entity.ClusterProperties;
import com.hortonworks.beacon.entity.exceptions.ValidationException;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.util.PropertiesIgnoreCase;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Test class ClusterResource.
 */
public class ClusterResourceTest extends XTestCase {

    private ClusterResource resource = new ClusterResource();

    @BeforeClass
    public void setupClass() throws BeaconException {
        initializeServices(null);
    }


    @Test(expectedExceptions = ValidationException.class)
    public void testValidateExclusionProps() throws Exception {
        PropertiesIgnoreCase properties = new PropertiesIgnoreCase();
        List<String> exclusionProps = ClusterProperties.updateExclusionProps();
        for (String prop : exclusionProps) {
            properties.put(prop, prop);
        }
        resource.validateExclusionProp(properties);
    }
}
