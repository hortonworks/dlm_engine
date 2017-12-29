/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.store.executors;

import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.constants.BeaconConstants;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.EntityManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

abstract class BaseExecutor {
    protected static final String AND = " AND ";
    protected static final String OR = " OR ";
    protected static final String EQUAL = " = ";
    protected final EntityManager entityManager = RequestContext.get().getEntityManager();

    protected Map<String, List<String>> parseFilterBy(String filterBy) {
        // Filter the results by specific field:value, eliminate empty values
        Map<String, List<String>> filterByFieldValues = new HashMap<>();
        if (StringUtils.isNotEmpty(filterBy)) {
            String[] fieldValueArray = filterBy.split(BeaconConstants.COMMA_SEPARATOR);
            for (String fieldValue : fieldValueArray) {
                String[] splits = fieldValue.split(BeaconConstants.COLON_SEPARATOR, 2);
                String filterByField = splits[0];
                if (splits.length == 2 && !splits[1].equals("")) {
                    List<String> currentValue = filterByFieldValues.get(filterByField);
                    if (currentValue == null) {
                        currentValue = new ArrayList<>();
                        filterByFieldValues.put(filterByField, currentValue);
                    }

                    String[] fields = splits[1].split("\\|");
                    currentValue.addAll(Arrays.asList(fields));
                }
            }
        }
        return filterByFieldValues;
    }
}
