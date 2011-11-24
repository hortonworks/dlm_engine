/**
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *    OR LOSS OR CORRUPTION OF DATA.
 */

package com.hortonworks.beacon.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.hortonworks.beacon.exceptions.BeaconException;

/**
 * Repository of services initialized at startup.
 */
public final class Services implements Iterable<BeaconService> {

    private static final Services INSTANCE = new Services();

    private Services() {
    }

    public static Services get() {
        return INSTANCE;
    }

    private final Map<String, BeaconService> services =
            new LinkedHashMap<>();

    public synchronized void register(BeaconService service) {
        if (!services.containsKey(service.getClass().getName())) {
            services.put(service.getClass().getName(), service);
        }
    }

    synchronized void deregister(String serviceName) throws BeaconException {

        if (!services.containsKey(serviceName)) {
            throw new BeaconException("Service {} is not registered", serviceName);
        } else {
            services.remove(serviceName);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends BeaconService> T getService(Class<T> tClass) {
        if (services.containsKey(tClass.getName())) {
            return (T) services.get(tClass.getName());
        } else {
            throw new NoSuchElementException("Service " + tClass.getName() + " not registered with registry");
        }
    }

    public boolean isRegistered(String serviceName) {
        return services.containsKey(serviceName);
    }

    @Override
    public Iterator<BeaconService> iterator() {
        return services.values().iterator();
    }

    public Iterator<BeaconService> reverseIterator() {
        List<BeaconService> list = new ArrayList(services.values());
        Collections.reverse(list);
        return list.iterator();
    }

    public void reset() {
        services.clear();
    }
}
