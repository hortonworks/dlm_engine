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

package com.hortonworks.beacon.entity.util;

import com.hortonworks.beacon.client.entity.CloudCred;
import com.hortonworks.beacon.client.entity.CloudCred.Config;
import com.hortonworks.beacon.client.resource.CloudCredList;
import com.hortonworks.beacon.exceptions.BeaconException;
import com.hortonworks.beacon.store.bean.CloudCredBean;
import com.hortonworks.beacon.store.executors.CloudCredExecutor;
import com.hortonworks.beacon.store.executors.CloudCredExecutor.CloudCredQuery;
import com.hortonworks.beacon.store.executors.CloudCredListExecutor;
import com.hortonworks.beacon.util.DateUtil;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Persistence based cloud cred interactions.
 */
public class CloudCredDao {

    public void submit(CloudCred cloudCred) {
        CloudCredBean bean = getBean(cloudCred);
        CloudCredExecutor executor = new CloudCredExecutor(bean);
        executor.submit();
    }

    private CloudCredBean getBean(CloudCred cloudCred) {
        CloudCredBean bean = new CloudCredBean();
        bean.setId(cloudCred.getId());
        bean.setName(cloudCred.getName());
        bean.setProvider(cloudCred.getProvider());
        bean.setAuthType(cloudCred.getAuthType());
        bean.setConfiguration(buildStoreConfig(cloudCred.getConfigs()));
        return bean;
    }

    public CloudCred cloudCredResults(String cloudCredId) {
        CloudCredBean bean = getCloudCredBean(cloudCredId);
        return buildCloudCred(bean);
    }

    public CloudCred getCloudCred(String cloudCredId) {
        CloudCredBean credBean = getCloudCredBean(cloudCredId);
        return buildCloudCred(credBean);
    }

    private CloudCredBean getCloudCredBean(String cloudCredId) {
        CloudCredBean bean = new CloudCredBean();
        bean.setId(cloudCredId);
        CloudCredExecutor executor = new CloudCredExecutor(bean);
        return executor.get(CloudCredQuery.GET_CLOUD_CRED);
    }

    private CloudCred buildCloudCred(CloudCredBean bean) {
        CloudCred element = new CloudCred();
        element.setId(bean.getId());
        element.setName(bean.getName());
        element.setProvider(bean.getProvider());
        element.setAuthType(bean.getAuthType());
        element.setConfigs(buildClientConfig(bean.getConfiguration()));
        element.setCreationTime(DateUtil.formatDate(bean.getCreationTime()));
        element.setLastModifiedTime(DateUtil.formatDate(bean.getLastModifiedTime()));
        return element;
    }

    private Map<Config, String> buildClientConfig(Map<String, String> map) {
        Map<Config, String> configs = new HashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            configs.put(Config.forValue(entry.getKey()), entry.getValue());
        }
        return configs;
    }

    private Map<String, String> buildStoreConfig(Map<Config, String> map) {
        Map<String, String> configs = new HashMap<>();
        for (Map.Entry<Config, String> entry : map.entrySet()) {
            configs.put(entry.getKey().getName(), entry.getValue());
        }
        return configs;
    }

    public void update(CloudCred cloudCred) {
        CloudCredBean bean = getBean(cloudCred);
        bean.setLastModifiedTime(new Date());
        CloudCredExecutor executor = new CloudCredExecutor(bean);
        executor.update(CloudCredQuery.UPDATE_CLOUD_CRED);
    }

    public void delete(String cloudCredId) {
        CloudCredBean bean =new CloudCredBean();
        bean.setId(cloudCredId);
        CloudCredExecutor executor = new CloudCredExecutor(bean);
        executor.delete(CloudCredQuery.DELETE_CLOUD_CRED);
    }

    public CloudCredList listCloudCred(String filterBy, String orderBy, String sortOrder,
                                       Integer offset, Integer resultsPerPage) throws BeaconException {
        CloudCredListExecutor executor = new CloudCredListExecutor();
        long count = executor.countListCloudCred(filterBy);
        List<CloudCredBean> beans = executor.listCloudCred(filterBy, orderBy, sortOrder, offset, resultsPerPage);
        List<CloudCred> elements = new ArrayList<>();
        for (int i = 0; i<beans.size(); i++) {
            elements.add(buildCloudCred(beans.get(i)));
        }
        return new CloudCredList(count, elements);
    }
}
