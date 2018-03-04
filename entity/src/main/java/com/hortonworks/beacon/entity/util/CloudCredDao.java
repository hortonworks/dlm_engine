/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
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
