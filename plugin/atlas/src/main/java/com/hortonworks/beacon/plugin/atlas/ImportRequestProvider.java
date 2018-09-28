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
package com.hortonworks.beacon.plugin.atlas;

import com.hortonworks.beacon.plugin.DataSet;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AttributeTransform;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hortonworks.beacon.plugin.atlas.ExportRequestProvider.ATLAS_TYPE_HDFS_PATH;
import static com.hortonworks.beacon.plugin.atlas.ExportRequestProvider.ATLAS_TYPE_HIVE_DB;

/**
 * Helper class to create import request.
 */
final class ImportRequestProvider {
    protected static final Logger LOG = LoggerFactory.getLogger(ImportRequestProvider.class);

    private static final String ATTRIBUTE_NAME_CLUSTER_NAME = ".clusterName";
    private static final String ATTRIBUTE_NAME_NAME = ".name";
    private static final String ATTRIBUTE_NAME_REPLICATED_TO = "replicatedTo";
    private static final String ATTRIBUTE_NAME_REPLICATED_FROM = "replicatedFrom";
    private static final String ATTRIBUTE_NAME_LOCATION = ".location";

    private static final String HDFS_PATH_CLUSTER_NAME = ATLAS_TYPE_HDFS_PATH + ATTRIBUTE_NAME_CLUSTER_NAME;
    private static final String HIVE_DB_CLUSTER_NAME = ATLAS_TYPE_HIVE_DB + ATTRIBUTE_NAME_CLUSTER_NAME;

    private static final String HDFS_PATH_NAME = ATLAS_TYPE_HDFS_PATH + ATTRIBUTE_NAME_NAME;
    private static final String HIVE_DB_NAME = ATLAS_TYPE_HIVE_DB + ATTRIBUTE_NAME_NAME;
    private static final String HIVE_DB_LOCATION = ATLAS_TYPE_HIVE_DB + ATTRIBUTE_NAME_LOCATION;

    private static final String TRANSFORM_ENTITY_SCOPE = "__entity";

    private ImportRequestProvider() {

    }

    private static final String REPLICATED_TAG_NAME = "%s_replicated";

    public static AtlasImportRequest create(DataSet dataSet) {
        return create(dataSet.getType(),
                dataSet.getSourceDataSet(),
                dataSet.getTargetDataSet(),
                AtlasProcess.getAtlasServerName(dataSet.getSourceCluster()),
                AtlasProcess.getAtlasServerName(dataSet.getTargetCluster()),
                AtlasProcess.getFsEndpoint(dataSet.getSourceCluster()),
                AtlasProcess.getFsEndpoint(dataSet.getTargetCluster()),
                (dataSet.getSourceCluster() != null) ? dataSet.getSourceCluster().getName() : StringUtils.EMPTY);
    }

    public static AtlasImportRequest create(DataSet.DataSetType dataSetType,
                                            String sourceDataSet, String targetDataSet,
                                            String sourceClusterName, String targetClusterName,
                                            String sourcefsEndpoint, String targetFsEndpoint,
                                            String sourceClusterFullyQualifiedName) {
        AtlasImportRequest request = new AtlasImportRequest();

        addTransforms(dataSetType, request.getOptions(),
                sourceClusterName, targetClusterName,
                sourceDataSet, targetDataSet,
                sourcefsEndpoint, targetFsEndpoint);

        addReplicatedFrom(request.getOptions(), sourceClusterFullyQualifiedName);

        if (LOG.isDebugEnabled()) {
            LOG.debug("AtlasProcess: importRequest: {}", request);
        }

        return request;
    }

    private static void addTransforms(DataSet.DataSetType dataSetType, Map<String, String> options,
                                      String sourceClusterName, String targetClusterName,
                                      String sourceDataSet, String targetDataSet,
                                      String sourcefsEndpoint, String targetFsEndpoint) {
        List<AttributeTransform> transforms = new ArrayList<>();

        String sanitizedSourceClusterName = sanitizeForClassificationName(sourceClusterName);
        addClassificationTransform(transforms,
                String.format(REPLICATED_TAG_NAME, sanitizedSourceClusterName));

        addClearReplicationAttributesTransform(transforms);
        addClusterRenameTransform(transforms, dataSetType, sourceClusterName, targetClusterName);

        if (dataSetType == DataSet.DataSetType.HIVE && !sourceDataSet.equals(targetDataSet)) {
            addDataSetRenameTransform(transforms, dataSetType, sourceDataSet, targetDataSet);
        }

        if (dataSetType == DataSet.DataSetType.HDFS) {
            String srcFsUri = ExportRequestProvider.getPathWithTrailingPathSeparator(sourcefsEndpoint, sourceDataSet);
            String tgtFsUri = ExportRequestProvider.getPathWithTrailingPathSeparator(targetFsEndpoint, targetDataSet);

            if (!srcFsUri.equals(tgtFsUri)) {
                addDataSetRenameTransform(transforms, dataSetType, srcFsUri, tgtFsUri);
            }
        }

        if (dataSetType == DataSet.DataSetType.HIVE) {
            addLocationTransform(transforms, sourceClusterName, targetClusterName,
                    sourcefsEndpoint, targetFsEndpoint);
        }

        options.put(AtlasImportRequest.TRANSFORMERS_KEY, AtlasType.toJson(transforms));
    }

    private static void addLocationTransform(List<AttributeTransform> transforms,
                                             String sourceClusterName, String targetClusterName,
                                             String srcFsUri, String tgtFsUri) {
        transforms.add(create(
                HIVE_DB_LOCATION, "STARTS_WITH_IGNORE_CASE: " + srcFsUri,
                HIVE_DB_LOCATION, "REPLACE_PREFIX: = :" + srcFsUri + "=" + tgtFsUri
                )
        );

        transforms.add(create(
                HIVE_DB_LOCATION, "STARTS_WITH_IGNORE_CASE: " + sourceClusterName,
                HIVE_DB_LOCATION, "REPLACE_PREFIX: = :" + sourceClusterName + "=" + targetClusterName
                )
        );
    }

    private static void addDataSetRenameTransform(List<AttributeTransform> transforms,
                                                  DataSet.DataSetType dataSetType,
                                                  String sourceDataSet, String targetDataSet) {
        String propertyName = dataSetType == DataSet.DataSetType.HDFS ? HDFS_PATH_NAME : HIVE_DB_NAME;

        transforms.add(create(
                propertyName, "EQUALS: " + sourceDataSet,
                propertyName, "SET: " + targetDataSet));
    }

    private static void addClusterRenameTransform(List<AttributeTransform> transforms, DataSet.DataSetType dataSetType,
                                                  String sourceClusterName, String targetClusterName) {

        String propertyName = dataSetType == DataSet.DataSetType.HDFS ? HDFS_PATH_CLUSTER_NAME : HIVE_DB_CLUSTER_NAME;

        transforms.add(create(propertyName, "EQUALS: " + sourceClusterName,
                                propertyName, "SET: " + targetClusterName));
    }

    private static void addReplicatedFrom(Map<String, String> options, String sourceClusterName) {
        options.put(AtlasImportRequest.OPTION_KEY_REPLICATED_FROM, sourceClusterName);
    }

    private static void addClassificationTransform(List<AttributeTransform> transforms, String classificationName) {
        transforms.add(create("__entity", "topLevel: ",
                "__entity", "ADD_CLASSIFICATION: " + classificationName));
    }

    private static String sanitizeForClassificationName(String s) {
        if (StringUtils.isEmpty(s)) {
            return s;
        }

        return s.replace('-', '_').replace(' ', '_');
    }

    private static void addClearReplicationAttributesTransform(List<AttributeTransform> transforms) {
        Map<String, String> actions = new HashMap<>();
        actions.put(TRANSFORM_ENTITY_SCOPE + "." + ATTRIBUTE_NAME_REPLICATED_TO, "CLEAR:");
        actions.put(TRANSFORM_ENTITY_SCOPE + "." + ATTRIBUTE_NAME_REPLICATED_FROM, "CLEAR:");

        transforms.add(new AttributeTransform(null, actions));
    }

    private static AttributeTransform create(String conditionLhs, String conditionRhs,
                                             String actionLhs, String actionRhs) {
        return new AttributeTransform(Collections.singletonMap(conditionLhs, conditionRhs),
                Collections.singletonMap(actionLhs, actionRhs));
    }
}
