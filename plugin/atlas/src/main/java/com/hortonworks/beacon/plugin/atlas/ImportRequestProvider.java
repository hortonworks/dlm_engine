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

import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Helper class to create import request.
 */
final class ImportRequestProvider {
    protected static final Logger LOG = LoggerFactory.getLogger(ImportRequestProvider.class);

    private ImportRequestProvider() {

    }

    private static final String REPLICATED_TAG_NAME = "%s_replicated";

    static final String IMPORT_TRANSFORM_FORMAT =
            "{ \"Asset\": { \"qualifiedName\":[ \"replace:@%s:@%s\"], "
                    + "\"*\":[ \"clearAttrValue:replicatedTo,replicatedFrom\", "
                    + "\"addClassification:"
                    + REPLICATED_TAG_NAME
                    + "\" ] } }";

    public static AtlasImportRequest create(String sourceClusterName, String targetClusterName) {
        AtlasImportRequest request = new AtlasImportRequest();
        addTransforms(request.getOptions(), sourceClusterName, targetClusterName);
        addMetaInfoUpdate(request.getOptions(), sourceClusterName);

        if (LOG.isDebugEnabled()) {
            LOG.debug("AtlasProcess: importRequest: {}", request);
        }

        return request;
    }

    private static void addTransforms(Map<String, String> options,
                                      String sourceClusterName,
                                      String targetClusterName) {

        String sanitizedSourceClusterName = sanitizeForClassificationName(sourceClusterName);
        options.put(AtlasImportRequest.TRANSFORMS_KEY,
                String.format(IMPORT_TRANSFORM_FORMAT,
                        sourceClusterName, targetClusterName, sanitizedSourceClusterName));
    }

    private static void addMetaInfoUpdate(Map<String, String> options, String sourceClusterName) {
        options.put(AtlasImportRequest.OPTION_KEY_REPLICATED_FROM, sourceClusterName);
    }

    private static String sanitizeForClassificationName(String s) {
        if (StringUtils.isEmpty(s)) {
            return s;
        }

        return s.replace('-', '_').replace(' ', '_');
    }
}