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

import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Test AtlasPluginStats.
 */
public class AtlasPluginStatsTest {
    @Test
    public void verify() throws JSONException {
        final long expectedStats = 10L;

        final AtlasPluginStats stats = new AtlasPluginStats();
        stats.updateExport(0);
        stats.updateImport(0);

        assertNotNull(stats.getPluginStats());

        final JSONObject statsData = stats.getPluginStats();
        assertStats(statsData, AtlasPluginStats.EXPORT_KEY, 0L);
        assertStats(statsData, AtlasPluginStats.IMPORT_KEY, 0L);

        stats.updateImport(expectedStats);
        stats.updateExport(9);
        stats.updateExport(1);

        assertStats(statsData, AtlasPluginStats.EXPORT_KEY, expectedStats);
        assertStats(statsData, AtlasPluginStats.IMPORT_KEY, expectedStats);
    }

    private void assertStats(JSONObject statsData, String exportKey, long expected) throws JSONException {
        assertEquals(statsData.get(exportKey), expected);
    }
}
