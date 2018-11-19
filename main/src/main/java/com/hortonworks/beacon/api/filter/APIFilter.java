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

package com.hortonworks.beacon.api.filter;

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.beacon.RequestContext;
import com.hortonworks.beacon.Timer;
import com.hortonworks.beacon.api.PropertiesIgnoreCase;
import com.hortonworks.beacon.constants.BeaconConstants;
import com.hortonworks.beacon.log.BeaconLogUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Initializing API context and logging request parameters.
 */
public class APIFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(APIFilter.class);
    public static final String MASK = "********";
    private static final Set<String> MASKING_KEYWORDS = new HashSet<String>() {
        {
            add("aws.access.key");
            add("aws.secret.key");
            add("cloud.encryptionKey");
            add("wasb.access.key");
            add("gcs.private.key.id");
            add("gcs.private.key");
            add("gcs.client.email");
        }
    };

    @Override
    public void init(FilterConfig filterConfig) {
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException {
        String timerKey = null;
        Timer timer = null;
        try {
            HttpServletRequest request = (HttpServletRequest) servletRequest;
            MultiReadHttpServletRequest multiReadRequest = new MultiReadHttpServletRequest(request);

            String apiPath = request.getPathInfo();
            timerKey = request.getMethod() + "->" + apiPath;
            RequestContext.setInitialValue();
            RequestContext requestContext = RequestContext.get();
            timer = requestContext.startTimer(timerKey);
            BeaconLogUtils.prefixRequest(requestContext.getRequestId());
            String queryString = request.getQueryString();
            LOG.info("ThreadId: {}, HTTP method: {}, Query Parameters: {}, APIPath: {}",
                    Thread.currentThread().getName(), request.getMethod(), queryString, apiPath);
            String body = multiReadRequest.getRequestBody();
            logHeaders(request);
            if (StringUtils.isNotBlank(body)) {
                LOG.info("Request body: {}", mask(body));
            }

            filterChain.doFilter(multiReadRequest, servletResponse);
        } finally {
            if (timer != null) {
                timer.stop();
            }
            if (StringUtils.isNotEmpty(timerKey)) {
                if (RequestContext.get().getMethodTimers().size() != 0) {
                    LOG.info("Time duration of method(s) executed : {}", RequestContext.get().getMethodTimers()
                            .toString());
                }
            }
            // Clear the thread level request context
            RequestContext.get().clear();
            NDC.remove();
            NDC.clear();
        }
    }

    private void logHeaders(HttpServletRequest request) {
        Enumeration<String> headers = request.getHeaderNames();
        StringBuilder builder = new StringBuilder();
        while (headers.hasMoreElements()) {
            String header = headers.nextElement();
            builder.append(header).append(BeaconConstants.EQUAL_SEPARATOR).append(request.getHeader(header))
                    .append(BeaconConstants.SEMICOLON_SEPARATOR);
        }
        LOG.debug("Headers: {}", builder);
    }

    @VisibleForTesting
    public String mask(String body) throws IOException {
        StringBuilder maskedEntries = new StringBuilder();
        PropertiesIgnoreCase properties = new PropertiesIgnoreCase(body);
        for (Map.Entry entry: properties.entrySet()) {
            if (MASKING_KEYWORDS.contains(entry.getKey())) {
                maskedEntries.append(entry.getKey()).append(BeaconConstants.EQUAL_SEPARATOR).append(MASK);
            } else {
                maskedEntries.append(entry.getKey()).append(BeaconConstants.EQUAL_SEPARATOR).append(entry.getValue());
            }
            maskedEntries.append(BeaconConstants.NEW_LINE);
        }
        return maskedEntries.toString();
    }

    @Override
    public void destroy() {
    }
}
