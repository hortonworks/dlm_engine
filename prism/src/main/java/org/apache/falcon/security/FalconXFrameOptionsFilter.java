/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;

/**
 * X-Frame-Options filter before processing the request.
 */
public class FalconXFrameOptionsFilter extends XFrameOptionsFilter {
    private static final Logger LOG = LoggerFactory.getLogger(FalconCSRFFilter.class);

    private boolean isXframeFilterEnabled;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        super.init(filterConfig);
        isXframeFilterEnabled = SecurityUtil.isXframeFilterEnabled();
        if (isXframeFilterEnabled) {
            LOG.info("Falcon is running with X-Frame-Options filter enabled");
        } else {
            LOG.info("X-Frame-Options filter is not enabled.");
        }
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
                         final FilterChain filterChain) throws IOException, ServletException {
        if (isXframeFilterEnabled) {
            super.doFilter(request, response, filterChain);
        } else {
            filterChain.doFilter(request, response);
        }
    }

    @Override
    public void destroy() {
        super.destroy();
    }
}
