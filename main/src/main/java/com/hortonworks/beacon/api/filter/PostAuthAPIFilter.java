/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.api.filter;

import com.hortonworks.beacon.RequestContext;
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

/**
 * The auth filters set remote user in HttpServletRequest. PostAuthAPIFilter sets this user in RequestContext.
 */
public class PostAuthAPIFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(PostAuthAPIFilter.class);

    @Override
    public void init(FilterConfig filterConfig) {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException,
            ServletException {
        String userName = ((HttpServletRequest)request).getRemoteUser();
        LOG.debug("Authenticated user: {}", userName);
        RequestContext.get().setUser(userName);
        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
    }
}
