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

package com.hortonworks.beacon.replication.fs;

import org.apache.commons.el.ExpressionEvaluatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import javax.servlet.jsp.el.ExpressionEvaluator;

/**
 * Utilities for feed eviction.
 */
public final class EvictionHelper {

    private static final Logger LOG = LoggerFactory.getLogger(EvictionHelper.class);

    private static final ExpressionEvaluator EVALUATOR = new ExpressionEvaluatorImpl();
    private static final ExpressionHelper RESOLVER = ExpressionHelper.get();

    private EvictionHelper(){}

    public static Long evalExpressionToMilliSeconds(String period) throws ELException {
        return (Long) EVALUATOR.evaluate("${" + period + "}", Long.class, RESOLVER, RESOLVER);
    }

}
