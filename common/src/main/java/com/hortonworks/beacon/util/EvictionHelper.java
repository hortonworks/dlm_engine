/**
 *   Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
 *
 *   Except as expressly permitted in a written agreement between you or your
 *   company and Hortonworks, Inc. or an authorized affiliate or partner
 *   thereof, any use, reproduction, modification, redistribution, sharing,
 *   lending or other exploitation of all or any part of the contents of this
 *   software is strictly prohibited.
 */

package com.hortonworks.beacon.util;

import org.apache.commons.el.ExpressionEvaluatorImpl;

import javax.servlet.jsp.el.ELException;
import javax.servlet.jsp.el.ExpressionEvaluator;

/**
 * Utilities for dataset eviction.
 */
public final class EvictionHelper {

    private static final ExpressionEvaluator EVALUATOR = new ExpressionEvaluatorImpl();
    private static final ExpressionHelper RESOLVER = ExpressionHelper.get();

    private EvictionHelper(){}

    public static Long evalExpressionToMilliSeconds(String period) throws ELException {
        return (Long) EVALUATOR.evaluate("${" + period + "}", Long.class, RESOLVER, RESOLVER);
    }

}
