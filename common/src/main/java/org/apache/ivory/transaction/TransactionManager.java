/*
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
package org.apache.ivory.transaction;

import org.apache.ivory.IvoryException;
import org.apache.ivory.monitors.Dimension;
import org.apache.ivory.monitors.Monitored;
import org.apache.log4j.Logger;

public class TransactionManager {
    private static final Logger LOG = Logger.getLogger(TransactionManager.class);

    private static ThreadLocal<AtomicActions> trans = null;

    public static void startTransaction() throws IvoryException {
        if (trans == null)
            trans = new ThreadLocal<AtomicActions>() {
                @Override
                protected AtomicActions initialValue() {
                    return new AtomicActions();
                }
            };

        if (trans.get().isBegun())
            throw new IllegalStateException("Transaction " + getTransactionId() + " is already started");
        trans.get().begin();
    }

    public static String getTransactionId() {
        if(trans == null)
            return null;
        return trans.get().getId();
    }

    public static void performAction(Action action) throws IvoryException {
        if (trans != null && trans.get().isBegun() && !trans.get().isFinalized())
            trans.get().peform(action);
    }

    public static void rollback() {
        if(trans == null || trans.get().isFinalized())
            throw new IllegalStateException("Invalid transaction " + getTransactionId());
        try {
            trans.get().rollback();
            trans = null;   //reset for thread re-use
        } catch (Throwable e) {
            trans = null;   //reset for thread re-use
            LOG.error("Transaction " + getTransactionId() + " rollback failed!", e);
            alertRollbackFailure(getTransactionId());
        }
    }

    @Monitored(event = "TransactionRollbackFailed")
    private static String alertRollbackFailure(@Dimension(value = "transactionId") String transactionId) {
        return transactionId;
    }

    public static void commit() throws IvoryException {
        if(trans == null || trans.get().isFinalized())
            throw new IllegalStateException("Invalid transaction " + getTransactionId());
        trans.get().commit();
        trans = null;   //reset for thread re-use
    }
}