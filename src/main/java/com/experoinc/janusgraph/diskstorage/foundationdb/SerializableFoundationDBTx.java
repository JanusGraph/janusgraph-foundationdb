// Copyright 2018 Expero Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.experoinc.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ted Wilmes (twilmes@gmail.com)
 */
public class SerializableFoundationDBTx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(SerializableFoundationDBTx.class);

    private volatile Transaction tx;

    private final Database db;

    public SerializableFoundationDBTx(Database db, Transaction t, BaseTransactionConfig config) {
        super(config);
        tx = t;
        this.db = db;
    }

    public Transaction getTransaction() {
        return tx;
    }

    public synchronized  void restart() throws BackendException {
//        tx.commit();
        tx.close();
        tx = db.createTransaction();
    }


    @Override
    public synchronized void rollback() throws BackendException {
        super.rollback();
        if (tx == null) return;
        if (log.isTraceEnabled())
            log.trace("{} rolled back", this.toString(), new TransactionClose(this.toString()));
        try {
            tx.cancel();
            tx.close();
            tx = null;
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public synchronized void commit() throws BackendException {
        super.commit();
        if (tx == null) return;
        if (log.isTraceEnabled())
            log.trace("{} committed", this.toString(), new TransactionClose(this.toString()));

        try {
            tx.commit().get();
            tx.close();
            tx = null;
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }
    }


    @Override
    public String toString() {
        return getClass().getSimpleName() + (null == tx ? "nulltx" : tx.toString());
    }

    private static class TransactionClose extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionClose(String msg) {
            super(msg);
        }
    }
}
