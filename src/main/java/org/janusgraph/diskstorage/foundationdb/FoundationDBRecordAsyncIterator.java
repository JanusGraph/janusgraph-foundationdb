// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.subspace.Subspace;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoundationDBRecordAsyncIterator extends FoundationDBRecordIterator {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBRecordAsyncIterator.class);

    private final FoundationDBTx tx;
    private final FoundationDBRangeQuery rangeQuery;

    private AsyncIterator<KeyValue> heldEntries;

    protected static final int TRANSACTION_TOO_OLD_CODE = 1007;

    public FoundationDBRecordAsyncIterator(
        Subspace ds,
        FoundationDBTx tx,
        FoundationDBRangeQuery query,
        final AsyncIterator<KeyValue> result,
        KeySelector selector) {
        super(ds, result, selector);

        this.tx = tx;
        this.rangeQuery = query;
        this.heldEntries = result;
    }

    private FDBException unwrapException(Throwable err) {
        Throwable curr = err;
        while (curr != null && !(curr instanceof FDBException)) {
            curr = curr.getCause();
        }
        if (curr != null) {
            return (FDBException) curr;
        } else {
            return null;
        }
    }

    @Override
    protected void fetchNext() {
        while (true) {
            try {
                super.fetchNext();
                break;
            } catch (RuntimeException e) {

                log.info("current async iterator canceled and current transaction could be re-started when conditions meet");
                heldEntries.cancel();

                Throwable t = e.getCause();
                FDBException fdbException = unwrapException(t);
                // Capture the transaction too old error
                if (tx.getIsolationLevel() != FoundationDBTx.IsolationLevel.SERIALIZABLE &&
                    fdbException != null && (fdbException.getCode() == TRANSACTION_TOO_OLD_CODE)) {
                    tx.restart();
                    heldEntries = tx.getRangeIter(rangeQuery, fetched);
                    // Initiate record iterator again, but keep cursor "fetched" not changed.
                    this.entries = heldEntries;
                    this.nextKeyValueEntry = null;
                } else {
                    log.error("The throwable is not restartable", t);
                    throw e;
                }
            } catch (Exception e) {
                log.error("AsyncIterator fetchNext() encountered exception", e);
                throw e;
            }
        }
    }

    @Override
    public void close() {
        heldEntries.cancel();
    }

    @Override
    public void remove() {
        heldEntries.remove();
    }
}
