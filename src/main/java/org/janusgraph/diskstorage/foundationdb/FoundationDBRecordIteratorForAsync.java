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
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.util.RecordIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoundationDBRecordIteratorForAsync implements RecordIterator<KeyValueEntry> {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBRecordIteratorForAsync.class);

    private final Subspace ds;
    private final FoundationDBTx tx;
    private AsyncIterator<KeyValue> entries;
    private final KeySelector selector;
    KeyValue nextKeyValue = null;

    private final byte[] startKey;
    private final byte[] endKey;
    private final int limit;
    private int fetched;

    protected static final int TRANSACTION_TOO_OLD_CODE = 1007;

    public FoundationDBRecordIteratorForAsync(Subspace ds,
                                              FoundationDBTx tx,
                                              byte[] startKey, byte[] endKey,
                                              int limit, final AsyncIterator<KeyValue> result,
                                              KeySelector selector) {
        this.ds = ds;
        this.tx = tx;
        this.entries = result;
        this.selector = selector;
        this.startKey = startKey;
        this.endKey = endKey;
        this.limit = limit;
        this.fetched = 0;
    }

    @Override
    public boolean hasNext() {
        fetchNext();
        return (nextKeyValue != null);
    }

    @Override
    public KeyValueEntry next() {
        if (hasNext()) {
            KeyValue kv = nextKeyValue;
            nextKeyValue = null;
            StaticBuffer key = FoundationDBKeyValueStore.getBuffer(ds.unpack(kv.getKey()).getBytes(0));
            return new KeyValueEntry(key, FoundationDBKeyValueStore.getBuffer(kv.getValue()));
        } else {
            throw new IllegalStateException("does not have any key-value to retrieve");
        }
    }

    private FDBException unwrapException(Throwable err) {
        Throwable curr = err;
        while (curr != null && !(curr instanceof FDBException))  {
            curr = curr.getCause();
        }
        if (curr != null) {
            return (FDBException) curr;
        } else {
            return null;
        }
    }

    private void fetchNext() {
        while (true) {
            try {
                while (nextKeyValue == null && entries.hasNext()) {
                    KeyValue kv = entries.next();
                    fetched++;
                    StaticBuffer key = FoundationDBKeyValueStore.getBuffer(ds.unpack(kv.getKey()).getBytes(0));
                    if (selector.include(key)) {
                        nextKeyValue = kv;
                    }
                }
                break;
            } catch (RuntimeException e) {

                log.info("current async iterator canceled and current transaction could be re-started when conditions meet.");
                entries.cancel();

                Throwable t = e.getCause();
                FDBException fdbException = unwrapException(t);
                // Capture the transaction too old error
                if (tx.getIsolationLevel() != FoundationDBTx.IsolationLevel.SERIALIZABLE &&
                        fdbException != null && (fdbException.getCode() == TRANSACTION_TOO_OLD_CODE)) {
                    tx.restart();
                    entries = tx.getRangeIter(startKey, endKey, limit, fetched);
                } else {
                    log.error("The throwable is not restartable", t);
                    throw e;
                }
            }
            catch (Exception e) {
                log.error("AsyncIterator fetchNext() encountered exception", e);
                throw e;
            }
        }
    }

    @Override
    public void close() {
        entries.cancel();
    }

    @Override
    public void remove() {
        entries.remove();
    }
}
