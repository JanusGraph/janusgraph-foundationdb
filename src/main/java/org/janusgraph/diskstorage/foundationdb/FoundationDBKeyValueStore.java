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

package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class FoundationDBKeyValueStore implements OrderedKeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBKeyValueStore.class);

    private static final StaticBuffer.Factory<byte[]> ENTRY_FACTORY = (array, offset, limit) -> {
        final byte[] bArray = new byte[limit - offset];
        System.arraycopy(array, offset, bArray, 0, limit - offset);
        return bArray;
    };


    private final DirectorySubspace db;
    private final String name;
    private final FoundationDBStoreManager manager;
    private boolean isOpen;

    public FoundationDBKeyValueStore(String n, DirectorySubspace data, FoundationDBStoreManager m) {
        db = data;
        name = n;
        manager = m;
        isOpen = true;
    }

     @Override
    public String getName() {
        return name;
    }

    private static FoundationDBTx getTransaction(StoreTransaction txh) {
        Preconditions.checkArgument(txh != null);
        return ((FoundationDBTx) txh);//.getTransaction();
    }

    @Override
    public synchronized void close() throws BackendException {
        try {
            //if(isOpen) db.close();
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }
        if (isOpen) manager.removeDatabase(this);
        isOpen = false;
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        FoundationDBTx tx = getTransaction(txh);
        try {
            byte[] databaseKey = db.pack(key.as(ENTRY_FACTORY));
            log.trace("db={}, op=get, tx={}", name, txh);
            final byte[] entry = tx.get(databaseKey);
            if (entry != null) {
                return getBuffer(entry);
            } else {
                return null;
            }
        } catch (Exception e) {
            log.error ("db={}, op=get, tx={} with exception", name, txh, e);

            if (e instanceof BackendException) {
                throw e;
            }
            else {
                throw new PermanentBackendException(e);
            }
        }
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return get(key,txh)!=null;
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        if (getTransaction(txh) == null) {
            log.warn("Attempt to acquire lock with transactions disabled");
        } //else we need no locking
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh, Integer integer) throws BackendException {
        insert(key, value, txh);
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException {
        if (manager.getMode() == FoundationDBStoreManager.NON_ASYNC) {
            return getSliceNonAsync(query, txh);
        } else {
            return getSliceAsync(query, txh);
        }
    }

    public RecordIterator<KeyValueEntry> getSliceNonAsync(KVQuery query, StoreTransaction txh) throws BackendException {
        log.trace("beginning db={}, op=getSlice, tx={}", name, txh);

        final FoundationDBTx tx = getTransaction(txh);
        final StaticBuffer keyStart = query.getStart();
        final StaticBuffer keyEnd = query.getEnd();
        final KeySelector selector = query.getKeySelector();
        final List<KeyValueEntry> result = new ArrayList<>();
        final byte[] foundKey = db.pack(keyStart.as(ENTRY_FACTORY));
        final byte[] endKey = db.pack(keyEnd.as(ENTRY_FACTORY));

        try {
            final List<KeyValue> results = tx.getRange(foundKey, endKey, query.getLimit());

            for (final KeyValue keyValue : results) {
                StaticBuffer key = getBuffer(db.unpack(keyValue.getKey()).getBytes(0));
                if (selector.include(key))
                    result.add(new KeyValueEntry(key, getBuffer(keyValue.getValue())));
            }
        } catch (Exception e) {
            log.error("db={}, op=getSliceNonAsync, tx={} with exception", name, txh, e);

            if (e instanceof BackendException) {
                throw e;
            }
            else {
                throw new PermanentBackendException(e);
            }
        }

        log.trace("db={}, op=getSliceNonAsync, tx={}, result-count={}", name, txh, result.size());

        return new FoundationDBRecordIterator(result);
    }


    public RecordIterator<KeyValueEntry> getSliceAsync(KVQuery query, StoreTransaction txh) throws BackendException {

        log.trace("db={}, op=getSliceAsync, tx={}", name, txh);

        final FoundationDBTx tx = getTransaction(txh);
        final StaticBuffer keyStart = query.getStart();
        final StaticBuffer keyEnd = query.getEnd();
        final byte[] foundKey = db.pack(keyStart.as(ENTRY_FACTORY));
        final byte[] endKey = db.pack(keyEnd.as(ENTRY_FACTORY));

        try {
            final AsyncIterator<KeyValue> result = tx.getRangeIter(foundKey, endKey, query.getLimit());

            return new FoundationDBRecordIteratorForAsync(tx, foundKey, endKey, query.getLimit(), result,
                    query.getKeySelector());
        } catch (Exception e) {
           log.error("getSliceAsync db=%s, tx=%s with exception", name, txh, e);

           throw new PermanentBackendException(e);
        }
    }

    private class FoundationDBRecordIterator implements RecordIterator<KeyValueEntry> {
        private final Iterator<KeyValueEntry> entries;

        public FoundationDBRecordIterator(final List<KeyValueEntry> result) {
              this.entries = result.iterator();
        }

        @Override
        public boolean hasNext() {
            return entries.hasNext();
        }

        @Override
        public KeyValueEntry next() {
            return entries.next();
        }

        @Override
        public void close() {
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private class FoundationDBRecordIteratorForAsync implements RecordIterator<KeyValueEntry> {
        private final FoundationDBTx tx;
        private AsyncIterator<KeyValue> entries;
        private final KeySelector selector;
        KeyValue nextKeyValue = null;

        private final byte[] startKey;
        private final byte[] endKey;
        private final int limit;
        private int fetched;

        protected static final int TRANSACTION_TOO_OLD_CODE = 1007;

        public FoundationDBRecordIteratorForAsync(FoundationDBTx tx,
                                                  byte[] startKey, byte[] endKey,
                                                  int limit, final AsyncIterator<KeyValue> result,
                                                  KeySelector selector) {
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
                StaticBuffer key = getBuffer(db.unpack(kv.getKey()).getBytes(0));
                return new KeyValueEntry(key, getBuffer(kv.getValue()));
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
                        StaticBuffer key = getBuffer(db.unpack(kv.getKey()).getBytes(0));
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


    @Override
    public Map<KVQuery,RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        if (manager.getMode() == FoundationDBStoreManager.NON_ASYNC) {
            return getSlicesNonAsync(queries, txh);
        } else {
            return getSlicesAsync(queries, txh);
        }
    }

    public Map<KVQuery,RecordIterator<KeyValueEntry>> getSlicesNonAsync (List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        log.trace("beginning db={}, op=getSlicesNonAsync, tx={}", name, txh);
        FoundationDBTx tx = getTransaction(txh);
        final Map<KVQuery, RecordIterator<KeyValueEntry>> resultMap = new HashMap<>();
        final int totalNumberOfQueries = queries.size();

        try {
            final List<Object[]> preppedQueries = new ArrayList<>(totalNumberOfQueries);
            for (final KVQuery query : queries) {
                final StaticBuffer keyStart = query.getStart();
                final StaticBuffer keyEnd = query.getEnd();
                final byte[] foundKey = db.pack(keyStart.as(ENTRY_FACTORY));
                final byte[] endKey = db.pack(keyEnd.as(ENTRY_FACTORY));
                preppedQueries.add(new Object[]{query, foundKey, endKey});
            }
            final Map<KVQuery, List<KeyValue>> result = tx.getMultiRange(preppedQueries);

            for (Map.Entry<KVQuery, List<KeyValue>> entry : result.entrySet()) {
                final List<KeyValueEntry> results = new ArrayList<>();
                for (final KeyValue keyValue : entry.getValue()) {
                final StaticBuffer key = getBuffer(db.unpack(keyValue.getKey()).getBytes(0));
                if (entry.getKey().getKeySelector().include(key))
                    results.add(new KeyValueEntry(key, getBuffer(keyValue.getValue())));
                }
                resultMap.put(entry.getKey(), new FoundationDBRecordIterator(results));

            }
        } catch (Exception e) {
            log.error("db={}, op=getSlicesNonAsync, tx={} throws exception", name, txh, e);

            if (e instanceof BackendException) {
                throw e;
            }
            else {
                throw new PermanentBackendException(e);
            }
        }

        return resultMap;
    }


    public Map<KVQuery,RecordIterator<KeyValueEntry>> getSlicesAsync(List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        //record the total number of sizes
        log.trace("beginning db={}, op=getSlicesAsync, tx={}", name, txh);

        FoundationDBTx tx = getTransaction(txh);
        final Map<KVQuery, RecordIterator<KeyValueEntry>> resultMap = new HashMap<>();

        try {
            for (final KVQuery query : queries) {
                final StaticBuffer keyStart = query.getStart();
                final StaticBuffer keyEnd = query.getEnd();
                final byte[] foundKey = db.pack(keyStart.as(ENTRY_FACTORY));
                final byte[] endKey = db.pack(keyEnd.as(ENTRY_FACTORY));

                AsyncIterator<KeyValue> result = tx.getRangeIter(foundKey, endKey, query.getLimit());
                resultMap.put(query, new FoundationDBRecordIteratorForAsync(tx, foundKey, endKey,
                        query.getLimit(), result, query.getKeySelector()));
            }
        } catch (Exception e) {
            log.error("db={}, getSlicesAsync, tx= {} with exception", name, txh, e);

            throw new PermanentBackendException(e);
        }


        return resultMap;
    }

    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh) throws BackendException {
        FoundationDBTx tx = getTransaction(txh);
        try {

            log.trace("db={}, op=insert, tx={}", name, txh);
            tx.set(db.pack(key.as(ENTRY_FACTORY)), value.as(ENTRY_FACTORY));
        } catch (Exception e) {
            log.error ("db={}, op=insert, tx={} with exception", name, txh, e);
            throw new PermanentBackendException(e);
        }
    }


    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        FoundationDBTx tx = getTransaction(txh);
        try {
            log.trace("db={}, op=delete, tx={}", name, txh);
            tx.clear(db.pack(key.as(ENTRY_FACTORY)));
        } catch (Exception e) {
            log.error("db={}, op=delete, tx={} with exception", name, txh, e);
            throw new PermanentBackendException(e);
        }
    }

    private static StaticBuffer getBuffer(byte[] entry) {
        return new StaticArrayBuffer(entry);
    }
}
