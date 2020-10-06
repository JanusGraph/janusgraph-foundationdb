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

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class FoundationDBKeyValueStore implements OrderedKeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBKeyValueStore.class);

    static final StaticBuffer.Factory<byte[]> ENTRY_FACTORY = (array, offset, limit) -> {
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

        try {
            final List<KeyValue> result = tx.getRange(new FoundationDBRangeQuery(db, query));
            if (result != null) {

                // only take KV pairs that match the KeySelector rules
                result.removeIf(kv -> {
                    StaticBuffer key = getBuffer(db.unpack(kv.getKey()).getBytes(0));
                    return !query.getKeySelector().include(key);
                });
            }

            log.trace("db={}, op=getSliceNonAsync, tx={}, result-count={}", name, txh, result.size());

            return new FoundationDBRecordIterator(db, result);

        } catch (Exception e) {
            log.error("db={}, op=getSliceNonAsync, tx={} with exception", name, txh, e);

            if (e instanceof BackendException) {
                throw e;
            }
            else {
                throw new PermanentBackendException(e);
            }
        }
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

            return new FoundationDBRecordIteratorForAsync(
                    db, tx, foundKey, endKey, query.getLimit(), result,
                    query.getKeySelector());
        } catch (Exception e) {
           log.error("getSliceAsync db=%s, tx=%s with exception", name, txh, e);

           throw new PermanentBackendException(e);
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
        log.trace("beginning db={}, op=getSlice, tx={}", name, txh);
        FoundationDBTx tx = getTransaction(txh);
        final Map<KVQuery, List<KeyValue>> resultMap = new ConcurrentHashMap<>();
        final Map<KVQuery, FoundationDBRangeQuery> fdbQueries = new ConcurrentHashMap<>();

        for (final KVQuery q : queries) {
            resultMap.put(q, new ArrayList<>());
            fdbQueries.put(q, new FoundationDBRangeQuery(db, q));
        }

        final Map<KVQuery, List<KeyValue>> unfilteredResultMap = tx.getMultiRange(fdbQueries.values());

        for (Map.Entry<KVQuery, List<KeyValue>> currentUnfilteredResultMap : unfilteredResultMap.entrySet()) {
            KVQuery currentQuery = currentUnfilteredResultMap.getKey();
            List<KeyValue> currentUnfilteredResult = currentUnfilteredResultMap.getValue();

            if (currentUnfilteredResult != null) {
                // only take KV pairs that match the KeySelector rules
                for (final KeyValue kv : currentUnfilteredResult) {
                    StaticBuffer key = getBuffer(db.unpack(kv.getKey()).getBytes(0));
                    if (currentQuery.getKeySelector().include(key)) {
                        resultMap.get(currentQuery).add(kv);
                    }
                }
            }
        }

        final Map<KVQuery, RecordIterator<KeyValueEntry>> iteratorMap = new HashMap<>();
        for (Map.Entry<KVQuery, List<KeyValue>> kv : resultMap.entrySet()) {
            iteratorMap.put(kv.getKey(), new FoundationDBRecordIterator(db, kv.getValue()));
        }

        return iteratorMap;
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
                resultMap.put(query, new FoundationDBRecordIteratorForAsync(
                        db, tx, foundKey, endKey,
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

    static StaticBuffer getBuffer(byte[] entry) {
        return new StaticArrayBuffer(entry);
    }
}
