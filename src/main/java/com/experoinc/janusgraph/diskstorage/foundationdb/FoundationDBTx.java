package com.experoinc.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author twilmes
 */
public class FoundationDBTx extends AbstractStoreTransaction {


    private static final Logger log = LoggerFactory.getLogger(SerializableFoundationDBTx.class);

    private volatile Transaction tx;

    private final Database db;

    private List<Mutation> inserts = new LinkedList<>();
    private List<byte[]> deletions = new LinkedList<>();

    private final AtomicInteger txCount = new AtomicInteger(0);

    public FoundationDBTx(Database db, Transaction t, BaseTransactionConfig config) {
        super(config);
        tx = t;
        this.db = db;
    }

    public Transaction getTransaction() {
        return tx;
    }

    public synchronized  void restart() throws PermanentBackendException {
        if (tx == null) return;
        tx.close();
        log.info("Restarting transaction");
        tx = db.createTransaction();
        txCount.incrementAndGet();
        // reapply mutations but do not clear them out just in case this transaction also
        // times out and they need to be reapplied
        for (Mutation mutation : inserts) {
            tx.set(mutation.getKey(), mutation.getValue());
        }
        for (byte[] delete : deletions) {
            tx.clear(delete);
        }
    }

    @Override
    public synchronized void rollback() throws BackendException {
        super.rollback();
        if (tx == null) return;
        if (log.isTraceEnabled())
            log.trace("{} rolled back", this.toString(), new FoundationDBTx.TransactionClose(this.toString()));
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
            log.trace("{} committed", this.toString(), new FoundationDBTx.TransactionClose(this.toString()));

        try {
            tx.commit().get();
            tx.close();
            tx = null;
        } catch (ExecutionException e) {
            restart();
            commit();
        }catch (Exception e) {
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

    public byte[] get(final byte[] key) throws PermanentBackendException {
        try {
            return this.tx.get(key).get();
        } catch (ExecutionException e) {
            this.restart();
            return get(key);
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }
    }

    public List<KeyValue> getRange(final byte[] startKey, final byte[] endKey,
                                            final int limit) throws PermanentBackendException {
        try {
            return tx.getRange(new Range(startKey, endKey), limit).asList().get();
        } catch (ExecutionException e) {
            this.restart();
            return getRange(startKey, endKey, limit);
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }
    }

    public Map<KVQuery, List<KeyValue>> getMultiRange(List<Object[]> queries) throws PermanentBackendException {
        Map<KVQuery, List<KeyValue>> resultMap = new ConcurrentHashMap<>();
        List<Object[]> retries = new LinkedList<>();
        for(Object[] obj : queries) {
            final KVQuery query = (KVQuery) obj[0];
            final byte[] start = (byte[]) obj[1];
            final byte[] end = (byte[]) obj[2];
            try {
                final List<KeyValue> result = tx.getRange(start, end, query.getLimit()).asList()
                        .whenComplete((res, th) -> {
                            if (th != null) {
                                retries.add(new Object[]{query, start, end});
                            } else {
                                resultMap.put(query, res);
                            }
                        }).join();
                resultMap.put(query, result);
            } catch (Exception e) {
                throw new PermanentBackendException(e);
            }
            if (!retries.isEmpty()) {
                final Map<KVQuery, List<KeyValue>> retryResults = getMultiRange(retries);
                retryResults.entrySet().forEach(entry -> resultMap.put(entry.getKey(), entry.getValue()));
            }
        }
        return resultMap;
    }

    public void set(final byte[] key, final byte[] value) throws PermanentBackendException {
        inserts.add(new Mutation(key, value));
        tx.set(key, value);
    }

    public void clear(final byte[] key) throws PermanentBackendException {
        deletions.add(key);
        tx.clear(key);
    }


    private class Mutation {
        private byte[] key;
        private byte[] value;

        public Mutation(final byte[] key, final byte[] value) {
            this.key = key;
            this.value = value;
        }

        public byte[] getKey() { return this.key; }

        public byte[] getValue() { return this.value; }
    }
}
