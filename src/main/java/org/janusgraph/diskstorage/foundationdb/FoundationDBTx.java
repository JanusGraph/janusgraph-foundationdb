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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.async.AsyncIterator;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class FoundationDBTx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBTx.class);

    private volatile Transaction tx;

    private final Database db;

    private List<Insert> inserts = Collections.synchronizedList(new ArrayList<>());
    private List<byte[]> deletions = Collections.synchronizedList(new ArrayList<>());

    private int maxRuns = 1;

    public enum IsolationLevel { SERIALIZABLE, READ_COMMITTED_NO_WRITE, READ_COMMITTED_WITH_WRITE }

    private final IsolationLevel isolationLevel;

    private AtomicInteger txCtr = new AtomicInteger(0);

    private static AtomicInteger transLocalIdCounter = new AtomicInteger(0);
    private int transactionId = 0;

    public FoundationDBTx(Database db, Transaction t, BaseTransactionConfig config, IsolationLevel isolationLevel) {
        super(config);
        tx = t;
        this.db = db;
        this.isolationLevel = isolationLevel;

        switch (isolationLevel) {
            case SERIALIZABLE:
                // no retries
                break;
            case READ_COMMITTED_NO_WRITE:
            case READ_COMMITTED_WITH_WRITE:
                maxRuns = 3;
        }

        this.transactionId = transLocalIdCounter.incrementAndGet();
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public synchronized void restart() {
        txCtr.incrementAndGet();

        if (tx != null) {
            try {
                tx.cancel();
            } catch (IllegalStateException e) {
                //
            } finally {
                try {
                    tx.close();
                } catch (Exception e) {
                    log.error("Exception when closing transaction: ", e);
                }
            }
        }
        else {
            log.warn("In execution mode: {} and when restart transaction, encountered FDB transaction object being null",
                        isolationLevel.name());
        }

        tx = db.createTransaction();
        // Reapply mutations but do not clear them out just in case this transaction also
        // times out and they need to be reapplied.
        //
        // @todo Note that at this point, the large transaction case (tx exceeds 10,000,000 bytes) is not handled.
        inserts.forEach(insert -> tx.set(insert.getKey(), insert.getValue()));
        deletions.forEach(delete -> tx.clear(delete));
    }

    @Override
    public synchronized void rollback() throws BackendException {
        super.rollback();
        if (tx == null) {
            log.warn("In execution mode: {} and when rollback, encountered FDB transaction object being null", isolationLevel.name());
            return;
        }
        if (log.isTraceEnabled())
            log.trace("{} rolled back", this.toString(), new FoundationDBTx.TransactionClose(this.toString()));

        try {
            tx.cancel();
            tx.close();
            tx = null;
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        } finally {
            if (tx != null) {
                try {
                    tx.close();
                } catch (Exception e) {
                    log.error("Exception when closing transaction:", e);
                }
               tx = null;
            }
        }

        log.debug("Transaction rolled back, num_inserts: {}, num_deletes: {}", inserts.size(), deletions.size());
    }


    private void logFDBException(Throwable t) {
        if (t != null && t instanceof FDBException) {
            FDBException fe = (FDBException) t;
            if (log.isDebugEnabled()) {
                log.debug("Catch FDBException code= {}, isRetryable={}, isMaybeCommitted={}, "
                                + "isRetryableNotCommitted={}, isSuccess={}",
                        fe.getCode(), fe.isRetryable(),
                        fe.isMaybeCommitted(), fe.isRetryableNotCommitted(), fe.isSuccess());
            }
        }
    }

    @Override
    public synchronized void commit() throws BackendException {
        boolean failing = true;
        int counter = 0;

        for (int i = 0; i < maxRuns; i++) {
            super.commit();
            if (tx == null) {
                log.warn("In execution mode: {} and when commit, encountered FDB transaction object being null", isolationLevel.name());
                return;
            }
            if (log.isTraceEnabled())
                log.trace("{} committed", this.toString(), new FoundationDBTx.TransactionClose(this.toString()));

            try {
                if (!inserts.isEmpty() || !deletions.isEmpty()) {
                    tx.commit().get();
                    log.debug("Transaction committed, num_inserts: {}, num_deletes: {}", inserts.size(), deletions.size());
                } else {
                    // nothing to commit so skip it
                    tx.cancel();
                }
                tx.close();
                tx = null;
                failing = false;
                break;
            } catch (IllegalStateException | ExecutionException e) {
                if (tx != null) {
                    try {
                        tx.close();
                    } catch (Exception ex) {
                        log.error( "Exception when closing transaction: {} ", ex.getMessage());
                    }
                    tx = null;
                }

                log.error ("Commit encountered exception: {}, i: {}, maxRuns: {}, to be restarted with inserts: {} and deletes: {}",
                           e.getMessage(), i, maxRuns, inserts.size(), deletions.size());

                logFDBException(e.getCause());
                if (isolationLevel.equals(IsolationLevel.SERIALIZABLE) ||
                        isolationLevel.equals(IsolationLevel.READ_COMMITTED_NO_WRITE)) {
                    log.error ("Commit failed with inserts: {}, deletes: {}", inserts.size(), deletions.size());

                    // throw the exception that carries the root exception cause
                    throw new PermanentBackendException("transaction fails to commit", e);
                }

                if (i+1 != maxRuns) {
                    restart();
                    counter++; // increase how many times that the commit has been re-started.
                }

            } catch (Exception e) {

                if (tx != null) {
                    try {
                        tx.close();
                    } catch (Exception ex) {
                        log.error ("Exception when closing transaction: {}", ex.getMessage());
                    }
                    tx = null;
                }

                log.error("Commit encountered exception: {} ", e.getMessage());
                throw new PermanentBackendException(e);
            }
        }

        if (failing) {
            log.error("Commit failed with maxRuns: {}, inserts: {}, deletes: {}, total re-starts: {}",
                      maxRuns, inserts.size(), deletions.size(), counter);

            // Note: even if the commit is retriable and the TemporaryBackendException is thrown here, at the commit(.)
            // method of StandardJanusGraph class, the thrown exception will be translated to rollback(.) and then
            // throw further JanusGraphException to the application. Thus, it is better to just throw the
            // PermanentBackendException here. as at this late commit stage, there is no retry logic defined
            // at the StandardJanusGraph class.
            // if (isRetriable)
            //    throw new TemporaryBackendException("Max transaction count exceed but transaction is retriable");
            // else
            //    throw new PermanentBackendException("Max transaction reset count exceeded");
            throw new PermanentBackendException("transaction fails to commit with max transaction reset count exceeded");
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
        for (int i = 0; i < maxRuns; i++) {
            try {
                return this.tx.get(key).get();
            } catch (ExecutionException e) {
                log.error("get encountered ExecutionException: {}, with number of retries: {}", e.getMessage(), i);

                logFDBException(e.getCause());
                if (i+1 != maxRuns) {
                    this.restart();
                } else {
                    throw new PermanentBackendException ("Max transaction reset count exceeded with final exception", e);
                }
            } catch (Exception e) {
                log.error( "get encountered other exception: {}, with number of retries: {} ", e.getMessage(), i);

                throw new PermanentBackendException(e);
            }
        }

        throw new PermanentBackendException ("Max transaction reset count exceeded");
    }


    public List<KeyValue> getRange(final FoundationDBRangeQuery query) throws PermanentBackendException {
        for (int i = 0; i < maxRuns; i++) {
            final int startTxId = txCtr.get();
            try {
                List<KeyValue> result =
                        tx.getRange(query.getStartKeySelector(), query.getEndKeySelector(), query.getLimit()).asList().get();
                return result != null ? result : Collections.emptyList();
            } catch (ExecutionException e) {
                log.error("getRange encountered ExecutionException: {}, with number of retries: {}", e.getMessage(), i);

                logFDBException(e.getCause());

                if (txCtr.get() == startTxId) {
                    log.debug("getRange tx-counter: {} and start tx-id: {} agree with each other, with exception: {} before restart",
                                 txCtr.get(), startTxId, e.getMessage());

                    if (i+1 != maxRuns) {
                        this.restart();
                        log.debug("Transaction restarted with iteration: {} and maxRuns: {} ", i, maxRuns);
                    } else {
                        throw new PermanentBackendException("Max transaction reset count exceeded with final exception", e);
                    }
                }
                else {
                   log.debug("getRange tx-counter: {} and start tx-id: {} not agree with each other", txCtr.get(), startTxId);
                }
            } catch (Exception e) {
                log.error("getRange encountered other exception: {}, with number of retries: {}", e.getMessage(), i);

                throw new PermanentBackendException(e);
            }
        }

        throw new PermanentBackendException ("Max transaction reset count exceeded");

    }

    public AsyncIterator<KeyValue> getRangeIter(FoundationDBRangeQuery query) {
        final int limit = query.asKVQuery().getLimit();
        return tx.getRange(query.getStartKeySelector(), query.getEndKeySelector(), limit,
                false, StreamingMode.WANT_ALL).iterator();
    }

    public AsyncIterator<KeyValue> getRangeIter(FoundationDBRangeQuery query, final int skip) {
        // Avoid using KeySelector(byte[] key, boolean orEqual, int offset) directly as stated in KeySelector.java
        // that client code will not generally call this constructor.
        final int limit = query.asKVQuery().getLimit();
        KeySelector begin = query.getStartKeySelector().add(skip);
        KeySelector end = query.getEndKeySelector();
        return tx.getRange(begin, end, limit-skip, false, StreamingMode.WANT_ALL).iterator();
    }

    public synchronized Map<KVQuery, List<KeyValue>> getMultiRange(final Collection<FoundationDBRangeQuery> queries)
            throws PermanentBackendException {
        Map<KVQuery, List<KeyValue>> resultMap = new ConcurrentHashMap<>();
        final List<FoundationDBRangeQuery> retries = new CopyOnWriteArrayList<>(queries);

        int counter = 0;
        for (int i = 0; i < maxRuns; i++) {
            counter++;

            if (retries.size() > 0) {

                List<FoundationDBRangeQuery> immutableRetries = new ArrayList<> (retries);

                final List<CompletableFuture<List<KeyValue>>> futures = new LinkedList<>();

                final int startTxId = txCtr.get();

                // Introduce the immutable list for iteration purpose, rather than having the dynamic list
                // retries to be the iterator.
                for (FoundationDBRangeQuery q : immutableRetries) {
                    final KVQuery query = q.asKVQuery();

                    CompletableFuture<List<KeyValue>> f = tx.getRange(q.getStartKeySelector(),
                            q.getEndKeySelector(), query.getLimit()).asList()
                            .whenComplete((res, th) -> {
                                if (th == null) {
                                    log.debug("(before) get range succeeded with current size of retries: {}, thread id: {}, tx id: {}",
                                              retries.size(), Thread.currentThread().getId(), transactionId);

                                    // retries's object type is: Object[], not KVQuery.
                                    retries.remove(q);

                                    log.debug("(after) get range succeeded with current size of retries: {}, thread id: {}, tx id: {}",
                                            retries.size(), Thread.currentThread().getId(), transactionId);

                                    if (res == null) {
                                        res = Collections.emptyList();
                                    }
                                    resultMap.put(query, res);

                                } else {
                                    logFDBException(th.getCause());

                                    // The restart here will bring the code into deadlock, as restart() is a
                                    // synchronized method and the thread to invoke this method is from a worker thread
                                    // that serves the completable future call, which is different from the thread that
                                    // invokes the getMultiRange call (this method) and getMultiRange is also a synchronized
                                    // call.
                                    // if (startTxId == txCtr.get())
                                    //    this.restart();
                                    resultMap.put(query, Collections.emptyList());

                                    log.debug("Encountered exception with: {}", th.getCause().getMessage());
                                }
                            });

                    futures.add(f);
                }

                CompletableFuture<Void> allFuturesDone = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
                // When some of the Future encounters exception, map will ignore it. we need count as the action!
                // allFuturesDone.thenApply(v -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
                try {
                    allFuturesDone.join();
                } catch (Exception ex) {
                    log.error("Multi-range query encountered transient exception in some futures: {}", ex.getCause().getMessage());
                }

                log.debug("get range succeeded with current size of retries: {}, thread id: {}, tx id: {}",
                           retries.size(), Thread.currentThread().getId(), transactionId);

                if (retries.size() > 0) {
                    log.debug("In multi-range query, retries size: {}, thread id: {}, tx id: {}, start tx id: {}, txCtr id: {}",
                              retries.size(), Thread.currentThread().getId(), transactionId, startTxId, txCtr.get());

                    if (startTxId == txCtr.get()) {
                        log.debug("In multi-range query, to restart with thread id: {}, tx id: {}",
                                  Thread.currentThread().getId(), transactionId);

                        if (i+1 != maxRuns) {
                            this.restart();
                        }
                    }
                }
            } else {
                log.debug("Finish multi-range query's all of future-based invocations with size: {}, thread id: {}, tx id: {}, number of retries:{}",
                           queries.size(), Thread.currentThread().getId(), transactionId, (counter - 1));
                break;
            }

        }

        if (retries.size() > 0) {
            log.error("After max number of retries: {}, some range queries still failed and forced with empty returns with transaction id: {}",
                      maxRuns, transactionId);
            throw new PermanentBackendException("Encounter exceptions when invoking getRange(.) calls in getMultiRange(.)");
        }

        return resultMap;

    }

    public void set(final byte[] key, final byte[] value) {
        inserts.add(new Insert(key, value));
        tx.set(key, value);
    }

    public void clear(final byte[] key) {
        deletions.add(key);
        tx.clear(key);
    }


    private class Insert {
        private byte[] key;
        private byte[] value;

        public Insert(final byte[] key, final byte[] value) {
            this.key = key;
            this.value = value;
        }

        public byte[] getKey() { return this.key; }

        public byte[] getValue() { return this.value; }
    }
}
