package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.subspace.Subspace;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;

/**
 * @author Florian Grieskamp
 */
public class FoundationDBRangeQuery {

    private KVQuery originalQuery;
    private KeySelector startKeySelector;
    private KeySelector endKeySelector;
    private int limit;

    public FoundationDBRangeQuery(Subspace db, KVQuery kvQuery) {
        originalQuery = kvQuery;
        limit = kvQuery.getLimit();

        byte[] startKey = db.pack(kvQuery.getStart().as(FoundationDBKeyValueStore.ENTRY_FACTORY));
        byte[] endKey = db.pack(kvQuery.getEnd().as(FoundationDBKeyValueStore.ENTRY_FACTORY));

        startKeySelector = KeySelector.firstGreaterOrEqual(startKey);
        endKeySelector = KeySelector.firstGreaterOrEqual(endKey);
    }

    public void setStartKeySelector(KeySelector startKeySelector) {
        this.startKeySelector = startKeySelector;
    }

    public void setEndKeySelector(KeySelector endKeySelector) {
        this.endKeySelector = endKeySelector;
    }

    public KVQuery asKVQuery() { return originalQuery; }

    public KeySelector getStartKeySelector() { return startKeySelector; }

    public KeySelector getEndKeySelector() { return endKeySelector; }

    public int getLimit() { return limit; }
}