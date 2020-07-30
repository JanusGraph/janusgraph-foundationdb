package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.subspace.Subspace;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.util.RecordIterator;

import java.util.Iterator;
import java.util.List;

public class FoundationDBRecordIterator implements RecordIterator<KeyValueEntry> {
    private final Subspace ds;
    private final Iterator<KeyValue> entries;

    public FoundationDBRecordIterator(Subspace ds, final List<KeyValue> result) {
        this.ds = ds;
        this.entries = result.iterator();
    }

    @Override
    public boolean hasNext() {
        return entries.hasNext();
    }

    @Override
    public KeyValueEntry next() {
        KeyValue nextKV = entries.next();
        StaticBuffer key = FoundationDBKeyValueStore.getBuffer(ds.unpack(nextKV.getKey()).getBytes(0));
        StaticBuffer value = FoundationDBKeyValueStore.getBuffer(nextKV.getValue());
        return new KeyValueEntry(key, value);
    }

    @Override
    public void close() {
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
