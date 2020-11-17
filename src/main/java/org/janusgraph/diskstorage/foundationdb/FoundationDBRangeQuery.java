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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.subspace.Subspace;
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