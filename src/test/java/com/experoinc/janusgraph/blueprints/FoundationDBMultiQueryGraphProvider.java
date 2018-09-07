package com.experoinc.janusgraph.blueprints;

import com.experoinc.janusgraph.FoundationDBStorageSetup;
import org.janusgraph.blueprints.AbstractJanusGraphProvider;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.USE_MULTIQUERY;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class FoundationDBMultiQueryGraphProvider extends AbstractJanusGraphProvider {

    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test, String testMethodName) {
        return FoundationDBStorageSetup.getFoundationDBConfiguration()
            .set(USE_MULTIQUERY, true);
    }
}
