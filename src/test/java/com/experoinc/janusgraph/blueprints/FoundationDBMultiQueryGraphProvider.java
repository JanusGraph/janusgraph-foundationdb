package com.experoinc.janusgraph.blueprints;

import com.experoinc.janusgraph.FoundationDBContainer;
import org.janusgraph.blueprints.AbstractJanusGraphProvider;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.USE_MULTIQUERY;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class FoundationDBMultiQueryGraphProvider extends AbstractJanusGraphProvider {

    private final static FoundationDBContainer container = new FoundationDBContainer();

    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test, String testMethodName) {
        container.start();
        return container.getFoundationDBConfiguration()
            .set(USE_MULTIQUERY, true);
    }
}
