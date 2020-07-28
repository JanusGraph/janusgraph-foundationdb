package org.janusgraph.blueprints.process;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.janusgraph.core.JanusGraph;
import org.junit.runner.RunWith;

import org.janusgraph.FoundationDBContainer;
import org.janusgraph.blueprints.FoundationDBMultiQueryGraphProvider;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
@RunWith(FoundationDBProcessStandardSuite.class)
@GraphProviderClass(provider = FoundationDBMultiQueryGraphProvider.class, graph = JanusGraph.class)
public class FoundationDBJanusGraphMultiQueryProcessTest {
}