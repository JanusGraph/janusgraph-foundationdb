package com.experoinc.janusgraph.blueprints.process;

import com.experoinc.janusgraph.blueprints.FoundationDBMultiQueryGraphProvider;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.janusgraph.core.JanusGraph;
import org.junit.runner.RunWith;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
@RunWith(FoundationDBProcessStandardSuite.class)
@GraphProviderClass(provider = FoundationDBMultiQueryGraphProvider.class, graph = JanusGraph.class)
public class FoundationDBJanusGraphMultiQueryProcessTest {
}