package com.tinkerpop.graph.benchmark.neo4j.traverser;

import com.tinkerpop.graph.benchmark.GraphTraverserService;
import com.tinkerpop.graph.benchmark.neo4j.NeoRelation;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.IteratorUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Sergey Sitnikov
 */
public class Neo4jTraverserImpl implements GraphTraverserService {
  private String neoDbDirName;

  private GraphDatabaseService graph;
  private long                 pageCount;

  public void init() {
    if (!new File(getNeoDbDirName()).exists()) {
      graph = null;
      return;
    }

    graph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(neoDbDirName)).setConfig(new HashMap<String, String>() {{
      put(GraphDatabaseSettings.pagecache_memory.name(), "2G");
      put(GraphDatabaseSettings.dump_configuration.name(), "true");
    }}).newGraphDatabase();

    try (Transaction tx = graph.beginTx()) {
      pageCount = IteratorUtil.count(graph.findNodes(DynamicLabel.label("Page")));
      tx.success();
    }
  }

  @Override
  public boolean databaseExists() {
    return graph != null;
  }

  @Override
  public long numberOfVerities() {
    return pageCount;
  }

  @Override
  public String getVertex(long serial) {
    try (Transaction tx = graph.beginTx()) {
      final String vertex = (String) graph.findNode(DynamicLabel.label("Page"), "serial", serial).getProperty("udk");
      tx.success();
      return vertex;
    }
  }

  @Override
  public String[] shortestPath(String from, String to) {
    try (Transaction tx = graph.beginTx()) {
      final Node fromNode = graph.findNode(DynamicLabel.label("Page"), "udk", from);
      final Node toNode = graph.findNode(DynamicLabel.label("Page"), "udk", to);

      final PathFinder<Path> finder = GraphAlgoFactory
          .shortestPath(PathExpanders.forTypeAndDirection(NeoRelation.LINKS_TO, Direction.BOTH), Integer.MAX_VALUE);
      final Path path = finder.findSinglePath(fromNode, toNode);

      final ArrayList<Node> nodes = new ArrayList<>();
      if (path != null)
        IteratorUtil.addToCollection(path.nodes(), nodes);

      final String[] result = nodes.stream().map(node -> (String) node.getProperty("udk")).toArray(String[]::new);

      tx.success();
      return result;
    }
  }

  @Override
  public void traverse(String[] path) {
    try (Transaction tx = graph.beginTx()) {
      for (String s : path) {
        final Node node = graph.findNode(DynamicLabel.label("Page"), "udk", s);
        for (Relationship r : node.getRelationships(NeoRelation.LINKS_TO, Direction.BOTH)) {
          if (r.getOtherNode(node).getProperty("udk") == null)
            System.out.println("should never happen ;)");
        }
      }
      tx.success();
    }
  }

  @Override
  public void close() {
    graph.shutdown();
  }

  public String getNeoDbDirName() {
    return neoDbDirName;
  }

  public void setNeoDbDirName(String neoDbDirName) {
    this.neoDbDirName = neoDbDirName;
  }
}
