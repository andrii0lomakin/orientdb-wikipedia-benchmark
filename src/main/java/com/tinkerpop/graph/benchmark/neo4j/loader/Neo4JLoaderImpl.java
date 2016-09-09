package com.tinkerpop.graph.benchmark.neo4j.loader;

import com.tinkerpop.graph.benchmark.GraphLoaderService;
import com.tinkerpop.graph.benchmark.neo4j.NeoRelation;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class Neo4JLoaderImpl implements GraphLoaderService {

  private GraphDatabaseService db;

  private String neoDbDirName;
  private long serial = 0;

  public void init() throws IOException {
    deleteDirectory(new File(neoDbDirName));

    db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(neoDbDirName)).setConfig(new HashMap<String, String>() {{
      put(GraphDatabaseSettings.pagecache_memory.name(), "2G");
      put(GraphDatabaseSettings.dump_configuration.name(), "true");
    }}).newGraphDatabase();

    try (Transaction tx = db.beginTx()) {
      db.schema().constraintFor(DynamicLabel.label(("Page"))).assertPropertyIsUnique("serial").create(); // also creates the index
      db.schema().constraintFor(DynamicLabel.label(("Page"))).assertPropertyIsUnique("udk").create(); // also creates the index
      tx.success();
    }

    try (Transaction tx = db.beginTx()) {
      db.schema().awaitIndexesOnline(1, TimeUnit.MINUTES);
      tx.success();
    }
  }

  static public boolean deleteDirectory(File path) {
    if (path.exists()) {
      File[] files = path.listFiles();
      for (int i = 0; i < files.length; i++) {
        if (files[i].isDirectory()) {
          deleteDirectory(files[i]);
        } else {
          files[i].delete();
        }
      }
    }
    return (path.delete());
  }

  private long        insertsCount = 0;
  private Transaction tx           = null;

  @Override
  public void addLink(String fromNodeKey, String toNodeKey) {
    if (insertsCount % 1000 == 0) {
      if (tx != null) {
        tx.success();
        tx.close();
      }
      tx = db.beginTx();
    }

    //try (Transaction tx = db.beginTx()) {
    Node fromNode;
    //it is likely that fromNodeKey is the same as the last call because of the way the Wikipedia content is organised
    if (fromNodeKey.equals(lastFromNodeKey)) {
      fromNode = lastFromNode;
    } else {
      //See if node exists using index
      fromNode = getGraphNode(fromNodeKey);
      if (fromNode == null)
        fromNode = createNode(fromNodeKey);
      lastFromNode = fromNode;
      lastFromNodeKey = fromNodeKey;
    }

    Node toNode;
    if (toNodeKey.equals(lastToNodeKey)) {
      toNode = lastToNode;
    } else {
      toNode = getGraphNode(toNodeKey);
      if (toNode == null)
        toNode = createNode(toNodeKey);
      lastToNodeKey = toNodeKey;
      lastToNode = toNode;
    }

    fromNode.createRelationshipTo(toNode, NeoRelation.LINKS_TO);
    ++insertsCount;

    //      tx.success();
    //    }
  }

  private Node getGraphNode(String fromNodeKey) {
    return db.findNode(DynamicLabel.label("Page"), "udk", fromNodeKey);
  }

  private Node createNode(String nodeKey) {
    final Node node = db.createNode(DynamicLabel.label("Page"));
    node.setProperty("serial", serial++);
    node.setProperty("udk", nodeKey);
    return node;
  }

  //Cached lookup from last call
  private String lastFromNodeKey;
  private Node   lastFromNode;
  private String lastToNodeKey;
  private Node   lastToNode;

  @Override
  public void close() {
    if (tx != null) {
      tx.success();
      tx.close();
    }
    db.shutdown();
  }

  public String getNeoDbDirName() {
    return neoDbDirName;
  }

  public void setNeoDbDirName(String neoDbDirName) {
    this.neoDbDirName = neoDbDirName;
  }
}