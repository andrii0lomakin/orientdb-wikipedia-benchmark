package com.tinkerpop.graph.benchmark.orientdb.loader;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.graph.benchmark.GraphLoaderService;

import java.io.File;

public class OrientDbLoaderImpl implements GraphLoaderService {
  private String          orientDbDirName;
  private String          orientDbName;
  private OrientBaseGraph graph;
  private OrientIndex     index;
  private long serial = 0;

  public OrientDbLoaderImpl() {
  }

  public void init() {
    File dir = new File(getOrientDbDirName());
    deleteDirectory(dir);
    dir.mkdirs();

    graph = new OrientGraph("plocal:" + getOrientDbDirName(), false);
    graph.drop();

    graph = new OrientGraph("plocal:" + getOrientDbDirName(), false);
    graph.setUseLightweightEdges(true);
    graph.createVertexType("Page");

    index = new OrientIndex(graph);
  }

  private long insertsCount = 0;

  @Override
  public void addLink(String fromNodeKey, String toNodeKey) {
    if (insertsCount % 1000 == 0) {
      graph.commit();
      graph.begin();
    }

    //		graph.begin();
    //		try {
    OrientVertex fromNode = null;
    // it is likely that fromNodeKey is the same as the last call because of the way the Wikipedia content is organised
    boolean fromNodeInsertIntoIndex = false;
    if (fromNodeKey.equals(lastFromNodeKey)) {
      fromNode = lastFromNode;
    } else {
      // See if node exists using index
      fromNode = getVertexFromIndex(fromNodeKey);
      if (fromNode == null) {
        // New vertex - add to graph and index
        fromNode = createVertex(fromNodeKey);
        fromNodeInsertIntoIndex = true;
      }
      lastFromNode = fromNode;
      lastFromNodeKey = fromNodeKey;
    }
    OrientVertex toNode = null;
    boolean toNodeInsertIntoIndex = false;
    // it is likely that toNodeKey is the same as the last call because of the way the Wikipedia content is organised
    if (toNodeKey.equals(lastToNodeKey)) {
      toNode = lastToNode;
    } else {
      // See if node exists using index
      toNode = getVertexFromIndex(toNodeKey);
      if (toNode == null) {
        // New vertex - add to graph and index
        toNode = createVertex(toNodeKey);
        toNodeInsertIntoIndex = true;
      }
      lastToNode = toNode;
      lastToNodeKey = toNodeKey;
    }
    // Create the edge
    graph.addEdge(null, fromNode, toNode, "contains");
    ++insertsCount;

    if (fromNodeInsertIntoIndex) {
      index.storeVertex(fromNodeKey, fromNode);
    }
    if (toNodeInsertIntoIndex) {
      index.storeVertex(toNodeKey, toNode);
    }

    //			graph.commit();
    //		}
    //		catch (Throwable t) {
    //			graph.rollback();
    //			throw t;
    //		}
  }

  private OrientVertex createVertex(String fromNodeKey) {
    final OrientVertex vertex = graph.addVertex("class:Page");
    vertex.setProperty("serial", serial++);
    vertex.setProperty("udk", fromNodeKey);
    return vertex;
  }

  private OrientVertex getVertexFromIndex(final String fromNodeKey) {
    final ODocument v = index.getVertex(fromNodeKey);
    if (v == null)
      return null;

    return graph.getVertex(v);
  }

  static class OrientIndex {
    OrientBaseGraph graph;
    private final OIndex<OIdentifiable> udkIndex;
    private final OIndex<OIdentifiable> serialIndex;

    public OrientIndex(OrientBaseGraph graph) {
      this.graph = graph;
      graph.getRawGraph().commit();
      udkIndex = (OIndex<OIdentifiable>) graph.getRawGraph().getMetadata().getIndexManager()
          .createIndex("Page.udk", "UNIQUE", new OSimpleKeyIndexDefinition(-1, OType.STRING), null, null, null);
      serialIndex = (OIndex<OIdentifiable>) graph.getRawGraph().getMetadata().getIndexManager()
          .createIndex("Page.serial", "UNIQUE", new OSimpleKeyIndexDefinition(-1, OType.LONG), null, null, null);
      graph.commit();
    }

    public void storeVertex(String udk, OrientVertex vertex) {
      udkIndex.put(udk, vertex.getIdentity());
      serialIndex.put(vertex.getProperty("serial"), vertex.getIdentity());
    }

    public ODocument getVertex(String udk) {
      final OIdentifiable result = udkIndex.get(udk);
      if (result != null) {
        return (ODocument) result.getRecord();
      }
      return null;
    }
  }

  //Cached lookup from last call
  private String       lastFromNodeKey;
  private OrientVertex lastFromNode;
  private String       lastToNodeKey;
  private OrientVertex lastToNode;

  @Override
  public void close() {
    graph.commit();
    graph.shutdown();
  }

  public void setOrientDbDirName(String orientDbDirName) {
    this.orientDbDirName = orientDbDirName;
  }

  public String getOrientDbDirName() {
    return orientDbDirName;
  }

  public void setOrientDbName(String orientDbName) {
    this.orientDbName = orientDbName;
  }

  public String getOrientDbName() {
    return orientDbName;
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
}