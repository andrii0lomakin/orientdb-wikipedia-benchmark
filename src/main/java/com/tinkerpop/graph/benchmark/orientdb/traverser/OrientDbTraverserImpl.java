package com.tinkerpop.graph.benchmark.orientdb.traverser;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.graph.sql.functions.OSQLFunctionShortestPath;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.graph.benchmark.GraphTraverserService;

import java.io.File;
import java.util.List;

/**
 * @author Sergey Sitnikov
 */
public class OrientDbTraverserImpl implements GraphTraverserService {
  private String orientDbDirName;
  private String orientDbName;

  private OrientGraph graph;
  private long        pageCount;

  public void init() {
    if (!new File(getOrientDbDirName()).exists()) {
      graph = null;
      return;
    }

    graph = new OrientGraph("plocal:" + getOrientDbDirName(), false);
    pageCount = graph.countVertices("Page");
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
    return graph.getVertices("Page.serial", serial).iterator().next().getProperty("udk");
  }

  @Override
  public String[] shortestPath(String from, String to) {
    final Vertex fromVertex = graph.getVertices("Page.udk", from).iterator().next();
    final Vertex toVertex = graph.getVertices("Page.udk", to).iterator().next();

    final OSQLFunctionShortestPath function = new OSQLFunctionShortestPath();
    final List<ORID> result = function.execute(null, null, null, new Object[] { fromVertex, toVertex }, new OBasicCommandContext());

    return result.stream().map(rid -> (String) graph.getVertex(rid).getProperty("udk")).toArray(String[]::new);
  }

  @Override
  public void traverse(String[] path) {
    for (String s : path) {
      final Vertex v = graph.getVertices("Page.udk", s).iterator().next();
      for (Vertex vv : v.getVertices(Direction.BOTH, "contains"))
        if (vv.getProperty("udk") == null)
          System.out.println("should never happen ;)");
    }
  }

  @Override
  public void close() {
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
}
