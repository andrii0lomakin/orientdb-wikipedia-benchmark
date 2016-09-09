package com.tinkerpop.graph.benchmark;

public interface GraphTraverserService {

  boolean databaseExists();

  void setSeed(long seed);

  String randomVertex();

  String[] shortestPath(String from, String to);

  void traverse(String[] path);

  /**
   * Close and commit all changes. This is called once at the end of the load.
   * The database may choose to flush and or commit batches of content at it's own discretion during the load
   * but must finalise all such activity as part of this close call.
   */
  void close();
}
