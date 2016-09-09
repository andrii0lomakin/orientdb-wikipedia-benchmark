package com.tinkerpop.graph.benchmark;

import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;

import java.util.ArrayList;
import java.util.List;

public class TestGraphTraversal {

  private GraphTraverserService graphTraverserService;
  private long                  seed;
  private int                   iterations;

  public static void main(String[] args) throws Exception {
    String testFile = "/config/graphBeans.xml";
    GenericXmlApplicationContext context = new GenericXmlApplicationContext();
    context.load(new ClassPathResource(testFile));
    TestGraphTraversal testRunner = (TestGraphTraversal) context.getBean("traversalTestRunner");
    testRunner.runTest();
  }

  public void runTest() throws Exception {
    if (!graphTraverserService.databaseExists()) {
      System.out.println("No database found. Run graph load test with full Wikipedia import first.");
      return;
    }

    graphTraverserService.setSeed(seed);

    final List<String[]> paths = new ArrayList<>();

    long total = 0;
    for (int t = 0; t < iterations; ++t) {
      final String from = graphTraverserService.randomVertex();
      final String to = graphTraverserService.randomVertex();
      System.out.print("\nSearching for shortest path from '" + from + "' to '" + to + "'... ");
      final long start = System.nanoTime();
      final String[] path = graphTraverserService.shortestPath(from, to);
      final long elapsed = System.nanoTime() - start;
      System.out.println("done in " + elapsed / 1000000 + "ms");
      total += elapsed;

      for (int i = 0; i < path.length; ++i) {
        final String vertex = path[i];
        System.out.print(vertex);
        if (i != path.length - 1)
          System.out.print(" -> ");
      }
      System.out.println();

      if (path.length != 0)
        paths.add(path);
    }

    System.out.println("\nTotal time spent searching: " + total / 1000000 + "ms");

    final long start = System.currentTimeMillis();
    System.out.print("\nTraversing... ");
    for (String[] path : paths)
      graphTraverserService.traverse(path);
    System.out.println("done in " + (System.currentTimeMillis() - start) + "ms");

    System.out.println("\nIssuing close request");

    graphTraverserService.close();
  }

  public GraphTraverserService getGraphTraverserService() {
    return graphTraverserService;
  }

  public void setGraphTraverserService(GraphTraverserService graphTraverserService) {
    this.graphTraverserService = graphTraverserService;
  }

  public long getSeed() {
    return seed;
  }

  public void setSeed(long seed) {
    this.seed = seed;
  }

  public int getIterations() {
    return iterations;
  }

  public void setIterations(int iterations) {
    this.iterations = iterations;
  }
}
