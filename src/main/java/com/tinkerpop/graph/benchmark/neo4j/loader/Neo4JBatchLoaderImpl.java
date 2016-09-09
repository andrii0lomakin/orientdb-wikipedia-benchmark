package com.tinkerpop.graph.benchmark.neo4j.loader;

import com.tinkerpop.graph.benchmark.GraphLoaderService;
import com.tinkerpop.graph.benchmark.neo4j.index.NeoLuceneIndexServiceImpl;
import com.tinkerpop.graph.benchmark.neo4j.index.SimpleKeyToNodeIdIndex;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link GraphLoaderService} that uses Neo4J's batch inserter
 * along with a choice of index implementations
 *
 * @author MAHarwood
 *
 */
public class Neo4JBatchLoaderImpl implements GraphLoaderService
{

  SimpleKeyToNodeIdIndex keyService;
  private BatchInserter inserter;

  String neoDbDirName;
  String batchPropertiesFileName;


  public Neo4JBatchLoaderImpl()
  {

  }
  public void init() throws IOException {
    deleteDirectory(new File(neoDbDirName));
    inserter = BatchInserters.inserter(new File(neoDbDirName));
    //Choice of indexed key service now Spring-wired with fallback to standard Neo implementation
    if(keyService==null)
    {
      //No custom key service - fall back to Neo4J's default implementation
      keyService=new NeoLuceneIndexServiceImpl(inserter);
    }
  }

  static public boolean deleteDirectory(File path) {
    if( path.exists() ) {
      File[] files = path.listFiles();
      for(int i=0; i<files.length; i++) {
        if(files[i].isDirectory()) {
          deleteDirectory(files[i]);
        }
        else {
          files[i].delete();
        }
      }
    }
    return( path.delete() );
  }

  @Override
  public void addLink(String fromNodeKey, String toNodeKey)
  {
    long fromId=-1;
    //it is likely that fromNodeKey is the same as the last call because of the way the Wikipedia content is organised
    if(fromNodeKey.equals(lastFromNodeKey))
    {
      fromId=lastFromId;
    }
    else
    {
      //See if node exists using index
      fromId=keyService.getGraphNodeId(fromNodeKey);
      if(fromId<0)
      {
        //New vertex - add to graph
        Map<String,Object> properties = new HashMap<String,Object>();
        properties.put( "udk", fromNodeKey );
        fromId= inserter.createNode( properties);
        //add to index
        keyService.put(fromNodeKey, fromId);
      }
      lastFromId=fromId;
      lastFromNodeKey=fromNodeKey;
    }

    long toId=-1;
    if(toNodeKey.equals(lastToNodeKey))
    {
      toId=lastToId;
    }
    else
    {
      toId=keyService.getGraphNodeId(toNodeKey);
      if(toId<0)
      {
        //New vertex- add to graph
        Map<String,Object> properties = new HashMap<String,Object>();
        properties.put( "udk", toNodeKey );
        toId= inserter.createNode( properties);
        //add to index
        keyService.put(toNodeKey, toId);
      }
      lastToNodeKey=toNodeKey;
      lastToId=toId;
    }
    //Create edge
    inserter.createRelationship( fromId, toId, DynamicRelationshipType.withName( "LINKSTO" ), null );
  }
  //Cached lookup from last call
  private String lastFromNodeKey;
  private long lastFromId;
  private String lastToNodeKey;
  private long lastToId;

  @Override
  public void close()
  {
    keyService.close();
    inserter.shutdown();
  }
  public SimpleKeyToNodeIdIndex getKeyService()
  {
    return keyService;
  }
  public void setKeyService(SimpleKeyToNodeIdIndex keyService)
  {
    this.keyService = keyService;
  }
  public String getNeoDbDirName()
  {
    return neoDbDirName;
  }
  public void setNeoDbDirName(String neoDbDirName)
  {
    this.neoDbDirName = neoDbDirName;
  }
  public String getBatchPropertiesFileName()
  {
    return batchPropertiesFileName;
  }
  public void setBatchPropertiesFileName(String batchPropertiesFileName)
  {
    this.batchPropertiesFileName = batchPropertiesFileName;
  }
}