package com.tinkerpop.graph.benchmark.neo4j.index;

/**
 *   By virtue of the Neo4j dependency.....
 *
 *   This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU Affero General Public License as
 *    published by the Free Software Foundation, either version 3 of the
 *    License, or (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
import java.util.HashMap;

import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;

/**
 * A wrapper for the Neo4j Batch index service
 * @author MAHarwood
 */
public class NeoLuceneIndexServiceImpl implements SimpleKeyToNodeIdIndex
{
  private BatchInserterIndexProvider indexService;
  private BatchInserterIndex         nodeIndex;

  public NeoLuceneIndexServiceImpl(BatchInserter inserter)
  {
    // create the batch index service
    indexService = new LuceneBatchInserterIndexProvider(inserter);
    nodeIndex = indexService.nodeIndex("nodes", MapUtil.stringMap("type", "exact"));
    nodeIndex.setCacheCapacity("udk", 100000); //$NON-NLS-1$
  }

  @Override
  public void close()
  {
    //		indexService.optimize();
    nodeIndex.flush();
    indexService.shutdown();
  }

  @Override
  public long getGraphNodeId(String udk)
  {
    long result = -1;
    //TODO need to flush to see changes?
    IndexHits<Long> hits = nodeIndex.get("udk", udk);
    if (hits != null)
    {
      Long possibleNullReturn = hits.getSingle();
      if (possibleNullReturn != null)
      {
        result = possibleNullReturn;
      }
    }
    return result;
  }

  @Override
  public void put(String udk, long graphNodeId)
  {
    HashMap<String, Object> properties = new HashMap<String, Object>();
    properties.put("udk", udk);
    nodeIndex.add(graphNodeId, properties);
    //TODO when to flush?
  }
}