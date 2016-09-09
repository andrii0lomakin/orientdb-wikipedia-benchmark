package com.tinkerpop.graph.benchmark.neo4j.index;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



/**
 * Common abstraction for services that provide simple lookups for user-defined primary key strings and
 * Vertex ids already stored in a graph.
 * This is typically where the bulk of time can go in loading large volumes of data because it is reliant on
 * an efficient index capable of scaling.
 *
 * @author MAHarwood
 *
 */
public interface SimpleKeyToNodeIdIndex
{
  //Adds a new record - assumption is client has checked user defined key (udk) is not stored already using getGraphNodeId
  public void put(String udk, long graphNodeId);
  //Returns nodeid or -1 if is not store
  public long getGraphNodeId(String udk);
  //Closes index and completes any flushing/committing activity before returning
  public void close();
}