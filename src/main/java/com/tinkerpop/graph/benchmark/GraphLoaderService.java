package com.tinkerpop.graph.benchmark;

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
 * A common abstraction for a service to load a large volume of related data.
 * This is deliberately simplified from Tinkerpop's Blueprint's APIs with no
 * requirements for indexing properties or application control over transaction scope.
 * <p/>
 * The challenge is simply to demonstrate loading a large amount of linked data into a
 * graph database as a simple stress test of dealing with a large volume of data.
 * <p/>
 * Implementations need not offer any guarantees over transactional integrity other than
 * when close() is called successfully all content is safely on the disk. A process death at
 * any other time does not have to offer any guarantees over database integrity.
 */
public interface GraphLoaderService {
	/**
	 * Add an edge from one Vertex to another, creating any new Vertex objects where necessary
	 *
	 * @param fromNodeKey The user-defined primary key of the edge's "from" node which may or may not already exist
	 * @param toNodeKey   The user-defined primary key of the edge's "to" node which may or may not already exist
	 */
	public void addLink(String fromNodeKey, String toNodeKey);

	/**
	 * Close and commit all changes. This is called once at the end of the load.
	 * The database may choose to flush and or commit batches of content at it's own discretion during the load
	 * but must finalise all such activity as part of this close call.
	 */
	public void close();
}
