package com.tinkerpop.graph.benchmark.orientdb;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.graph.benchmark.GraphLoaderService;

import java.io.File;

/**
 * A {@link GraphLoaderService} that uses OrientDB via Native APIs
 *
 * @author MAHarwood with help from Luca
 */
public class OrientDbNativeLoaderImpl implements GraphLoaderService {
	private String orientDbDirName;
	private String orientDbName;
	private OrientBaseGraph graph;
	OrientIndex index;

	public OrientDbNativeLoaderImpl() {
	}

	public void init() {
		OGlobalConfiguration.CACHE_LEVEL2_ENABLED.setValue(false);
		OGlobalConfiguration.CACHE_LEVEL2_SIZE.setValue(0);

		File dir = new File(getOrientDbDirName());
		deleteDirectory(dir);
		dir.mkdirs();

		graph = new OrientGraph("plocal:" + getOrientDbDirName());
		graph.drop();

		graph = new OrientGraph("plocal:" + getOrientDbDirName());

		index = new OrientIndex(graph);
	}

	@Override
	public void addLink(String fromNodeKey, String toNodeKey) {
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
				fromNode = graph.addVertex(null);
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
				toNode = graph.addVertex(null);
				toNodeInsertIntoIndex = true;
			}
			lastToNode = toNode;
			lastToNodeKey = toNodeKey;
		}
		// Create the edge
		graph.addEdge(null, fromNode, toNode, "contains");
		graph.commit();

		if (fromNodeInsertIntoIndex) {
			index.storeVertex(fromNodeKey, fromNode);
		}
		if (toNodeInsertIntoIndex) {
			index.storeVertex(toNodeKey, toNode);
		}
		graph.commit();
	}

	private OrientVertex getVertexFromIndex(final String fromNodeKey) {
		final ODocument v = index.getVertex(fromNodeKey);
		if (v == null)
			return null;

		return graph.getVertex(v);
	}


	static class OrientIndex {
		OrientBaseGraph graph;
		private OIndex<OIdentifiable> index;

		public OrientIndex(OrientBaseGraph graph) {
			this.graph = graph;
			graph.getRawGraph().commit();
			index = (OIndex<OIdentifiable>) graph.getRawGraph()
							.getMetadata()
							.getIndexManager()
							.createIndex("idx", "UNIQUE", new OSimpleKeyIndexDefinition(OType.STRING),
											null, null, null);
			graph.commit();
		}


		public void storeVertex(String udk, OrientVertex vertex) {
			index.put(udk, vertex.getIdentity());
		}

		public ODocument getVertex(String udk) {
			final OIdentifiable result = index.get(udk);
			if (result != null) {
				return (ODocument) result.getRecord();
			}
			return null;
		}
	}

	//Cached lookup from last call
	private String lastFromNodeKey;
	private OrientVertex lastFromNode;
	private String lastToNodeKey;
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