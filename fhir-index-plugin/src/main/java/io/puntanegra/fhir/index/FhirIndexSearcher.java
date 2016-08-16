/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.puntanegra.fhir.index;

import java.util.NavigableSet;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;

import io.puntanegra.fhir.index.cache.SearchCacheUpdater;
import io.puntanegra.fhir.index.lucene.LuceneDocumentIterator;

/**
 * {@link UnfilteredPartitionIterator} for retrieving rows from Cassandra
 * partition table.
 *
 * @author Jorge L. Middleton {@literal <jorge.middleton@gmail.com>}
 *
 */
public class FhirIndexSearcher implements UnfilteredPartitionIterator {

	private final ReadCommand command;
	private final ColumnFamilyStore table;
	private final OpOrder.Group orderGroup;
	private final LuceneDocumentIterator documents;
	private UnfilteredRowIterator next;

	private final FhirIndexService service;
	private final ClusteringComparator comparator;
	private final SearchCacheUpdater cacheUpdater;
	private Pair<Document, ScoreDoc> nextDoc;

	/**
	 * Constructor taking the Cassandra read data and the Lucene results
	 * iterator.
	 *
	 * @param service
	 *            the index service
	 * @param command
	 *            the read command
	 * @param table
	 *            the base table
	 * @param orderGroup
	 *            the order group of the read operation
	 * @param documents
	 *            the documents iterator
	 * @param cacheUpdater
	 *            the search cache updater
	 */
	public FhirIndexSearcher(FhirIndexService service, ReadCommand command, ColumnFamilyStore table,
			ReadExecutionController readExecutionController, LuceneDocumentIterator documents,
			SearchCacheUpdater cacheUpdater) {

		this.command = command;
		this.table = table;
		this.orderGroup = readExecutionController.baseReadOpOrderGroup();
		this.documents = documents;

		this.service = service;
		this.comparator = service.metadata.comparator;
		this.cacheUpdater = cacheUpdater;
	}

	@Override
	public boolean isForThrift() {
		return command.isForThrift();
	}

	@Override
	public CFMetaData metadata() {
		return table.metadata;
	}

	@Override
	public boolean hasNext() {
		if (next != null) {
			return true;
		}

		if (nextDoc == null) {
			if (!documents.hasNext()) {
				return false;
			}
			nextDoc = documents.next();
		}

		DecoratedKey key = service.decoratedKey(nextDoc.left);
		NavigableSet<Clustering> clusterings = clusterings(key);

		if (clusterings.isEmpty()) {
			return hasNext();
		}

		ClusteringIndexFilter filter = new ClusteringIndexNamesFilter(clusterings, false);
		UnfilteredRowIterator data = read(key, filter);

		if (data.isEmpty()) {
			data.close();
			return hasNext();
		}

		next = data;
		return true;
	}

	@Override
	public UnfilteredRowIterator next() {
		if (next == null) {
			hasNext();
		}
		UnfilteredRowIterator result = next;
		next = null;
		return result;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() {
		try {
			if (next != null) {
				next.close();
			}
		} finally {
			documents.close();
		}
	}

	public UnfilteredRowIterator read(DecoratedKey key, ClusteringIndexFilter filter) {
		return SinglePartitionReadCommand.create(isForThrift(), table.metadata, command.nowInSec(),
				command.columnFilter(), command.rowFilter(), command.limits(), key, filter)
				.queryMemtableAndDisk(table, orderGroup);
	}

	private NavigableSet<Clustering> clusterings(DecoratedKey key) {

		NavigableSet<Clustering> clusterings = service.clusterings();
		Clustering clustering = service.clustering(nextDoc.left);

		Clustering lastClustering = null;
		while (nextDoc != null && key.getKey().equals(service.decoratedKey(nextDoc.left).getKey())
				&& (lastClustering == null || comparator.compare(lastClustering, clustering) < 0)) {
			if (command.selectsKey(key) && command.selectsClustering(key, clustering)) {
				lastClustering = clustering;
				clusterings.add(clustering);
				cacheUpdater.put(key, clustering, nextDoc.right);
			}
			if (documents.hasNext()) {
				nextDoc = documents.next();
				clustering = service.clustering(nextDoc.left);
			} else {
				nextDoc = null;
			}
			if (documents.needsFetch()) {
				break;
			}
		}
		return clusterings;
	}
}
