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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.puntanegra.fhir.index.util.ByteBufferUtils;

/**
 * Lucene-based {@link Index.Indexer} that processes events emitted during partition updates.
 *
 * @author Jorge L. Middleton {@literal <jorge.middleton@gmail.com>}
 *
 */
public class FhirIndexIndexer implements Index.Indexer {

	private static final Logger logger = LoggerFactory.getLogger(FhirIndexIndexer.class);

	private final FhirIndexService service;
	private final DecoratedKey key;
	private final int nowInSec;
	private final OpOrder.Group opGroup;
	private final IndexTransaction.Type transactionType;
	private final Map<Clustering, Optional<Row>> rows;

	/**
	 * Builds a new {@link FhirIndexIndexer} for tables with wide rows.
	 *
	 * @param service
	 *            the service to perform the indexing operation
	 * @param key
	 *            key of the partition being modified
	 * @param nowInSec
	 *            current time of the update operation
	 * @param opGroup
	 *            operation group spanning the update operation
	 * @param transactionType
	 *            what kind of update is being performed on the base data
	 */
	public FhirIndexIndexer(FhirIndexService service, DecoratedKey key, int nowInSec, OpOrder.Group opGroup,
			IndexTransaction.Type transactionType) {
		this.service = service;
		this.key = key;
		this.nowInSec = nowInSec;
		this.opGroup = opGroup;
		this.transactionType = transactionType;
		rows = new LinkedHashMap<>();
	}

	@Override
	public void begin() {
	}

	@Override
	public void partitionDelete(DeletionTime deletionTime) {
		logger.trace("Delete partition inoked {}: {}", this.transactionType, deletionTime);
		delete();
	}

	@Override
	public void rangeTombstone(RangeTombstone tombstone) {
		// nothing to do here
	}

	@Override
	public void insertRow(Row row) {
		logger.trace("Inserting row {}: {}", this.transactionType, row);
		index(row);
	}

	@Override
	public void updateRow(Row oldRowData, Row newRowData) {
		logger.trace("Updating row {}: {} to {}", this.transactionType, oldRowData, newRowData);
		index(newRowData);
	}

	@Override
	public void removeRow(Row row) {
		logger.trace("Removing row {}: {}", this.transactionType, row);
		index(row);
	}

	/**
	 * Temporally store the row to be indexed in a map.
	 *
	 * @param row
	 *            the row to be indexed.
	 */
	public void index(Row row) {
		if (!row.isStatic()) {
			Clustering clustering = row.clustering();
			if (service.needsReadBeforeWrite(key, row)) {
				rows.put(clustering, Optional.empty());
			} else {
				rows.put(clustering, Optional.of(row));
			}
		}
	}

	public void finish() {
		// Read required rows from storage engine
		service.read(key, nowInSec, opGroup).forEachRemaining(unfiltered -> {
			Row row = (Row) unfiltered;
			rows.put(row.clustering(), Optional.of(row));
		});

		// Write rows to Lucene index
		rows.forEach((clustering, optional) -> optional.ifPresent(row -> {
			if (row.hasLiveData(nowInSec)) {
				service.upsert(key, row);
			} else {
				service.delete(key, row);
			}
		}));
	}

	/**
	 * Delete the partition.
	 */
	private void delete() {
		service.delete(key);
		rows.clear();
	}

	@SuppressWarnings("unused")
	private boolean insertInIndex(final String indexResourceType, Row row) {
		// only index in the correct lucene index.
		// this code checks if the resourceType is the same as the index.
		// TODO: externalizar resource_type

		for (Cell cell : row.cells()) {
			String columnname = cell.column().name.toString();
			if ("resource_type".equals(columnname)) {
				String resourceType = ByteBufferUtils.toString(cell.value(), UTF8Type.instance);

				if (indexResourceType.equals(resourceType)) {
					return true;
				}
				return false;
			}
		}
		return false;
	}
}
