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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadOrderGroup;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.filter.RowFilter.Expression;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index.Indexer;
import org.apache.cassandra.index.Index.Searcher;
import org.apache.cassandra.index.transactions.IndexTransaction.Type;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.OpOrder.Group;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.CachingWrapperQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.fhir.rest.annotation.Search;
import io.puntanegra.fhir.index.cache.SearchCache;
import io.puntanegra.fhir.index.cache.SearchCacheEntry;
import io.puntanegra.fhir.index.cache.SearchCacheUpdater;
import io.puntanegra.fhir.index.config.IndexOptions;
import io.puntanegra.fhir.index.lucene.LuceneDocumentIterator;
import io.puntanegra.fhir.index.lucene.LuceneService;
import io.puntanegra.fhir.index.mapper.FhirMapper;
import io.puntanegra.fhir.index.mapper.KeyMapper;
import io.puntanegra.fhir.index.mapper.PartitionMapper;
import io.puntanegra.fhir.index.mapper.TokenMapper;
import io.puntanegra.fhir.index.util.ByteBufferUtils;
import io.puntanegra.fhir.index.util.TaskQueue;

/**
 * Service that implement the logic behind the index. It reads the Cassandra's
 * index configuration and passes to {@link LuceneService} to create the index.
 * <br>
 * It also creates the mappers required to process any CRUD operation on the
 * {@link SSTable}
 * 
 * @author Jorge L. Middleton {@literal <jorge.middleton@gmail.com>}
 *
 */
public class FhirIndexService {

	protected static final Logger logger = LoggerFactory.getLogger(FhirIndexService.class);

	public ColumnFamilyStore table;
	public IndexMetadata config;
	public CFMetaData metadata;
	public String name;

	public LuceneService lucene;
	public TaskQueue queue;
	public IndexOptions indexOptions;
	public TokenMapper tokenMapper;
	public PartitionMapper partitionMapper;
	public FhirMapper fhirMapper;
	public boolean mapsMultiCells;

	public SearchCache searchCache;
	private KeyMapper keyMapper;

	public FhirIndexService() {
	}

	public void build(ColumnFamilyStore cfStore, IndexMetadata cfg) {
		table = cfStore;
		config = cfg;
		metadata = table.metadata;

		CFMetaData metadata = table.metadata;
		name = config.name;

		// Setup cache, index and write queue
		indexOptions = new IndexOptions(metadata, config);

		String mbeanName = String.format("com.stratio.cassandra.lucene:type=Lucene,keyspace=%s,table=%s,index=%s",
				metadata.ksName, metadata.cfName, name);

		searchCache = new SearchCache(metadata, indexOptions.searchCacheSize);
		lucene = new LuceneService();
		lucene.init(name, mbeanName, indexOptions.path, indexOptions.search.defaultAnalyzer,
				indexOptions.refreshSeconds, indexOptions.ramBufferMB, indexOptions.maxMergeMB,
				indexOptions.maxCachedMB, searchCache::invalidate);
		queue = new TaskQueue(indexOptions.indexingThreads, indexOptions.indexingQueuesSize);

		// Setup mappers
		fhirMapper = new FhirMapper(indexOptions.search);
		tokenMapper = new TokenMapper();
		partitionMapper = new PartitionMapper(metadata);
		keyMapper = new KeyMapper(metadata);
	}

	public String getName() {
		return name;
	}

	/**
	 * Commits the pending changes.
	 */
	public final void commit() {
		queue.submitSynchronous(lucene::commit);
	}

	/**
	 * Deletes all the index contents.
	 */
	public final void truncate() {
		queue.submitSynchronous(lucene::truncate);
	}

	/**
	 * Completely deletes the index.
	 */
	public void delete() {
		queue.shutdown();
		lucene.delete();
	}

	/**
	 * Upserts the specified {@link Row} to Lucene Index. <br>
	 * Based on the {@link Row} information, it creates a Lucene
	 * {@link Document} and calls the upsert on the {@link LuceneService}.
	 *
	 * @param key
	 *            the partition key
	 * @param row
	 *            the row to be upserted
	 */
	public void upsert(DecoratedKey key, Row row) {
		queue.submitAsynchronous(key, () -> document(key, row).ifPresent(document -> {
			Term term = term(key, row);
			lucene.upsert(term, document);
		}));
	}

	/**
	 * Deletes the partition identified by the specified key.
	 *
	 * @param key
	 *            the partition key
	 * @param row
	 *            the row to be deleted
	 */
	public void delete(DecoratedKey key, Row row) {
		queue.submitAsynchronous(key, () -> {
			Term term = term(key, row);
			lucene.delete(term);
		});
	}

	/**
	 * Deletes the partition identified by the specified key.
	 *
	 * @param key
	 *            the partition key
	 */
	public void delete(DecoratedKey key) {
		queue.submitAsynchronous(key, () -> {
			Term term = term(key);
			lucene.delete(term);
		});
	}

	/**
	 * This method is invoked when a CQL query is executed.
	 * 
	 * @param command
	 * @return
	 */
	// TODO: add support for ORDER BY. i.e.: ORDER BY family:asc, given:desc
	public Searcher searcher(ReadCommand command) {
		// Parse search
		String expression = expression(command);
		// Search search = SearchBuilder.fromJson(expression).build();
		Sort sort = null;// new Sort(new SortField("family",
							// SortField.Type.STRING));

		// Try luck with cache
		Optional<SearchCacheEntry> optional = searchCache.get(expression, command);
		if (optional.isPresent()) {
			logger.debug("Search cache hits");
			SearchCacheEntry entry = optional.get();
			Query query = entry.getQuery();
			ScoreDoc after = entry.getScoreDoc();
			SearchCacheUpdater cacheUpdater = entry.updater();
			return (ReadOrderGroup orderGroup) -> read(query, sort, after, command, orderGroup, cacheUpdater);
		} else {
			logger.debug("Search cache fails");
			Query query = new CachingWrapperQuery(query(expression, command));
			searchCache.put(expression, command, query);
			SearchCacheUpdater cacheUpdater = searchCache.updater(expression, command, query);
			return (ReadOrderGroup orderGroup) -> read(query, sort, null, command, orderGroup, cacheUpdater);
		}
	}

	/**
	 * Post processes in the coordinator node the results of a distributed
	 * search. Gets the k globally best results from all the k best node-local
	 * results.
	 *
	 * @param partitions
	 *            the node results iterator
	 * @param command
	 *            the read command
	 * @return the k globally best results
	 */
	public PartitionIterator postProcess(PartitionIterator partitions, ReadCommand command) {

		// Search search = search(command);
		//
		// // Skip if search does not require full scan
		// if (search.requiresFullScan()) {
		//
		// List<Pair<DecoratedKey, SimpleRowIterator>> collectedRows =
		// collect(partitions);
		//
		// Query query = search.query(schema);
		// Sort sort = sort(search);
		// int limit = command.limits().count();
		//
		// // Skip if search is not top-k TODO: Skip if only one partitioner
		// // range is involved
		// if (search.isTopK()) {
		// return process(query, sort, limit, collectedRows);
		// }
		// }
		return partitions;
	}

	// private SimplePartitionIterator process(Query query, Sort sort, int
	// limit,
	// List<Pair<DecoratedKey, SimpleRowIterator>> collectedRows) {
	// TimeCounter sortTime = TimeCounter.create().start();
	// List<SimpleRowIterator> processedRows = new LinkedList<>();
	// try {
	//
	// // Index collected rows in memory
	// RAMIndex index = new RAMIndex(schema.getAnalyzer());
	// Map<Term, SimpleRowIterator> rowsByTerm = new HashMap<>();
	// for (Pair<DecoratedKey, SimpleRowIterator> pair : collectedRows) {
	// DecoratedKey key = pair.left;
	// SimpleRowIterator rowIterator = pair.right;
	// Row row = rowIterator.getRow();
	// Term term = term(key, row);
	// Document document = document(key, row).get();
	// rowsByTerm.put(term, rowIterator);
	// index.add(document);
	// }
	//
	// // Repeat search to sort partial results
	// List<Document> documents = index.search(query, sort, limit,
	// fieldsToLoad());
	// index.close();
	//
	// // Collect post processed results
	// for (Document document : documents) {
	// Term term = term(document);
	// SimpleRowIterator rowIterator = rowsByTerm.get(term);
	// processedRows.add(rowIterator);
	// }
	//
	// } finally {
	// logger.debug("Post-processed {} collected rows to {} rows in {}",
	// collectedRows.size(),
	// processedRows.size(), sortTime.stop());
	// }
	// return new SimplePartitionIterator(processedRows);
	// }

	/**
	 * Creates an {@link Indexer} writer which will process the inserted/updated
	 * {@link Row}.
	 * 
	 * @param key
	 * @param columns
	 * @param nowInSec
	 * @param opGroup
	 * @param transactionType
	 * @return
	 */
	public Indexer indexWriter(DecoratedKey key, PartitionColumns columns, int nowInSec, Group opGroup,
			Type transactionType) {
		return new FhirIndexIndexer(this, key, nowInSec, opGroup, transactionType);
	}

	/**
	 * Validate the values present in {@link PartitionUpdate} are valid
	 * according to the index schema.
	 * 
	 * @param update
	 */
	public void validate(PartitionUpdate update) {
		// TODO we might need to validate with HAPI if the JSON is valid

	}

	/**
	 * Returns the {@link Search} contained in the specified {@link ReadCommand}
	 * .
	 *
	 * @param command
	 *            the read command containing the {@link Search}
	 * @return the {@link Search} contained in {@code command}
	 */
	private String expression(ReadCommand command) {
		for (Expression expression : command.rowFilter().getExpressions()) {
			if (expression.isCustom()) {
				RowFilter.CustomExpression customExpression = (RowFilter.CustomExpression) expression;
				if (name.equals(customExpression.getTargetIndex().name)) {
					ByteBuffer bb = customExpression.getValue();
					return UTF8Type.instance.compose(bb);
				}
			}
		}
		throw new FhirIndexException("Lucene search expression not found in command expressions");
	}

	/**
	 * Retrieves from the local storage all the {@link Row}s in the specified
	 * partition.
	 *
	 * @param key
	 *            the partition key
	 * @param nowInSec
	 *            max allowed time in seconds
	 * @param opGroup
	 *            operation group spanning the calling operation
	 * @return a {@link Row} iterator
	 */
	public UnfilteredRowIterator read(DecoratedKey key, int nowInSec, OpOrder.Group opGroup) {
		return read(key, clusterings(Clustering.EMPTY), nowInSec, opGroup);
	}

	/**
	 * Retrieves from the local storage the {@link Row}s in the specified
	 * partition slice.
	 *
	 * @param key
	 *            the partition key
	 * @param clusterings
	 *            the clustering keys
	 * @param nowInSec
	 *            max allowed time in seconds
	 * @param opGroup
	 *            operation group spanning the calling operation
	 * @return a {@link Row} iterator
	 */
	private UnfilteredRowIterator read(DecoratedKey key, NavigableSet<Clustering> clusterings, int nowInSec,
			OpOrder.Group opGroup) {
		ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(clusterings, false);
		ColumnFilter columnFilter = ColumnFilter.all(metadata);
		return SinglePartitionReadCommand.create(metadata, nowInSec, key, columnFilter, filter)
				.queryMemtableAndDisk(table, opGroup);
	}

	/**
	 * Reads from the local SSTables the rows identified by the specified
	 * search.
	 *
	 * @param query
	 *            the Lucene query
	 * @param sort
	 *            the Lucene sort
	 * @param after
	 *            the last Lucene doc
	 * @param command
	 *            the Cassandra command
	 * @param orderGroup
	 *            the Cassandra read order group
	 * @param cacheUpdater
	 *            the search cache updater
	 * @return the local {@link Row}s satisfying the search
	 */
	private UnfilteredPartitionIterator read(Query query, Sort sort, ScoreDoc after, ReadCommand command,
			ReadOrderGroup orderGroup, SearchCacheUpdater cacheUpdater) {
		int limit = command.limits().count();
		LuceneDocumentIterator documents = lucene.search(query, sort, after, limit, fieldsToLoad());
		return new FhirIndexSearcher(this, command, table, orderGroup, documents, cacheUpdater);
	}

	private Set<String> fieldsToLoad() {
		return new HashSet<>(Arrays.asList(PartitionMapper.FIELD_NAME, KeyMapper.FIELD_NAME));
	}

	/**
	 * Returns the {@link DecoratedKey} contained in the specified Lucene
	 * {@link Document}.
	 *
	 * @param document
	 *            the {@link Document} containing the partition key to be get
	 * @return the {@link DecoratedKey} contained in the specified Lucene
	 *         {@link Document}
	 */
	public DecoratedKey decoratedKey(Document document) {
		return partitionMapper.decoratedKey(document);
	}

	/**
	 * Returns the Lucene {@link Query} represented by the specified
	 * {@link Search} and key filter.
	 *
	 * @param expression
	 *            the expression
	 * @param command
	 *            the command
	 * @return a Lucene {@link Query}
	 */
	private Query query(String expression, ReadCommand command) {
		try {
			QueryParser queryParser = new QueryParser("query", this.indexOptions.search.defaultAnalyzer);
			queryParser.setDateResolution(Resolution.SECOND);
			Query searchQuery = queryParser.parse(expression);
			
			return searchQuery;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			throw new FhirIndexException(e);
		}

		// TODO: mejorar las busquedas por tipo
		// Optional<Query> maybeKeyRangeQuery = query(command);
		// if (maybeKeyRangeQuery.isPresent()) {
		// BooleanQuery.Builder builder = new BooleanQuery.Builder();
		// builder.add(maybeKeyRangeQuery.get(), FILTER);
		// builder.add(searchQuery, MUST);
		// return builder.build();
		// } else {
		// }
	}

	/**
	 * Creates a Lucene {@link Document} to index a FHIR {@link IBaseResource}.
	 * 
	 * @param key,
	 *            the key of the row inserted.
	 * @param row,
	 *            the row inserted.
	 * @return the Lucene {@link Document} to be inserted in the index.
	 */
	public Optional<Document> document(DecoratedKey key, Row row) {
		Document document = new Document();

		Cell cell = row.getCell(this.indexOptions.targetColumn);
		ByteBuffer value = cell.value();
		if (cell != null && !ByteBufferUtils.isEmpty(value)) {
			String json = ByteBufferUtils.toString(value, UTF8Type.instance);
			fhirMapper.addFields(document, json);
		}

		if (document.getFields().isEmpty()) {
			return Optional.empty();
		} else {
			Clustering clustering = row.clustering();
			tokenMapper.addFields(document, key);
			partitionMapper.addFields(document, key);
			keyMapper.addFields(document, key, clustering);
			return Optional.of(document);
		}
	}

	/**
	 * Returns a Lucene {@link Term} identifying the {@link Document}
	 * representing the {@link Row} identified by the specified partition and
	 * clustering keys.
	 *
	 * @param key
	 *            the partition key
	 * @param clustering
	 *            the clustering key
	 * @return the term identifying the document
	 */
	private Term term(DecoratedKey key, Clustering clustering) {
		return keyMapper.term(key, clustering);
	}

	@SuppressWarnings("unused")
	private Term term(Document document) {
		return KeyMapper.term(document);
	}

	private Term term(DecoratedKey key, Row row) {
		return term(key, row.clustering());
	}

	/**
	 * Returns a Lucene {@link Term} identifying documents representing all the
	 * {@link Row}'s which are in the partition the specified
	 * {@link DecoratedKey}.
	 *
	 * @param key
	 *            the partition key
	 * @return a Lucene {@link Term} representing {@code key}
	 */
	private Term term(DecoratedKey key) {
		return null;// partitionMapper.term(key);
	}

	/**
	 * Returns a {@link NavigableSet} of the specified clusterings, sorted by
	 * the table metadata.
	 *
	 * @param clusterings
	 *            the clusterings to be included in the set
	 * @return the navigable sorted set
	 */
	public NavigableSet<Clustering> clusterings(Clustering... clusterings) {
		NavigableSet<Clustering> sortedClusterings = new TreeSet<>(metadata.comparator);
		if (clusterings.length > 0) {
			sortedClusterings.addAll(Arrays.asList(clusterings));
		}
		return sortedClusterings;
	}

	/**
	 * Returns the clustering key contained in the specified {@link Document}.
	 *
	 * @param document
	 *            a {@link Document} containing the clustering key to be get
	 * @return the clustering key contained in {@code document}
	 */
	public Clustering clustering(Document document) {
		return keyMapper.clustering(document);
	}

	/**
	 * Returns if SSTables can contain additional columns of the specified
	 * {@link Row} so read-before-write is required prior to indexing.
	 *
	 * @param key
	 *            the partition key
	 * @param row
	 *            the {@link Row}
	 * @return {@code true} if read-before-write is required, {@code false}
	 *         otherwise
	 */
	public boolean needsReadBeforeWrite(DecoratedKey key, Row row) {
		return false;
	}
}
