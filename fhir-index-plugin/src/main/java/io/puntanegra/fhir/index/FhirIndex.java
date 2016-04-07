package io.puntanegra.fhir.index;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.transactions.IndexTransaction.Type;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder.Group;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cassandra Secondary {@link Index} implementation that uses Lucene to index
 * FHIR Resources. <br>
 * The index operates on a JSON column where the Resources are stored as JSON.
 * This index firstly parses the JSON, then based on the index's configuration
 * extracts the values from the Resource and finally creates a Lucene
 * {@link Document} with the indexed search parameters.
 * <p>
 * This implementation uses HAPI library
 * (http://jamesagnew.github.io/hapi-fhir/) to parse and extract information
 * from the FHIR Resource.
 * <p>
 * The index uses an extension of Lucene {@link QueryParser} to parse the query
 * expression.<br>
 * 
 * It allows the following type of search:
 * <li>Range: value-quantity:[1 TO 200]</li><br>
 * <li>Wildcard: family:Pero*</li><br>
 * <li>Full Text Search: name:Jonh</li><br>
 * <li>Boolean Search: family:Pero* AND active:true</li><br>
 * <p>
 * It also support sorting and top-k queries.
 * 
 * <p>
 * NOTE: This implementation is based on Stratio cassandra-lucene index (
 * <a>https://github.com/Stratio/cassandra-lucene-index</a>).
 * 
 * @author Jorge L. Middleton {@literal <jorge.middleton@gmail.com>}
 *
 */
public class FhirIndex implements Index {

	private static final Logger logger = LoggerFactory.getLogger(FhirIndex.class);

	private final ColumnFamilyStore table;
	private final IndexMetadata config;
	private FhirIndexService service;
	private String name;

	/**
	 * Builds a new Lucene index for the specified {@link ColumnFamilyStore}
	 * using the specified {@link IndexMetadata}.
	 *
	 * @param table
	 *            the indexed {@link ColumnFamilyStore}
	 * @param indexDef
	 *            the index's metadata
	 */
	public FhirIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef) {
		logger.debug("Building Lucene index {} {}", baseCfs.metadata, indexDef);

		this.table = baseCfs;
		this.config = indexDef;

		this.service = new FhirIndexService();
		service.build(this.table, this.config);

		this.name = service.getName();

	}

	public AbstractType<?> customExpressionValueType() {
		return UTF8Type.instance;
	}

	public boolean dependsOn(ColumnDefinition column) {
		return false;
	}

	public Optional<ColumnFamilyStore> getBackingTable() {
		return Optional.empty();
	}

	public Callable<?> getBlockingFlushTask() {
		return () -> {
			logger.info("Flushing Lucene index {}", name);
			service.commit();
			return null;
		};
	}

	public long getEstimatedResultRows() {
		logger.trace("Getting the estimated result rows");
		return 1;
	}

	public IndexMetadata getIndexMetadata() {
		return this.config;
	}

	public Callable<?> getInitializationTask() {
		logger.info("Getting initialization task of {}", name);
		if (table.isEmpty() || SystemKeyspace.isIndexBuilt(table.keyspace.getName(), config.name)) {
			logger.info("Index {} doesn't need (re)building", name);
			return null;
		} else {
			logger.info("Index {} needs (re)building", name);
			return () -> {
				table.forceBlockingFlush();
				service.truncate();
				table.indexManager.buildIndexBlocking(this);
				return null;
			};
		}
	}

	public Callable<?> getInvalidateTask() {
		return () -> {
			service.delete();
			return null;
		};
	}

	public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata) {
		return () -> {
			logger.debug("Reloading Lucene index {} metadata: {}", name, indexMetadata);
			return null;
		};
	}

	public RowFilter getPostIndexQueryFilter(RowFilter filter) {
		logger.trace("Getting the post index query filter for {}", filter);
		return filter;
	}

	public Callable<?> getTruncateTask(long time) {
		logger.trace("Getting truncate task");
		return () -> {
			logger.info("Truncating Lucene index {}...", name);
			service.truncate();
			logger.info("Truncated Lucene index {}", name);
			return null;
		};
	}

	/**
	 * This method is invoked when a new row is inserted/updated to the table
	 */
	public Indexer indexerFor(DecoratedKey key, PartitionColumns columns, int nowInSec, Group opGroup,
			Type transactionType) {
		return service.indexWriter(key, columns, nowInSec, opGroup, transactionType);
	}

	public BiFunction<PartitionIterator, ReadCommand, PartitionIterator> postProcessorFor(ReadCommand arg0) {
		return (partitions, readCommand) -> service.postProcess(partitions, readCommand);
	}

	public void register(IndexRegistry indexRegistry) {
		indexRegistry.registerIndex(this);
	}

	/**
	 * This method is invoked when a CQL query is executed.
	 */
	public Searcher searcherFor(ReadCommand command) {
		logger.trace("Getting searcher for {}", command);
		try {
			return service.searcher(command);
		} catch (Exception e) {
			logger.error("Error while searching", e);
			throw new InvalidRequestException(e.getMessage());
		}
	}

	public boolean shouldBuildBlocking() {
		return true;
	}

	public boolean supportsExpression(ColumnDefinition column, Operator operator) {
		logger.trace("Asking if it supports the expression {} {}", column, operator);
		return false;
	}

	public void validate(PartitionUpdate update) throws InvalidRequestException {
		logger.trace("Validating {}...", update);
		try {
			service.validate(update);
		} catch (Exception e) {
			throw new InvalidRequestException(e.getMessage());
		}

	}

}
