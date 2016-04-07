package io.puntanegra.fhir.index.config;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import io.puntanegra.fhir.index.FhirIndexException;
import io.puntanegra.fhir.index.util.JsonSerializer;

/**
 * FHIR Index configuration options parser.
 *
 * @author Jorge L. Middleton {@literal <jorge.middleton@gmail.com>}
 *
 */
public class IndexOptions {

	public static final String REFRESH_SECONDS_OPTION = "refresh_seconds";
	public static final double DEFAULT_REFRESH_SECONDS = 60;

	public static final String RAM_BUFFER_MB_OPTION = "ram_buffer_mb";
	public static final int DEFAULT_RAM_BUFFER_MB = 64;

	public static final String MAX_MERGE_MB_OPTION = "max_merge_mb";
	public static final int DEFAULT_MAX_MERGE_MB = 5;

	public static final String MAX_CACHED_MB_OPTION = "max_cached_mb";
	public static final int DEFAULT_MAX_CACHED_MB = 30;

	public static final String INDEXING_THREADS_OPTION = "indexing_threads";
	public static final int DEFAULT_INDEXING_THREADS = 0;

	public static final String INDEXING_QUEUES_SIZE_OPTION = "indexing_queues_size";
	public static final int DEFAULT_INDEXING_QUEUES_SIZE = 50;

	public static final String SEARCH_CACHE_SIZE_OPTION = "search_cache_size";
	public static final int DEFAULT_SEARCH_CACHE_SIZE = 16;

	public static final String DIRECTORY_PATH_OPTION = "directory_path";
	public static final String INDEXES_DIR_NAME = "lucene";

	public static final String SEARCH_OPTION = "search";

	public static final String RESOURCE_TYPE_COLUMN = "resource_type_column";

	public final ResourceOptions search;

	/** The path of the directory where the index files will be stored */
	public final Path path;

	public final String resourceTypeColumn;

	/** The Lucene index searcher refresh frequency, in seconds */
	public final double refreshSeconds;

	/** The Lucene's max RAM buffer size, in MB */
	public final int ramBufferMB;

	/** The Lucene's max segments merge size size, in MB */
	public final int maxMergeMB;

	/** The Lucene's max cache size, in MB */
	public final int maxCachedMB;

	/** The number of asynchronous indexing threads */
	public final int indexingThreads;

	/** The size of the asynchronous indexing queues */
	public final int indexingQueuesSize;

	/** The max size of the search cache */
	public final int searchCacheSize;

	/** ColumnDefinition of the target column associated with the index **/
	public final ColumnDefinition targetColumn;

	/**
	 * Builds a new {@link IndexOptions} for the column family and index
	 * metadata.
	 *
	 * @param tableMetadata
	 *            the indexed table metadata
	 * @param indexMetadata
	 *            the index metadata
	 */
	public IndexOptions(CFMetaData tableMetadata, IndexMetadata indexMetadata) {
		Map<String, String> options = indexMetadata.options;
		refreshSeconds = parseRefresh(options);
		resourceTypeColumn = parseResourceTypeColumn(options);
		ramBufferMB = parseRamBufferMB(options);
		maxMergeMB = parseMaxMergeMB(options);
		maxCachedMB = parseMaxCachedMB(options);
		indexingThreads = parseIndexingThreads(options);
		indexingQueuesSize = parseIndexingQueuesSize(options);
		searchCacheSize = parseSearchCacheSize(options);
		path = parsePath(options, tableMetadata, indexMetadata);
		search = parseSearchOptions(options);

		String targetColumnName = indexMetadata.options.get("target");
		targetColumn = tableMetadata.getColumnDefinition(ByteBufferUtil.bytes(targetColumnName));

	}

	private String parseResourceTypeColumn(Map<String, String> options) {
		return options.get(RESOURCE_TYPE_COLUMN);
	}

	/**
	 * Validates the specified index options.
	 *
	 * @param options
	 *            the options to be validated
	 * @param metadata
	 *            the indexed table metadata
	 */
	public static void validateOptions(Map<String, String> options, CFMetaData metadata) {
		parseRefresh(options);
		parseRamBufferMB(options);
		parseMaxMergeMB(options);
		parseMaxCachedMB(options);
		parseIndexingThreads(options);
		parseIndexingQueuesSize(options);
		parseSearchCacheSize(options);
		parseSearchOptions(options);
		parsePath(options, metadata, null);
	}

	private static double parseRefresh(Map<String, String> options) {
		String refreshOption = options.get(REFRESH_SECONDS_OPTION);
		if (refreshOption != null) {
			double refreshSeconds;
			try {
				refreshSeconds = Double.parseDouble(refreshOption);
			} catch (NumberFormatException e) {
				throw new FhirIndexException("'%s' must be a strictly positive double", REFRESH_SECONDS_OPTION);
			}
			if (refreshSeconds <= 0) {
				throw new FhirIndexException("'%s' must be strictly positive", REFRESH_SECONDS_OPTION);
			}
			return refreshSeconds;
		} else {
			return DEFAULT_REFRESH_SECONDS;
		}
	}

	private static int parseRamBufferMB(Map<String, String> options) {
		String ramBufferSizeOption = options.get(RAM_BUFFER_MB_OPTION);
		if (ramBufferSizeOption != null) {
			int ramBufferMB;
			try {
				ramBufferMB = Integer.parseInt(ramBufferSizeOption);
			} catch (NumberFormatException e) {
				throw new FhirIndexException("'%s' must be a strictly positive integer", RAM_BUFFER_MB_OPTION);
			}
			if (ramBufferMB <= 0) {
				throw new FhirIndexException("'%s' must be strictly positive", RAM_BUFFER_MB_OPTION);
			}
			return ramBufferMB;
		} else {
			return DEFAULT_RAM_BUFFER_MB;
		}
	}

	private static int parseMaxMergeMB(Map<String, String> options) {
		String maxMergeSizeMBOption = options.get(MAX_MERGE_MB_OPTION);
		if (maxMergeSizeMBOption != null) {
			int maxMergeMB;
			try {
				maxMergeMB = Integer.parseInt(maxMergeSizeMBOption);
			} catch (NumberFormatException e) {
				throw new FhirIndexException("'%s' must be a strictly positive integer", MAX_MERGE_MB_OPTION);
			}
			if (maxMergeMB <= 0) {
				throw new FhirIndexException("'%s' must be strictly positive", MAX_MERGE_MB_OPTION);
			}
			return maxMergeMB;
		} else {
			return DEFAULT_MAX_MERGE_MB;
		}
	}

	private static int parseMaxCachedMB(Map<String, String> options) {
		String maxCachedMBOption = options.get(MAX_CACHED_MB_OPTION);
		if (maxCachedMBOption != null) {
			int maxCachedMB;
			try {
				maxCachedMB = Integer.parseInt(maxCachedMBOption);
			} catch (NumberFormatException e) {
				throw new FhirIndexException("'%s' must be a strictly positive integer", MAX_CACHED_MB_OPTION);
			}
			if (maxCachedMB <= 0) {
				throw new FhirIndexException("'%s' must be strictly positive", MAX_CACHED_MB_OPTION);
			}
			return maxCachedMB;
		} else {
			return DEFAULT_MAX_CACHED_MB;
		}
	}

	private static int parseIndexingThreads(Map<String, String> options) {
		String indexPoolNumQueuesOption = options.get(INDEXING_THREADS_OPTION);
		if (indexPoolNumQueuesOption != null) {
			try {
				return Integer.parseInt(indexPoolNumQueuesOption);
			} catch (NumberFormatException e) {
				throw new FhirIndexException("'%s' must be a positive integer", INDEXING_THREADS_OPTION);
			}
		} else {
			return DEFAULT_INDEXING_THREADS;
		}
	}

	private static int parseIndexingQueuesSize(Map<String, String> options) {
		String indexPoolQueuesSizeOption = options.get(INDEXING_QUEUES_SIZE_OPTION);
		if (indexPoolQueuesSizeOption != null) {
			int indexingQueuesSize;
			try {
				indexingQueuesSize = Integer.parseInt(indexPoolQueuesSizeOption);
			} catch (NumberFormatException e) {
				throw new FhirIndexException("'%s' must be a strictly positive integer", INDEXING_QUEUES_SIZE_OPTION);
			}
			if (indexingQueuesSize <= 0) {
				throw new FhirIndexException("'%s' must be strictly positive", INDEXING_QUEUES_SIZE_OPTION);
			}
			return indexingQueuesSize;
		} else {
			return DEFAULT_INDEXING_QUEUES_SIZE;
		}
	}

	private static int parseSearchCacheSize(Map<String, String> options) {
		String searchCacheSizeOption = options.get(SEARCH_CACHE_SIZE_OPTION);
		if (searchCacheSizeOption != null) {
			int searchCacheSize;
			try {
				searchCacheSize = Integer.parseInt(searchCacheSizeOption);
			} catch (NumberFormatException e) {
				throw new FhirIndexException("'%s' must be a positive integer", SEARCH_CACHE_SIZE_OPTION);
			}
			if (searchCacheSize < 0) {
				throw new FhirIndexException("'%s' must be positive", SEARCH_CACHE_SIZE_OPTION);
			}
			return searchCacheSize;
		} else {
			return DEFAULT_SEARCH_CACHE_SIZE;
		}
	}

	private static Path parsePath(Map<String, String> options, CFMetaData tableMetadata, IndexMetadata indexMetadata) {
		String pathOption = options.get(DIRECTORY_PATH_OPTION);
		if (pathOption != null) {
			return Paths.get(pathOption);
		} else if (indexMetadata != null) {
			Directories directories = new Directories(tableMetadata);
			String basePath = directories.getDirectoryForNewSSTables().getAbsolutePath();
			return Paths.get(basePath + File.separator + INDEXES_DIR_NAME + File.separator + indexMetadata.name);
		}
		return null;
	}

	private static ResourceOptions parseSearchOptions(Map<String, String> options) {
		String searchOption = options.get(SEARCH_OPTION);
		if (searchOption != null && !searchOption.trim().isEmpty()) {
			ResourceOptions searchOptions;
			try {
				searchOptions = JsonSerializer.fromString(searchOption, ResourceOptions.class);
				return searchOptions;
			} catch (Exception e) {
				throw new FhirIndexException(e, "'%s' is invalid : %s", SEARCH_OPTION, e.getMessage());
			}
		} else {
			throw new FhirIndexException("'%s' required", SEARCH_OPTION);
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("IndexOptions [path=");
		builder.append(path);
		builder.append(", resourceTypeColumn=");
		builder.append(resourceTypeColumn);
		builder.append(", refreshSeconds=");
		builder.append(refreshSeconds);
		builder.append(", ramBufferMB=");
		builder.append(ramBufferMB);
		builder.append(", maxMergeMB=");
		builder.append(maxMergeMB);
		builder.append(", maxCachedMB=");
		builder.append(maxCachedMB);
		builder.append(", indexingThreads=");
		builder.append(indexingThreads);
		builder.append(", indexingQueuesSize=");
		builder.append(indexingQueuesSize);
		builder.append(", searchCacheSize=");
		builder.append(searchCacheSize);
		builder.append(", targetColumn=");
		builder.append(targetColumn);
		builder.append("]");
		return builder.toString();
	}

}
