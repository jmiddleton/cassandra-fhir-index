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

package io.puntanegra.fhir.index.mapper;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;

/**
 * Class for several token mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class TokenMapper {

	/** The Lucene field name */
	static final String FIELD_NAME = "_token";

	/** The Lucene field type */
	static final FieldType FIELD_TYPE = new FieldType();

	static {
		FIELD_TYPE.setTokenized(true);
		FIELD_TYPE.setOmitNorms(true);
		FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
		FIELD_TYPE.setNumericType(FieldType.NumericType.LONG);
		FIELD_TYPE.setDocValuesType(DocValuesType.NUMERIC);
		FIELD_TYPE.freeze();
	}

	/**
	 * Adds to the specified {@link Document} the {@link Field}s associated to
	 * the token of the specified row key.
	 *
	 * @param document
	 *            a {@link Document}
	 * @param key
	 *            the raw partition key to be added
	 */
	public void addFields(Document document, DecoratedKey key) {
		Token token = key.getToken();
		Long value = value(token);
		Field field = new LongField(FIELD_NAME, value, FIELD_TYPE);
		document.add(field);
	}

	/**
	 * Returns the {code Long} value of the specified Murmur3 partitioning
	 * {@link Token}.
	 *
	 * @param token
	 *            a Murmur3 token
	 * @return the {@code token}'s {code Long} value
	 */
	public static Long value(Token token) {
		return (Long) token.getTokenValue();
	}

	/**
	 * Returns the {code ByteBuffer} value of the specified Murmur3 partitioning
	 * {@link Token}.
	 *
	 * @param token
	 *            a Murmur3 token
	 * @return the {@code token}'s {code ByteBuffer} value
	 */
	public static ByteBuffer byteBuffer(Token token) {
		return LongType.instance.decompose(value(token));
	}

	/**
	 * Returns the {@link BytesRef} indexing value of the specified Murmur3
	 * partitioning {@link Token}.
	 *
	 * @param token
	 *            a Murmur3 token
	 * @return the {@code token}'s indexing value
	 */
	private static BytesRef bytesRef(Token token) {
		Long value = value(token);
		BytesRefBuilder bytesRef = new BytesRefBuilder();
		NumericUtils.longToPrefixCoded(value, 0, bytesRef);
		return bytesRef.get();
	}

	/**
	 * Returns a Lucene {@link SortField} for sorting documents/rows according
	 * to the partitioner's order.
	 *
	 * @return a sort field for sorting by token
	 */
	public SortField sortField() {
		return new SortField(FIELD_NAME, SortField.Type.LONG);
	}

	/**
	 * Returns if the specified lower partition position must be included in a
	 * filtered range.
	 *
	 * @param position
	 *            a {@link PartitionPosition}
	 * @return {@code true} if {@code position} must be included, {@code false}
	 *         otherwise
	 */
	public boolean includeStart(PartitionPosition position) {
		return position.kind() == PartitionPosition.Kind.MIN_BOUND;
	}

	/**
	 * Returns if the specified upper partition position must be included in a
	 * filtered range.
	 *
	 * @param position
	 *            a {@link PartitionPosition}
	 * @return {@code true} if {@code position} must be included, {@code false}
	 *         otherwise
	 */
	public boolean includeStop(PartitionPosition position) {
		return position.kind() == PartitionPosition.Kind.MAX_BOUND;
	}

	/**
	 * Returns a Lucene {@link Query} to find the {@link Document}s containing
	 * the specified {@link Token}.
	 *
	 * @param token
	 *            the token
	 * @return the query to find the documents containing {@code token}
	 */
	public Query query(Token token) {
		return new TermQuery(new Term(FIELD_NAME, bytesRef(token)));
	}
}
