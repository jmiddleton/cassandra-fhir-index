package io.puntanegra.fhir.index.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Stores the default Lucene analyzer and search resources defined during index
 * creation.
 * 
 * @author Jorge L. Middleton {@literal <jorge.middleton@gmail.com>}
 *
 */
public class ResourceOptions {

	/** The wrapping all-in-one {@link Analyzer}. */
	@JsonProperty("default_analyzer")
	public final Analyzer defaultAnalyzer;

	@JsonProperty("resources")
	public final Map<String, Set<String>> resources = new HashMap<String, Set<String>>();

	@JsonCreator
	public ResourceOptions(@JsonProperty("default_analyzer") String analyzer,
			@JsonProperty("resources") Map<String, Set<String>> resources)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		if (analyzer == null) {
			this.defaultAnalyzer = new WhitespaceAnalyzer();
		} else {
			this.defaultAnalyzer = (Analyzer) Class.forName(analyzer).newInstance();
		}

		if (resources != null) {
			this.resources.putAll(resources);
		}
	}

}
