package io.puntanegra.fhir.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.junit.Test;

import io.puntanegra.fhir.index.config.ResourceOptions;
import io.puntanegra.fhir.index.util.JsonSerializer;

public class SearchOptionsTest {

	@Test
	public void testConfiguration() {
		//@formatter:off
		String json = "{" +
                "  \"default_analyzer\" : \"org.apache.lucene.analysis.en.EnglishAnalyzer\"," +
                "  \"resources\" : {"+
			    "     \"Patient\" : [\"family\", \"email\"],"+
			    "     \"Observation\" : [\"code\", \"value-quantity\"]"+
			    "  }"+
                "}";
		//@formatter:on

		ResourceOptions options;
		try {
			options = JsonSerializer.fromString(json, ResourceOptions.class);

			Analyzer defaultAnalyzer = options.defaultAnalyzer;
			assertTrue("Expected english analyzer", defaultAnalyzer instanceof EnglishAnalyzer);

			Map<String, Set<String>> resources = options.resources;
			assertEquals("Expected 2", 2, resources.size());

			resources.forEach((k, v) -> {
				System.out.println(k + "=" + v);
				assertEquals(2, v.size());
				v.forEach(p -> {
					System.out.println(p);
				});
			});

		} catch (IOException e) {
			fail(e.getMessage());
		}
	}

}
