package com.solr.report;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;

/**
 * Hello world!
 *
 */
public class SolrProducer 
{
	private final static String ZK_STRING = "localhost:2181";
	private static final String SOLR_FIELD_PREFIX = "i_";

	public static void main( String[] args ) {
		CloudSolrClient solr = new CloudSolrClient.Builder().withZkHost(ZK_STRING).build();
		solr.setDefaultCollection("hyperloop.sla_stats");

		SolrInputDocument document = new SolrInputDocument();
		document.addField("id", UUID.randomUUID().toString());
		document.addField("record_date", ZonedDateTime.now(ZoneOffset.UTC).toString()); 

		try {
			@SuppressWarnings("unused")
			org.apache.solr.client.solrj.response.UpdateResponse response = solr.add(document);
			solr.commit();
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally{
			try {
				solr.close();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	public static void createRecords( List<Map<String, String>> records) {
		CloudSolrClient solr = new CloudSolrClient.Builder().withZkHost(ZK_STRING).build();
		solr.setDefaultCollection("gmp_payments.order");

		records.stream().forEach(record -> {
			SolrInputDocument document = new SolrInputDocument();
			document.addField("id", UUID.randomUUID().toString());
			document.addField("record_date", ZonedDateTime.now(ZoneOffset.UTC).toString()); 
			record.forEach( (k,v) -> {
				document.addField(SOLR_FIELD_PREFIX + k, v);
			});
			try {
				solr.add(document);
			} catch (SolrServerException | IOException e) {
				e.printStackTrace();
			}
		});
		
		try {
			solr.commit();
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
		}  finally{
			try {
				solr.close();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

}
