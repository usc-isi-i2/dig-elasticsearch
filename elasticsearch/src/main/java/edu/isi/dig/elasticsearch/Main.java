package edu.isi.dig.elasticsearch;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.json.JSONObject;



public class Main {
	
	

	public static void main(String[] args) throws Exception {
		
		String hostName = "localhost";

		int portNumber = 9300;
		String clusterName = "elasticsearch";
		int batchSize = 100;

		
		String type = "WebPage";
		String indices = "dig-latest";
		Client client = TransportClient.builder().build();
		
		
		((TransportClient) client).addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(hostName, portNumber)));
		
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		
		
		for (String line : Files.readAllLines(Paths.get("/tmp/geonames/input/input.jl"), Charset.defaultCharset())) {

			JSONObject obj = new JSONObject(line);
			if (obj.has("uri")) {
				bulkRequest.add(client.prepareIndex(indices, type, obj.getString("uri")).setSource(line));
			}
			else {
				bulkRequest.add(client.prepareIndex(indices, type).setSource(line));
			}
			
			
			
			
			}
		BulkResponse br = bulkRequest.get();
		if(br.hasFailures()){
			System.out.println(br.buildFailureMessage());
		}
		bulkRequest = client.prepareBulk();
		
		client.close();
		
		}
		



}



