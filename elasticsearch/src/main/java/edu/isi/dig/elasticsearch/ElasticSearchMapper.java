package edu.isi.dig.elasticsearch;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchMapper extends Mapper<Writable, Text, Text, Text> {
	private static Logger LOG = LoggerFactory.getLogger(ElasticSearchMapper.class);
	private int batchSize = 1000;
	Client client;
	BulkRequestBuilder bulkRequest;
	String type;
	String index;
	int count = 0;
	@SuppressWarnings("resource")
	@Override
	public void setup(Context context) {
		String hostName = context.getConfiguration().get("elasticsearch.hostname");
		int portNumber = Integer.parseInt(context.getConfiguration().get("elasticsearch.port"));
		String clusterName = context.getConfiguration().get("elasticsearch.clustername");
		try {
			batchSize = Integer.parseInt(context.getConfiguration().get("elasticsearch.batchsize"));
		} catch(Exception e) {
			batchSize = 1000;
		}
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
		type = context.getConfiguration().get("elasticsearch.type");
		index = context.getConfiguration().get("elasticsearch.index");
		client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(hostName, portNumber));
		bulkRequest = client.prepareBulk();
	}
	
	@Override
	public void map(Writable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			JSONObject obj = new JSONObject(value.toString());
			if (obj.has("uri")) {
				bulkRequest.add(client.prepareIndex(index, type, obj.getString("uri")).setSource(obj.toString()));
			}
			else {
				bulkRequest.add(client.prepareIndex(index, type).setSource(obj.toString()));
			}
			count++;
			if (count == batchSize) {
				bulkRequest.execute().actionGet();
				bulkRequest = client.prepareBulk();
				count = 0;
			}
		}catch(Exception e) {
			LOG.error("something is wrong", e);
		}
	}
	
	@Override
	public void cleanup(Context context) {
		try {
			bulkRequest.execute().actionGet();
		} catch(Exception e) {
			LOG.error("something is wrong", e);
		}
	}
}
