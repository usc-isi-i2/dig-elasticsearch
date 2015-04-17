package edu.isi.dig.elasticsearch;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchReducer extends Reducer<Text, Text, NullWritable, NullWritable> {
	private static Logger LOG = LoggerFactory.getLogger(ElasticSearchReducer.class);
	private int batchSize = 1000;
	Client client;
	BulkRequestBuilder bulkRequest;
	String type;
	String[] indices;
	int count = 0;
	@Override
	public void setup(Context context) {
		String[] hostNames = context.getConfiguration().get("elasticsearch.hostname").split(",");

		int portNumber = Integer.parseInt(context.getConfiguration().get("elasticsearch.port"));
		String clusterName = context.getConfiguration().get("elasticsearch.clustername");
		try {
			batchSize = Integer.parseInt(context.getConfiguration().get("elasticsearch.batchsize"));
		} catch(Exception e) {
			batchSize = 1000;
		}
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
		type = context.getConfiguration().get("elasticsearch.type");
		indices = context.getConfiguration().get("elasticsearch.index").split(",");
		client = new TransportClient(settings);
		for (String hostName : hostNames) {
			((TransportClient) client).addTransportAddress(new InetSocketTransportAddress(hostName, portNumber));
		}
		bulkRequest = client.prepareBulk();
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Iterator<Text> itr = values.iterator();
		while (itr.hasNext()) {
			try {
				String tmp = itr.next().toString();
				JSONObject obj = new JSONObject(tmp);
				if (obj.has("uri")) {
					for (String index : indices) {
						bulkRequest.add(client.prepareIndex(index, type, obj.getString("uri")).setSource(tmp));
					}
				}
				else {
					for (String index : indices) {
						bulkRequest.add(client.prepareIndex(index, type).setSource(tmp));
					}
				}
				count++;
				if (count == batchSize) {
					bulkRequest.execute().actionGet();
					bulkRequest = client.prepareBulk();
					count = 0;
				}
			}catch(Exception e) {
				LOG.error("something is wrong", e);
				//bulkRequest = client.prepareBulk();
				count = 0;
			}
		}
		context.progress();
	}

	@Override
	public void cleanup(Context context) {
		try {
			bulkRequest.execute().actionGet();
		} catch(Exception e) {
			LOG.error("something is wrong", e);
		}
		client.close();
	}
}
