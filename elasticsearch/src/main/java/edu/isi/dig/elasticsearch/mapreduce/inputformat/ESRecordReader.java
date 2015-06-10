package edu.isi.dig.elasticsearch.mapreduce.inputformat;

import java.io.IOException;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESRecordReader extends RecordReader<Writable, Writable>{

	private static Logger LOG = LoggerFactory.getLogger(ESRecordReader.class);
	
	private String esHost=null;
	private String esPort=null;
	private String esIndex=null;
	private String esUser=null;
	private String esPassword = null;
	private String startTimestamp=null;
	private String endTimeStamp=null;
	private String batchSize=null;
	private String esProtocol=null;
	private JSONArray results = null;
	private int resultsIndex = -1;
	private int totalHits=-1;
	private int fromIndex=0;
	CloseableHttpClient httpClient = null;
	HttpPost httpPost=null;
	
	@Override
	public void close() throws IOException {
		httpClient.close();
	}

	@Override
	public Writable getCurrentKey() throws IOException, InterruptedException {
	
		return NullWritable.get();
	}

	@Override
	public Writable getCurrentValue() throws IOException, InterruptedException {
	
		//TODO increment number of hits processed
		if(results != null && resultsIndex < results.size())
		{
			return new Text(results.get(resultsIndex).toString());
		}
		return null;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// TODO calculate based on count from query result
		return 0;
	}

	@Override
	public void initialize(InputSplit rawSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
			
			Configuration jobConfig = context.getConfiguration();
			esHost = jobConfig.get("elasticsearch.hostname");
			esPort = jobConfig.get("elasticsearch.port");
			batchSize = jobConfig.get("elasticsearch.batchsize");
			esIndex = jobConfig.get("elasticsearch.index");
			esUser = jobConfig.get("elasticsearch.username");
			esPassword = jobConfig.get("elasticsearch.password");
			startTimestamp= jobConfig.get("elasticsearch.starttimestamp");
			endTimeStamp = jobConfig.get("elasticsearch.endtimestamp");
			esProtocol = jobConfig.get("elasticsearch.protocol");
			
			
			SSLContextBuilder builder = new SSLContextBuilder();
			SSLConnectionSocketFactory sslsf=null;
			try {
				builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
				sslsf = new SSLConnectionSocketFactory(builder.build());
			} catch(Exception e)
			{
				LOG.error(e.getMessage());
			}
			

			if(esProtocol.equalsIgnoreCase("https"))
				httpClient = HttpClients.custom().setSSLSocketFactory(sslsf).build();
			else if(esProtocol.equalsIgnoreCase("http"))
				httpClient = HttpClients.createDefault();
			
			httpPost = new HttpPost(esProtocol+"://" + esUser + ":" + esPassword + "@" + esHost + ":" + esPort + "/" + esIndex + "/_search");
			
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
	
		if(results == null || resultsIndex >= results.size() - 1)
		{
			
			if(fromIndex <= totalHits || totalHits == -1)
			{
				String esQuery = "{ "+
									"\"query\": {" +
									"\"range\" : {" +
									"\"timestamp\" : {" +
										"\"gte\" :" +  startTimestamp + "," + 
										"\"lte\" : "+ endTimeStamp + "}}}," +
										"\"size\": " + batchSize + ","+
										"\"from\": " + String.valueOf(fromIndex) + "," +
										"\"sort\": [{" + "\"_uid\": {" + "\"order\":" + "\"asc\"}}] " +
										"}";
				
				
				if(httpPost.getEntity() != null)
				{
					httpPost.setEntity(null);
				}
				
		
				StringEntity entity = new StringEntity(esQuery,"UTF-8");
				entity.setContentType("application/json");
				httpPost.setEntity(entity);
				
				HttpResponse httpResp = httpClient.execute(httpPost);
				
				if(httpResp.getStatusLine().getStatusCode() == 200)
				{
					String resultEntity = EntityUtils.toString(httpResp.getEntity());
					JSONObject termQueryResponse = (JSONObject) JSONSerializer.toJSON(resultEntity);
					//TODO save off total hits count to see if we need to page
					if(termQueryResponse.containsKey("hits")) 
					{
						JSONObject jHitsObject = termQueryResponse.getJSONObject("hits");
						
						totalHits = Integer.parseInt(jHitsObject.get("total").toString());
						fromIndex+=Integer.parseInt(batchSize);
						if(jHitsObject.containsKey("hits")) 
						{
							results = jHitsObject.getJSONArray("hits");
							resultsIndex = 0;
							return true;
						}
						
					}
				}
				else
				{
					LOG.error("Unable to complete query: "+ httpResp.getStatusLine().getReasonPhrase());
				}
			}
		}
		else if(resultsIndex < results.size()-1) {
			resultsIndex++;
			return true;
		}
		return false;
	}

}
