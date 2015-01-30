package edu.isi.dig.elasticsearch;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HTTP;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;


public class BulkLoadSequenceFile {

	public static void main(String[] args) throws IllegalArgumentException, IOException, InterruptedException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		Options options = createCommandLineOptions();
		CommandLine cl = parse(args, options, BulkLoadSequenceFile.class.getSimpleName());
		if(cl == null)
		{
			return;
		}
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.TRACE);
		String filePath = (String)cl.getOptionValue("filepath");
		String index = (String)cl.getOptionValue("index");
		String type = (String)cl.getOptionValue("type");
		String hostname = (String)cl.getOptionValue("hostname");
		String clustername = (String)cl.getOptionValue("clustername");
		String sleep = (String)cl.getOptionValue("sleep");
		String bulksize = (String)cl.getOptionValue("bulksize");
		String port = (String)cl.getOptionValue("port");
		Settings settings = ImmutableSettings.settingsBuilder()
		            .put("cluster.name", clustername).build();
		
		
		 SSLContextBuilder builder = new SSLContextBuilder();
		    builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
		    SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
		            builder.build());
		    CloseableHttpClient httpClient = HttpClients.custom().setSSLSocketFactory(
		            sslsf).build();
		
		//CloseableHttpClient httpClient = HttpClients.createDefault();
		
		HttpPost httpPost = new HttpPost("https://" + hostname + ":" + port + "/" + index + "/_bulk");
		
		
		String bulkFormat = "{\"index\":{\"_index\":\"" + index+ "\",\"_type\":\""+ type +"\"}}";
		
		
		SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(new Path(filePath)));
		BytesWritable key = new BytesWritable();
		Text val = new Text();
		//Client client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(hostname, 9300));
		//BulkRequestBuilder bulkRequest = client.prepareBulk();
		StringBuilder sb = new StringBuilder();
		long counter = 0;
		while (reader.next(key, val)) {			
			//bulkRequest.add(client.prepareIndex(index, type).setSource(val.toString()));
			sb.append(bulkFormat);
			//sb.append("\n");
			sb.append(System.getProperty("line.separator"));
			sb.append(val.toString());
			sb.append(System.getProperty("line.separator"));
			//sb.append("\n");
			
			break;
			/*counter++;
			if (counter % Integer.parseInt(bulksize) == 0) {
				System.out.println(counter + " resources processed");
				//bulkRequest.execute().actionGet();
				//bulkRequest = client.prepareBulk();
				StringEntity entity = new StringEntity(sb.toString(),"UTF-8");
				entity.setContentType("application/json");
				httpPost.setEntity(entity);
				httpClient.execute(httpPost);
				
				Thread.sleep(Integer.parseInt(sleep));
				sb.setLength(0);
				//httpClient.close();
			}
			break;
			*/
		}
		StringEntity entity = new StringEntity(sb.toString(),"UTF-8");
		entity.setContentType("application/json");
		httpPost.setEntity(entity);
		httpClient.execute(httpPost);
		httpClient.close();
		reader.close();
		//client.close();
	}
	
	private static Options createCommandLineOptions() {
		Options options = new Options();
				options.addOption(new Option("filepath", "filepath", true, "location of the input file directory"));
				options.addOption(new Option("type", "type", true, "elasticsearch type"));
				options.addOption(new Option("index", "index", true, "elasticsearch index"));
				options.addOption(new Option("hostname", "hostname", true, "elasticsearch hostname"));
				options.addOption(new Option("clustername", "clustername", true, "elasticsearch clustername"));
				options.addOption(new Option("sleep", "sleep", true, "thread sleep in ms"));
				options.addOption(new Option("bulksize", "bulksize", true, "bulk size"));
				options.addOption(new Option("port", "port", true, "es port"));

		return options;
	}
	
	public static CommandLine parse(String args[], Options options, String commandName)
	{
		CommandLineParser parser = new BasicParser();
		 CommandLine cl = null;
		try {
	        cl = parser.parse(options, args);
	        if (cl == null || cl.getOptions().length == 0 || cl.hasOption("help")) {
	        	HelpFormatter hf = new HelpFormatter();
	        	hf.printHelp(commandName, options);
	            return null;
	        }
		
	} catch (Exception e) {
		return cl;
	}
		return cl;
	}

}
