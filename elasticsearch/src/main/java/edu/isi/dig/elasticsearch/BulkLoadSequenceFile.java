package edu.isi.dig.elasticsearch;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;


public class BulkLoadSequenceFile {

	private static final int retry = 10;
	public static void main(String[] args) throws IllegalArgumentException, IOException, InterruptedException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		Options options = createCommandLineOptions();
		CommandLine cl = parse(args, options, BulkLoadSequenceFile.class.getSimpleName());
		if(cl == null)
		{
			return;
		}
		String filePath = (String)cl.getOptionValue("filepath");
		String index = (String)cl.getOptionValue("index");
		String type = (String)cl.getOptionValue("type");
		String hostname = (String)cl.getOptionValue("hostname");
		String sleep = (String)cl.getOptionValue("sleep");
		String bulksize = (String)cl.getOptionValue("bulksize");
		String port = (String)cl.getOptionValue("port");
		String protocol = (String)cl.getOptionValue("protocol");


		SSLContextBuilder builder = new SSLContextBuilder();
		builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
		SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build());

		CloseableHttpClient httpClient = null;

		if(protocol.equalsIgnoreCase("https"))
			httpClient = HttpClients.custom().setSSLSocketFactory(sslsf).build();
		else if(protocol.equalsIgnoreCase("http"))
			httpClient = HttpClients.createDefault();

		HttpPost httpPost = new HttpPost(protocol+"://" + hostname + ":" + port + "/" + index + "/_bulk");
		String bulkFormat = null;

		SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(new Path(filePath)));
		Writable key = (Writable) Class.forName(reader.getKeyClass().getCanonicalName()).newInstance();
		Text val = new Text();
		StringBuilder sb = new StringBuilder();
		long counter = 0;
		while (reader.next(key, val)) {	
			JSONObject jObj = (JSONObject)JSONSerializer.toJSON(val.toString());
			
			String id = null;
			
			if(jObj.containsKey("uri"))
			{
				id = jObj.getString("uri");
			}
			
			if(id != null)
			{
				bulkFormat = "{\"index\":{\"_index\":\"" + index+ "\",\"_type\":\""+ type +"\",\"_id\":\""+id+"\"}}";
			}
			else
			{
				bulkFormat = "{\"index\":{\"_index\":\"" + index+ "\",\"_type\":\""+ type +"\"}}";
			}
			sb.append(bulkFormat);
			sb.append(System.getProperty("line.separator"));
			sb.append(val.toString());
			sb.append(System.getProperty("line.separator"));
			counter++;
			if (counter % Integer.parseInt(bulksize) == 0) {
				int i = 0;
				Exception ex = null;
				while (i < retry) {
					try {
						StringEntity entity = new StringEntity(sb.toString(),"UTF-8");
						entity.setContentType("application/json");
						httpPost.setEntity(entity);
						httpClient.execute(httpPost);
						httpClient.close();
						System.out.println(counter + " processed");
						break;
					}catch(Exception e) {
						ex = e;
						i++;
					}
				}
				if (i > 0) {
					System.out.println("Exception occurred!");
					ex.printStackTrace();
					break;
				}
				httpClient = null;

				if(protocol.equalsIgnoreCase("https"))
					httpClient = HttpClients.custom().setSSLSocketFactory(sslsf).build();
				else if(protocol.equalsIgnoreCase("http"))
					httpClient = HttpClients.createDefault();

				httpPost = new HttpPost(protocol + "://" + hostname + ":" + port + "/" + index + "/_bulk");
				sb = new StringBuilder();
				Thread.sleep(Integer.parseInt(sleep));
			}
		}
		StringEntity entity = new StringEntity(sb.toString(),"UTF-8");
		entity.setContentType("application/json");
		httpPost.setEntity(entity);
		httpClient.execute(httpPost);
		httpClient.close();
		reader.close();
	}

	private static Options createCommandLineOptions() {
		Options options = new Options();
		options.addOption(new Option("filepath", "filepath", true, "location of the input file directory"));
		options.addOption(new Option("type", "type", true, "elasticsearch type"));
		options.addOption(new Option("index", "index", true, "elasticsearch index"));
		options.addOption(new Option("hostname", "hostname", true, "elasticsearch hostname"));
		options.addOption(new Option("sleep", "sleep", true, "thread sleep in ms"));
		options.addOption(new Option("bulksize", "bulksize", true, "bulk size"));
		options.addOption(new Option("port", "port", true, "es port"));
		options.addOption(new Option("protocol", "protocol", true, "es handshake protocol"));

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
