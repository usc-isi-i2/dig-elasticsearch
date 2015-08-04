package edu.isi.dig.elasticsearch;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchScroll;
import io.searchbox.params.Parameters;
import io.searchbox.params.SearchType;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ScanAndScroll {
	
	private static Logger LOG = LoggerFactory.getLogger(ScanAndScroll.class);
	private final JestClient client;
	private final static String SCROLL = "5m";
	private PrintWriter writer;
	
	
	public static void main(String args[]){
		Options options = createCommandLineOptions();
		CommandLine cl = parse(args, options, ScanAndScroll.class.getSimpleName());
		if(cl == null)
		{
			return;
		}
		
		
		String esHostName ;
		String esPort;
		String esProtocol;
		String esUserName;
		String esPassword;
		int pageSize;
		String esQuery;
		
		
		
		if (cl.hasOption("eshostname")){
			esHostName = (String) cl.getOptionValue("eshostname");
		}else{
			esHostName = "localhost";
		}
		
		if(cl.hasOption("esport")){
			esPort = (String) cl.getOptionValue("esport");
		}else{
			esPort = "9200";
		}
		
		if(cl.hasOption("esprotocol")){
			esProtocol = (String) cl.getOptionValue("esprotocol");
		}else{
			esProtocol = "http";
		}

		if(cl.hasOption("esusername")){
			esUserName = (String) cl.getOptionValue("esusername");
		}else{
			esUserName = "";
		}

		if(cl.hasOption("espassword")){
			esPassword = (String) cl.getOptionValue("espassword");
		}else{
			esPassword = "";
		}
		
		if(cl.hasOption("pagesize")){
			pageSize = Integer.parseInt(cl.getOptionValue("pagesize"));
		}else{
			pageSize = 25; //lucky number?
		}
		
		if(cl.hasOption("esquery")){
			esQuery = (String) cl.getOptionValue("esquery");
		}else{
			esQuery = "{\"query\" : {\"match_all\" : {}}}"; //get everything
		}

		String esIndex = (String) cl.getOptionValue("esindex");
		String esDocType = (String) cl.getOptionValue("esdoctype");
		String outPutFilePath = (String) cl.getOptionValue("outputfile");
		
		
		String url = esProtocol + "://" + esHostName + ":" + esPort;
		
		ScanAndScroll sas;
		try {
			sas = new ScanAndScroll(url, esUserName, esPassword,outPutFilePath);
			sas.executeQuery(esQuery, pageSize, esIndex, esDocType);
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			LOG.error("Error executing query:" + e);
		}
		
		
		
	}
	
	private static Options createCommandLineOptions() {
		Options options = new Options();
		
		options.addOption(new Option("esprotocol", "esprotocol",true, "http or https"));
		options.addOption(new Option("esindex", "esindex",true, "elasticsearch index name"));
		options.addOption(new Option("esdoctype", "esdoctype",true, "elasticsearch doc type"));
		options.addOption(new Option("esusername", "esusername",true, "elasticsearch username"));
		options.addOption(new Option("espassword", "espassword",true, "elasticsearch password"));
		options.addOption(new Option("esport", "esport",true, "elasticsearch port"));
		options.addOption(new Option("eshostname", "eshostname",true, "elasticsearch hostname"));
		options.addOption(new Option("esquery","esquery",true,"elasticsearch query"));
		options.addOption(new Option("pagesize", "pagesize", true,"number of documents per shard to get at one time"));
		options.addOption(new Option("outputfile","outputfile",true,"output file path"));

		return options;
	}
	
	public static CommandLine parse(String args[], Options options, String commandName)
	{
		CommandLineParser parser = new BasicParser();
		 CommandLine cl = null;
		try {
			/**
			 * PARSE THE COMMAND LINE ARGUMENTS *
			 */
	        cl = parser.parse(options, args);
	        if (cl == null || cl.getOptions().length == 0 || cl.hasOption("help")) {
	        	HelpFormatter hf = new HelpFormatter();
	        	hf.printHelp(commandName, options);
	            return null;
	        }
		
	} catch (Exception e) {
		LOG.error("Error occured while parsing arguments!", e);
		return cl;
	}
		return cl;
	}
	
	
	public ScanAndScroll(String url,String username, String password,String outputFilePath) throws FileNotFoundException, UnsupportedEncodingException{
		SSLContextBuilder builder = new SSLContextBuilder();
		SSLConnectionSocketFactory sslsf=null;
		try {
			builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
			sslsf = new SSLConnectionSocketFactory(builder.build());
		} catch(Exception e)
		{
			LOG.error(e.getMessage());
		}
		
		HttpClientConfig.Builder httpClientBuilder =  new HttpClientConfig.Builder(url)
																.sslSocketFactory(sslsf)
																.readTimeout(30000)
																.multiThreaded(false);
		
		if(username.trim() != "" && password.trim() != ""){
			httpClientBuilder.defaultCredentials(username, password);
		}
		
		JestClientFactory jcf = new JestClientFactory();
		
		jcf.setHttpClientConfig(httpClientBuilder.build());
		
		this.client = jcf.getObject();
		
		this.writer = new PrintWriter(outputFilePath, "UTF-8");
	}
	
	
	public void executeQuery(String query, int pageSize,String index, String docType){
		
		Search search = new Search.Builder(query)
									 .addIndex(index)
									 .addType(docType)
									 .setParameter(Parameters.SEARCH_TYPE,SearchType.SCAN)
									 .setParameter(Parameters.SIZE, pageSize)
									 .setParameter(Parameters.SCROLL, SCROLL)
									 .build();
		
		try {

            JestResult searchResult = client.execute(search);
            String scrollId = searchResult.getJsonObject().get("_scroll_id").getAsString();

            int currentResultSize = 0;
            int currentPage = 1;
            do {

                SearchScroll scrollRequest = new SearchScroll.Builder(scrollId, SCROLL)
                                                    .setParameter(Parameters.SIZE, pageSize)
                                                    .build();

                JestResult scrollResult = client.execute(scrollRequest);
                scrollId = scrollResult.getJsonObject().get("_scroll_id").getAsString();
                
                JSONObject jObj =  (JSONObject) JSONSerializer.toJSON(scrollResult.toString());
                
                JSONArray jArrayHits = jObj.getJSONObject("hits").getJSONArray("hits");
                
                for(int i=0;i<jArrayHits.size();i++){
                	
                	writer.println(jArrayHits.getString(i));
                }

                // Note: Current result size will be Page Size * number of shards
                currentResultSize = jArrayHits.size();
                currentPage++;

            } while (currentResultSize != 0);
            
            
            writer.close();

        } catch (IOException e) {
            LOG.error("Error retrieving from Elasticsearch", e);
        }
	}

}
