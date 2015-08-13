package edu.isi.dig.elasticsearch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
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
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;


public class ScanAndScroll {
	
	private static Logger LOG = LoggerFactory.getLogger(ScanAndScroll.class);
	private final JestClient client;
	private final static String SCROLL = "5m";
	private int outputType=0;
	private String outputFile;
	
	
	public static void main(String args[]) throws IOException{
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
		String esQueryFile=null;
		String esQuery;
		int docLimit;
		int outputtype;
		
		if(cl.hasOption("outputtype")){
			outputtype = Integer.parseInt(cl.getOptionValue("outputtype"));
		}else{
			outputtype = 0; //create json array by default
		}
		
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
			esQueryFile = (String) cl.getOptionValue("esquery");
		}
		
		if(esQueryFile == null){
			esQuery = "{\"query\" : {\"match_all\" : {}}}"; //get everything
		}else{
			BufferedReader brQuery = new BufferedReader(new FileReader(new File(esQueryFile)));
			String line = null;
			StringBuilder sbQuery = new StringBuilder();
			while((line = brQuery.readLine()) != null){
				sbQuery.append(line.trim());
			}
			esQuery = sbQuery.toString();
			brQuery.close();
			
		}
		
		if(cl.hasOption("doclimit")){
			docLimit = Integer.parseInt(cl.getOptionValue("doclimit"));
		}else{
			docLimit = 100; //limit the scroller to get only 100 documents out of the potential trillions
		}
		String esIndex = (String) cl.getOptionValue("esindex");
		String esDocType = (String) cl.getOptionValue("esdoctype");
		String outPutFilePath = (String) cl.getOptionValue("outputfile");
		
		
		
		
		String url = esProtocol + "://" + esHostName + ":" + esPort;
		
		ScanAndScroll sas;
		try {
			sas = new ScanAndScroll(url, esUserName, esPassword,outPutFilePath,outputtype,outPutFilePath);
			sas.executeQuery(esQuery, pageSize, esIndex, esDocType,docLimit);
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
		options.addOption(new Option("esquery","esquery",true,"elasticsearch query file"));
		options.addOption(new Option("pagesize", "pagesize", true,"number of documents per shard to get at one time"));
		options.addOption(new Option("outputfile","outputfile",true,"output file path"));
		options.addOption(new Option("doclimit","doclimit",true, "number of documents retrieved, -1 to get trillion"));
		options.addOption(new Option("outputtype","outputtype",true,"0 for json array, 1 for json lines"));

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
	
	
	public ScanAndScroll(String url,String username, String password,String outputFilePath,int outputType,String outputFile) throws FileNotFoundException, UnsupportedEncodingException{
		SSLContextBuilder builder = new SSLContextBuilder();
		SSLConnectionSocketFactory sslsf=null;
		try {
			builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
			sslsf = new SSLConnectionSocketFactory(builder.build());
		} catch(Exception e)
		{
			LOG.error(e.getMessage());
		}
		System.out.println(url);
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
		
		
		this.outputType = outputType;
		this.outputFile = outputFile;
	}
	
	
private JSONObject extractTika(String contents){
		
		JSONObject jObj = (JSONObject)JSONSerializer.toJSON(contents);
		
		if(jObj.containsKey("_source"))
		{
			JSONObject jObjSource = jObj.getJSONObject("_source");
			
			if(jObjSource.containsKey("raw_content"))
			{
				String rawHtml = jObjSource.getString("raw_content");
				
				ByteArrayInputStream bIs = new ByteArrayInputStream(rawHtml.getBytes());
				
				Metadata metadata = new Metadata();
				
				AutoDetectParser adp = new AutoDetectParser();
				
				ContentHandler handler = new BodyContentHandler(10*1024*1024);
				
				
				
				try {
					adp.parse(bIs, handler, metadata);
					
					String[] metadataNames = metadata.names();
					
					
					JSONObject jObjMetadata = new JSONObject();
					
					for(String metadataName:metadataNames)
					{
						String[] values = metadata.getValues(metadataName);
						
						JSONArray jArray = new JSONArray();
						for(String mValue: values)
						{
							jArray.add(mValue);
						}
						
						jObjMetadata.accumulate(metadataName, jArray);
						
					}
					
					//remove empty lines from the text
					String rawTextAdjusted = handler.toString().replaceAll("(?m)^[ \t]*\r?\n", "");
					
					//detect language
					LanguageIdentifier li = new LanguageIdentifier(rawTextAdjusted);
					
					jObjSource.accumulate("tikametadata", jObjMetadata);
					jObjSource.accumulate("raw_text", rawTextAdjusted);
					jObjSource.accumulate("rawtextdetectedlanguage", li.getLanguage());
					
				} catch (Exception e) {
					LOG.error("Error:",e);;
				}
				
			}
			
		}
		return jObj;
}
	
	
	public void executeQuery(String query, int pageSize,String index, String docType,int docLimit){
		
		Search search = new Search.Builder(query)
									 .addIndex(index)
									 .addType(docType)
									 .setParameter(Parameters.SEARCH_TYPE,SearchType.SCAN)
									 .setParameter(Parameters.SIZE, pageSize)
									 .setParameter(Parameters.SCROLL, SCROLL)
									 .build();
		System.out.println(query + "$$$$");
		
		try {

            JestResult searchResult = client.execute(search);
            //System.out.println(searchResult.getJsonString());
            String scrollId = searchResult.getJsonObject().get("_scroll_id").getAsString();

            int currentResultSize = 0;
            int numDocs = 0;
            
            do {
            	JSONArray jArrayResult = new JSONArray();
            	
                SearchScroll scrollRequest = new SearchScroll.Builder(scrollId, SCROLL)
                                                    .setParameter(Parameters.SIZE, pageSize)
                                                    .build();

                JestResult scrollResult = client.execute(scrollRequest);
                scrollId = scrollResult.getJsonObject().get("_scroll_id").getAsString();
                
                JSONObject jObj =  (JSONObject) JSONSerializer.toJSON(scrollResult.getJsonString());
                
                JSONArray jArrayHits = jObj.getJSONObject("hits").getJSONArray("hits");
                
                for(int i=0;i<jArrayHits.size();i++){
                	
                	jArrayResult.add(extractTika(jArrayHits.getString(i)).toString());
                }
                writeToFile(jArrayResult);
                // Note: Current result size will be Page Size * number of shards
                currentResultSize = jArrayHits.size();
                numDocs+=currentResultSize;
                System.out.println("num docs:" + String.valueOf(numDocs));
                if(docLimit != -1 && numDocs >= docLimit){
                	break;
                }
            } while (currentResultSize != 0);
            
           
        } catch (IOException e) {
            LOG.error("Error retrieving from Elasticsearch", e);
        }
	}
	
	private void writeToFile(JSONArray jArray){
		
		try{
			
			FileWriter fw = new FileWriter(outputFile, true);
			BufferedWriter writer = new BufferedWriter(fw);
			if(outputType == 0){
				 writer.write(jArray.toString());
			}else if(outputType == 1){
				for(int i=0;i<jArray.size();i++){
					writer.write(jArray.getJSONObject(i).getJSONObject("_source").getString("url").trim() + "\t" + jArray.getString(i));
					writer.newLine();
				}
			}
			writer.close();
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
		
		
	}
}
