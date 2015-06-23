package edu.isi.dig.elasticsearch.mapreduce.driver;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.tika.exception.TikaException;
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;


public class ESMapper extends Mapper<Writable,Text,Text,Text>{
	
	private Text reusableKey = new Text();
	
	@Override
	public void map(Writable key,Text value, Context context) throws IOException, InterruptedException
	{
		String contents = value.toString();
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
				
				ContentHandler handler = new BodyContentHandler();
				
				
				
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
					
				} catch (SAXException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TikaException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
		
		if (jObj.containsKey("_id")) 
		{
			reusableKey.set(jObj.getString("_id"));
			context.write(reusableKey, new Text(jObj.toString()));
		}
		else
		{
			context.write(null, new Text(jObj.toString()));
		}
		
		
			
		}
	

}
