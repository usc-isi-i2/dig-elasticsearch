package edu.isi.dig.elasticsearch.mapreduce.driver;

import java.io.IOException;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class ESMapper extends Mapper<Writable,Text,Text,Text>{
	
	private Text reusableKey = new Text();
	
	@Override
	public void map(Writable key,Text value, Context context) throws IOException, InterruptedException
	{
		String contents = value.toString();
		JSONObject jObj = (JSONObject)JSONSerializer.toJSON(contents);
		
			
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
