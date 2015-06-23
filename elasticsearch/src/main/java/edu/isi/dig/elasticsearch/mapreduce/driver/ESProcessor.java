package edu.isi.dig.elasticsearch.mapreduce.driver;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.isi.dig.elasticsearch.mapreduce.inputformat.ESInputFormat;

public class ESProcessor extends Configured implements Tool{
	
	public Job configure(Properties p) throws Exception
	{
		Configuration conf = new Configuration();
		
		if(p.getProperty("elasticsearch.hostname") != null)
		{
			//System.out.println(p.getProperty("elasticsearch.hostname"));
			//System.out.println(conf.getClass().getName());
			conf.setIfUnset("elasticsearch.hostname", p.getProperty("elasticsearch.hostname"));
		}
		
		if(p.getProperty("elasticsearch.port") != null)
		{
			conf.setIfUnset("elasticsearch.port", p.getProperty("elasticsearch.port"));
		}
		
		if(p.getProperty("elasticsearch.batchsize") != null)
		{
			conf.setIfUnset("elasticsearch.batchsize", p.getProperty("elasticsearch.batchsize"));
		}
		
		if(p.getProperty("elasticsearch.index") != null)
		{
			conf.setIfUnset("elasticsearch.index", p.getProperty("elasticsearch.index"));
		}
		
		if(p.getProperty("elasticsearch.username") != null)
		{
			conf.setIfUnset("elasticsearch.username", p.getProperty("elasticsearch.username"));
		}
		
		if(p.getProperty("elasticsearch.password") != null)
		{
			conf.setIfUnset("elasticsearch.password", p.getProperty("elasticsearch.password"));
		}
		
		if(p.getProperty("elasticsearch.starttimestamp") != null)
		{
			conf.setIfUnset("elasticsearch.starttimestamp", p.getProperty("elasticsearch.starttimestamp"));
		}
		
		if(p.getProperty("elasticsearch.endtimestamp") != null)
		{
			conf.setIfUnset("elasticsearch.endtimestamp", p.getProperty("elasticsearch.endtimestamp"));
		}
		
		if(p.getProperty("elasticsearch.protocol") != null)
		{
			conf.setIfUnset("elasticsearch.protocol", p.getProperty("elasticsearch.protocol"));
		}
		
		if(p.getProperty("elasticsearch.doctype") != null)
		{
			conf.setIfUnset("elasticsearch.doctype", p.getProperty("elasticsearch.doctype"));
		}
		
		Job job = Job.getInstance(conf);
		job.setInputFormatClass(ESInputFormat.class);
		job.setJarByClass(ESProcessor.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(ESMapper.class);
		//job.setReducerClass(JSONReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//FileInputFormat.setInputPaths(job, new Path(p.getProperty("input.directory")));
		FileOutputFormat.setOutputPath(job, new Path(p.getProperty("output.directory")));
		
		job.setNumReduceTasks(0);
		
		return job;
	}

	@Override
	public int run(String[] args) throws Exception {
		Properties p = new Properties();

		try
		{
			p.load(new FileInputStream(new File(args[0])));
		}catch(Exception e)
		{	System.out.println("Error:");
			e.printStackTrace();
		}
		 
        
        Job job = configure(p);
        
        if(!job.waitForCompletion(true))
        {
       	 System.err.println("Unable to finish job");
       	 return -1;
        }
       
		return 0;
	}
	

}
