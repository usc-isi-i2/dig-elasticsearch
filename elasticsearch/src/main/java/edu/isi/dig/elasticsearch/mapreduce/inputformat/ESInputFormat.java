package edu.isi.dig.elasticsearch.mapreduce.inputformat;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESInputFormat extends InputFormat<Writable, Writable>{

	
	private static Logger LOG = LoggerFactory.getLogger(ESInputFormat.class);
	
	@Override
	public RecordReader<Writable,Writable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
			RecordReader<Writable, Writable> recordReader = 
				 new ESRecordReader();
				recordReader.initialize(split, context);
				return recordReader;
	}
	
	
	private class ESInputSplit extends InputSplit{

		@Override
		public long getLength() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			String[] locations = new String[1];
			locations[0] = "localhost";
			return locations;
		}
		
	}
	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException,
			InterruptedException {
		List<InputSplit> splits = new LinkedList<InputSplit>();
		FileSplit split = new FileSplit(new Path("/"), 0L,0L,new String[0]);
		splits.add(split);
		return splits;
	}

}
