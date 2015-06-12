package edu.isi.dig.elasticsearch.mapreduce.inputformat;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.lang.reflect.Field;
import java.nio.charset.Charset;

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
	
	
	static {
		try {
			Field defaultCharsetField = Charset.class.getDeclaredField("defaultCharset");
			defaultCharsetField.setAccessible(true);
			defaultCharsetField.set(null, Charset.forName("UTF-8"));
		} catch (Exception e) {
			LOG.error("something wrong", e);
		}
	}
		
	@Override
	public RecordReader<Writable,Writable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
			RecordReader<Writable, Writable> recordReader = 
				 new ESRecordReader();
				recordReader.initialize(split, context);
				return recordReader;
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
