package edu.isi.dig.elasticsearch;

import edu.isi.dig.elasticsearch.mapreduce.driver.ESProcessor;
import edu.isi.dig.elasticsearch.mapreduce.inputformat.ESRecordReader;

public class Main {

	public static void main(String[] args) throws Exception {
		
		ESProcessor esp = new ESProcessor();
		esp.run(args);
		
		//ESRecordReader esr = new ESRecordReader();
		//System.out.println(esr.timestampToEpoch("2009-08-10T13:10Z"));

	}

}
