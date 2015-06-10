package edu.isi.dig.elasticsearch;

import edu.isi.dig.elasticsearch.mapreduce.driver.ESProcessor;

public class Main {

	public static void main(String[] args) throws Exception {
		
		ESProcessor esp = new ESProcessor();
		esp.run(args);

	}

}
