package com.solr.report;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvToJsonConverter {
	
	private static String filePath; 
	public static void main(String[] args){
		getInput(args);
		CsvToJsonConverter reader = new CsvToJsonConverter();
		List<Map<String, String>> records = reader.read();
		SolrProducer.createRecords(records);
	}
	
	private static void getInput(String[] args) {
		if(args.length<1){
			System.out.println("invalid usage. please enter the report file path");
			System.exit(-1);
		}
		filePath = args[0];
	}

	public List<Map<String, String>> read(){
		Path path = Paths.get(filePath);
		List<Map<String, String>> records = new ArrayList<>();
		try {
			BufferedReader br = Files.newBufferedReader(path);
			String line = null;
			if((line = br.readLine()) != null){
				String[] headers = line.split(",");
				int fieldCount = headers.length;
				while((line = br.readLine()) != null){
					Map<String, String> map = new HashMap<>();
					String[] fields = line.split(",");
					assert(fieldCount==fields.length);
					for(int i=0;i<fieldCount;i++){
						map.put(headers[i], fields[i]);
					}
					records.add(map);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return records;
	}
}
