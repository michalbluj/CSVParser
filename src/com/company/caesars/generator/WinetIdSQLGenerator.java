package com.company.caesars.generator;

import java.io.FileReader;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.company.caesars.generator.concurrent.ConcurrentInsert;

public class WinetIdSQLGenerator extends SQLGeneratorBase implements SQLGenerator {

    private static final String [] FILE_HEADER_MAPPING = {"winet_id","c_tier_cd","c_prev_tier_cd","d_prev_tier_dt","d_prev_tier_assign_dt","c_how_assigned","d_expire_dt","c_reason_cd","c_logon","c_logon_prop_cd","d_timestamp"};

    private static final String SQLSEPARATOR = ",";

    private String readFilePath = "C://Users//Micha³ Bluj//Desktop//Analysis//gst_tier_14.csv";
    //private String readFilePath = "data/devst.caesars.comTest2/2017-07-17_noCR/guest2__01.txt";
    
    private String insertStatement = "INSERT INTO report.tier_winets (\"winetId\") VALUES";

    Map<Integer, Connection> conPool = new HashMap<Integer, Connection>();

    public WinetIdSQLGenerator() {
    }
    
    public WinetIdSQLGenerator(String file) {
    	readFilePath = file;
    }
    
    public void insertRecordsToDatabase() throws Exception{

    	Long start = System.currentTimeMillis();
    	System.out.println("Parsing " + readFilePath);
    	
    	System.out.println("Start CSVParser : " + (System.currentTimeMillis() - start));
        CSVFormat csvFileFormat =  CSVFormat.newFormat('|').withHeader(FILE_HEADER_MAPPING);
        FileReader fileReader = new FileReader(readFilePath);
        CSVParser csvFileParser = new CSVParser(fileReader, csvFileFormat);

        //list of 500k records
        
        List<CSVRecord> csvRecords = csvFileParser.getRecords();
        System.out.println("End CSVParser : " + (System.currentTimeMillis() - start));

        Integer connectionPool = 10;
        Integer jobPool = 1000;

        //Allocate same # of Active Thread as in connection.
        //will submit 1000 jobs but only 50 active at a time to avoid connection Pool locking
        ExecutorService executor = Executors.newFixedThreadPool(connectionPool);

        System.out.println("Start Allocating Connections : " + (System.currentTimeMillis() - start));
        for (Integer i = 0; i < connectionPool; i++) { // initialize connection pool
        	//conPool.put(i, getConnection());
            conPool.put(i, getConnectionReporterDB());
        }
        System.out.println("Finished Allocating Connections : " + (System.currentTimeMillis() - start));
        
        //each entry in map has 500 insert statements
        Map<Integer,StringBuffer> statements = new HashMap<Integer,StringBuffer>(); // initialize statements map
        for(Integer i = 0; i< jobPool ;i++){
            statements.put(i,new StringBuffer());
        }

        //sort the 500k records into 1000 Jobs of 500 each
        Integer counter = 0;
        for (int i = 1; i < csvRecords.size(); i++) {
        	CSVRecord record = csvRecords.get(i);
        	//check if input has 26 fields
           
            	String generatedLineValues = generateInsertValues(record); 
            	//System.out.print(generatedLineValues);
            	//attaching line to thread - concatenating
            	//BL - to use stringBuffer
            	StringBuffer sb = statements.get(counter % jobPool);
            	sb.append(generatedLineValues);
	            statements.put(counter % jobPool, sb); 
	            
	            if ( counter % 10000 == 0  && counter != 0) {
	            	System.out.print(counter +" ");
	            }
	            if ( counter % 100000 == 0 && counter != 0) {
	            	System.out.println(" ");
	            }
	            counter++;
           
            
        }
        
        csvFileParser.close();

        for (Integer key : statements.keySet()) {
            String stmt = insertStatement + statements.get(key);
            stmt = stmt.substring(0, stmt.length() - 1);
            executor.execute(new ConcurrentInsert(key, stmt, conPool.get(key%connectionPool), start)); // execute insert using connection from connection pool
        }
        executor.shutdown();
        System.out.println("");
        System.out.println("End of mainloop : " + (System.currentTimeMillis() - start));
    }

    private String generateInsertValues(CSVRecord record) {
    	return " (" +addStringValue(record.get("winet_id"))+ "),";
    }
}
