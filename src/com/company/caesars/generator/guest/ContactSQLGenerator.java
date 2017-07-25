package com.company.caesars.generator.guest;

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

import com.company.caesars.generator.SQLGenerator;
import com.company.caesars.generator.SQLGeneratorBase;
import com.company.caesars.generator.concurrent.ConcurrentInsert;

/**
 * Created by Michal Bluj on 2017-06-26.
 */
public class ContactSQLGenerator extends SQLGeneratorBase implements SQLGenerator {

    private static final String [] FILE_HEADER_MAPPING = {"i_dmid","c_home_prop_cd","c_dom_cd","c_sec_cd","c_prev_dom_cd","c_last_name","c_first_name","c_middle_init","c_title","c_suffix","d_dob","c_addr_pref","d_create_dt","c_mail_flag","c_quality_cd","d_timestamp","c_phonetic_last","c_phonetic_first","c_acct_type_cd","d_acct_type_as_of","C_AGE_19_PLUS","C_AGE_21_PLUS","C_AGE_18_PLUS","c_ucl_supp_flag","c_uci_supp_flag","c_tdc_supp_flag"};

    private static final String SQLSEPARATOR = ",";

    //private String readFilePath = "C://Users//Michal Bluj//Downloads//FullGuestFile/guest2__04.txt";
    private String readFilePath = "data/devst.caesars.comTest2/2017-07-17_noCR/guest2__01.txt";
    
    private String insertStatement = "INSERT INTO salesforce.contact (winet_id__c,home_property__c,dominant_property__c,lastname,firstname,birthdate,address_preferences__c,mail_flag__c,account_type__c,c_sec_cd__c,c_prev_dom_cd__c,c_middle_init__c,c_title__c,c_suffix__c,c_quality_cd__c,c_phonetic_last__c,c_phonetic_first__c,d_acct_type_as_of__c,C_AGE_19_PLUS__c,C_AGE_21_PLUS__c,C_AGE_18_PLUS__c,c_ucl_supp_flag__c,c_uci_supp_flag__c,c_tdc_supp_flag__c) VALUES";

    Map<Integer, Connection> conPool = new HashMap<Integer, Connection>();

    public ContactSQLGenerator() {
    }
    
    public ContactSQLGenerator(String file) {
    	readFilePath = file;
    }

    private void loadReferences(){
    	//retrieve from MasterDB
        retrieveCampaignCodeTable();
        retrieveCampaignTypeTable();
        retrievePropertyTable();
        retrieveTierCodeTable();
        retrieveMarketTable();
        retrieveAccountTypeCodeTable();
    }
    
    public void insertRecordsToDatabase() throws Exception{

    	Long start = System.currentTimeMillis();
    	System.out.println("Parsing " + readFilePath);
    	
    	loadReferences();
    	
    	System.out.println("Start CSVParser : " + (System.currentTimeMillis() - start));
        CSVFormat csvFileFormat =  CSVFormat.newFormat('|').withHeader(FILE_HEADER_MAPPING);
        FileReader fileReader = new FileReader(readFilePath);
        CSVParser csvFileParser = new CSVParser(fileReader, csvFileFormat);

        //list of 500k records
        
        List<CSVRecord> csvRecords = csvFileParser.getRecords();
        System.out.println("End CSVParser : " + (System.currentTimeMillis() - start));

        Integer connectionPool = 50;
        Integer jobPool = 1000;

        //Allocate same # of Active Thread as in connection.
        //will submit 1000 jobs but only 50 active at a time to avoid connection Pool locking
        ExecutorService executor = Executors.newFixedThreadPool(connectionPool);

        System.out.println("Start Allocating Connections : " + (System.currentTimeMillis() - start));
        for (Integer i = 0; i < connectionPool; i++) { // initialize connection pool
        	//conPool.put(i, getConnection());
            conPool.put(i, getConnectionFull1TestDB());
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
            if(record.size() == 26){
            	// line corresponding to record row
            	String generatedLineValues = generateInsertValues(record); 
            	
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
            } else {
            	//discard any record with incorrect number of fields
            	addToErrorLog("line:"+counter+"  " + record.get("i_dmid"),"Wrong number of columns "+ record.toString());
            }
            
        }
        
        //pushLogsToDB();
        
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
    	return " (" +
        	addNumericValue(record.get("i_dmid")) + SQLSEPARATOR
            + addStringValue(propertyCodeKeyMap.get(record.get("c_home_prop_cd"))) + SQLSEPARATOR
            + addStringValue(propertyCodeKeyMap.get(record.get("c_dom_cd"))) + SQLSEPARATOR
            + addStringValue(record.get("c_last_name") == null || record.get("c_last_name").equals("") ? "Missing last name" : record.get("c_last_name")) + SQLSEPARATOR
            + addStringValue(record.get("c_first_name") == null || record.get("c_first_name").equals("") ? "Missing first name" : record.get("c_first_name")) + SQLSEPARATOR
            + addDateValue(record.get("d_dob")) + SQLSEPARATOR
            + addStringValue(record.get("c_addr_pref")) + SQLSEPARATOR
            + addStringValue(record.get("c_mail_flag")) + SQLSEPARATOR
            + addStringValue(accountTypeCodeKeyMap.get(record.get("c_acct_type_cd"))) + SQLSEPARATOR
            + addStringValue(propertyCodeKeyMap.get(record.get("c_sec_cd"))) + SQLSEPARATOR
            + addStringValue(propertyCodeKeyMap.get(record.get("c_prev_dom_cd"))) + SQLSEPARATOR
            + addStringValue(record.get("c_middle_init")) + SQLSEPARATOR
            + addStringValue(record.get("c_title")) + SQLSEPARATOR
            + addStringValue(record.get("c_suffix")) + SQLSEPARATOR
            + addStringValue(record.get("c_quality_cd")) + SQLSEPARATOR
            + addStringValue(record.get("c_phonetic_last")) + SQLSEPARATOR
            + addStringValue(record.get("c_phonetic_first")) + SQLSEPARATOR
            + addDateValue(record.get("d_acct_type_as_of")) + SQLSEPARATOR
            + addStringValue(record.get("C_AGE_19_PLUS")) + SQLSEPARATOR
            + addStringValue(record.get("C_AGE_21_PLUS")) + SQLSEPARATOR
            + addStringValue(record.get("C_AGE_18_PLUS")) + SQLSEPARATOR
            + addStringValue(record.get("c_ucl_supp_flag")) + SQLSEPARATOR
            + addStringValue(record.get("c_uci_supp_flag")) + SQLSEPARATOR
            + addStringValue(record.get("c_tdc_supp_flag"))
            + "),";
    }
}
