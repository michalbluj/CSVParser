package com.company.caesars.generator.contact;

import com.company.caesars.generator.SQLGenerator;
import com.company.caesars.generator.SQLGeneratorBase;
import com.company.caesars.generator.concurrent.ConcurrentInsert;
import com.company.caesars.generator.concurrent.SQLInsertExecutor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Michal Bluj on 2017-06-26.
 */
public class ContactSQLGenerator extends SQLGeneratorBase implements SQLGenerator {

    private static final String [] FILE_HEADER_MAPPING = {"i_dmid","c_home_prop_cd","c_dom_cd","c_sec_cd","c_prev_dom_cd","c_last_name","c_first_name","c_middle_init","c_title","c_suffix","d_dob","c_addr_pref","d_create_dt","c_mail_flag","c_quality_cd","d_timestamp","c_phonetic_last","c_phonetic_first","c_acct_type_cd","d_acct_type_as_of","C_AGE_19_PLUS","C_AGE_21_PLUS","C_AGE_18_PLUS","c_ucl_supp_flag","c_uci_supp_flag","c_tdc_supp_flag"};

    private static final String SEPARATOR = ",";

    private String readFilePath = "C://Users//Michal Bluj//Downloads//FullGuestFile/guest-009-001.csv";
    private String writeFilePath = "C://Users//Michal Bluj/Desktop//migration scripts/contacts insert.txt";
    private String insertStatement = "INSERT INTO salesforce.contact (winet_id__c,home_property__c,dominant_property__c,lastname,firstname,birthdate,address_preferences__c,mail_flag__c,account_type__c,c_sec_cd__c,c_prev_dom_cd__c,c_middle_init__c,c_title__c,c_suffix__c,c_quality_cd__c,c_phonetic_last__c,c_phonetic_first__c,d_acct_type_as_of__c,C_AGE_19_PLUS__c,C_AGE_21_PLUS__c,C_AGE_18_PLUS__c,c_ucl_supp_flag__c,c_uci_supp_flag__c,c_tdc_supp_flag__c) VALUES";

    Map<Integer, Connection> conPool = new HashMap<Integer, Connection>();

    public ContactSQLGenerator() {
    }

    public ContactSQLGenerator(String readFilePath) {
    }

    public ContactSQLGenerator(String readFilePath, String writeFilePath) {
        this.readFilePath = readFilePath;
        this.writeFilePath = writeFilePath;
    }

    private String generateInsertLine(String line) {
        return "";
    }
    public void generateSQLInsertsToFile() throws Exception {
    }

    private void loadReferences(){
        retrieveCampaignCodeTable();
        retrieveCampaignTypeTable();
        retrievePropertyTable();
        retrieveTierCodeTable();
        retrieveMarketTable();
        retrieveAccountTypeCodeTable();
    }
    
    public void insertRecordsToDatabase() throws Exception{

    	System.out.println("Parsing " + readFilePath);
    	
    	loadReferences();
    	
        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(FILE_HEADER_MAPPING);

        FileReader fileReader = new FileReader(readFilePath);

        CSVParser csvFileParser = new CSVParser(fileReader, csvFileFormat);

        List<CSVRecord> csvRecords = csvFileParser.getRecords();

        Integer connectionPool = 10;

        Integer threadPool = 1000;

        ExecutorService executor = Executors.newFixedThreadPool(threadPool);

        for (Integer i = 0; i < connectionPool; i++) { // initialize connection pool
            conPool.put(i, getConnection());
        }


        Map<Integer,String> statements = new HashMap<Integer,String>(); // initialize statements map
        for(Integer i = 0; i< threadPool ;i++){
            statements.put(i,"");
        }

        Integer counter = 0;
        for (int i = 1; i < csvRecords.size(); i++) {
        	CSVRecord record = csvRecords.get(i);
            if(record.size() == 26){
            	String generatedLine = generateInsertLine(record); // line coresponding to record row
	            statements.put(counter % threadPool, statements.get(counter % threadPool) + generatedLine); // attaching line to thread
	            System.out.println(counter);
	            counter++;
            } else {
            	addToErrorLog(record.toString(),"To many collumns");
            }
        }
        
        pushLogsToDB();
        
        csvFileParser.close();

        for (Integer key : statements.keySet()) {
            String stmt = insertStatement + statements.get(key);
            stmt = stmt.substring(0, stmt.length() - 1);
            executor.execute(new ConcurrentInsert(key, stmt, conPool.get(key%connectionPool))); // execute insert using connection from connection pool
        }
        executor.shutdown();

    }

    private String generateInsertLine(CSVRecord record) {
    	return " (" +
        	addNumericValue(record.get("i_dmid")) + SEPARATOR
            + addStringValue(propertyCodeKeyMap.get(record.get("c_home_prop_cd"))) + SEPARATOR
            + addStringValue(propertyCodeKeyMap.get(record.get("c_dom_cd"))) + SEPARATOR
            + addStringValue(record.get("c_last_name")) + SEPARATOR
            + addStringValue(record.get("c_first_name")) + SEPARATOR
            + addDateValue(record.get("d_dob")) + SEPARATOR
            + addStringValue(record.get("c_addr_pref")) + SEPARATOR
            + addStringValue(record.get("c_mail_flag")) + SEPARATOR
            + addStringValue(accountTypeCodeKeyMap.get(record.get("c_acct_type_cd"))) + SEPARATOR
            + addStringValue(propertyCodeKeyMap.get(record.get("c_sec_cd"))) + SEPARATOR
            + addStringValue(propertyCodeKeyMap.get(record.get("c_prev_dom_cd"))) + SEPARATOR
            + addStringValue(record.get("c_middle_init")) + SEPARATOR
            + addStringValue(record.get("c_title")) + SEPARATOR
            + addStringValue(record.get("c_suffix")) + SEPARATOR
            + addStringValue(record.get("c_quality_cd")) + SEPARATOR
            + addStringValue(record.get("c_phonetic_last")) + SEPARATOR
            + addStringValue(record.get("c_phonetic_first")) + SEPARATOR
            + addDateValue(record.get("d_acct_type_as_of")) + SEPARATOR
            + addStringValue(record.get("C_AGE_19_PLUS")) + SEPARATOR
            + addStringValue(record.get("C_AGE_21_PLUS")) + SEPARATOR
            + addStringValue(record.get("C_AGE_18_PLUS")) + SEPARATOR
            + addStringValue(record.get("c_ucl_supp_flag")) + SEPARATOR
            + addStringValue(record.get("c_uci_supp_flag")) + SEPARATOR
            + addStringValue(record.get("c_tdc_supp_flag"))
            + "),";
    }
}
