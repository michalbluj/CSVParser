package com.company.caesars.generator;

import com.company.caesars.generator.concurrent.ConcurrentInsert;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Michal Bluj on 2017-07-04.
 */
public class  PropertyDescriptionSQLGenerator  extends SQLGeneratorBase implements SQLGenerator {

    private String readFilePath = "C://Users//Michal Bluj//Desktop//code tables 4.07.2017/property_desc.csv";

    private static final String[] FILE_HEADER_MAPPING = {"c_prop_cd", "i_prop_id", "c_brand_cd", "c_property_type", "c_prop_desc", "c_status", "d_prop_open_dt", "c_tr_flag", "c_hotel_flag", "c_event_flag", "i_end_dow", "c_ppdb_auth_cd", "c_prop_short", "i_susp_warn_days", "c_quality_cd", "d_timestamp"};

    private static final String SEPARATOR = ",";


    Map<Integer, Connection> conPool = new HashMap<Integer, Connection>();

    public void insertRecordsToDatabase() throws Exception {
    	Long start = System.currentTimeMillis();
        Integer numberOfWorkers = 10;

        ExecutorService executor = Executors.newFixedThreadPool(numberOfWorkers);

        for (Integer i = 0; i < numberOfWorkers; i++) {
            conPool.put(i, getConnection());
        }

        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(FILE_HEADER_MAPPING);

        FileReader fileReader = new FileReader(readFilePath);

        CSVParser csvFileParser = new CSVParser(fileReader, csvFileFormat);

        List<CSVRecord> csvRecords = csvFileParser.getRecords();


        Map<Integer, String> statements = new HashMap<Integer, String>();
        for (Integer i = 0; i < numberOfWorkers; i++) {
            statements.put(i, "");
        }

        Integer counter = 0;
        for (int i = 1; i < csvRecords.size(); i++) {
            CSVRecord record = csvRecords.get(i);
            String generatedLine = generateInsertLine(record);
            statements.put(counter % numberOfWorkers, statements.get(counter % numberOfWorkers) + generatedLine);
            counter++;
        }

        //Executor executor = new SQLInsertExecutor();

        for (Integer key : statements.keySet()) {
            String stmt = statements.get(key);
            executor.execute(new ConcurrentInsert(key, stmt, conPool.get(key), start));
        }
        executor.shutdown();
    }

    private String generateInsertLine(CSVRecord record) {
        return "Insert into salesforce.property_description__c (name,i_prop_id__c,c_brand_cd__c,c_property_type__c,c_prop_desc__c,c_status__c,d_prop_open_dt__c,c_tr_flag__c,c_hotel_flag__c,c_event_flag__c,i_end_dow__c,c_ppdb_auth_cd__c,c_prop_short__c,i_susp_warn_days__c,c_quality_cd__c) VALUES (" +
                addStringValue(record.get("c_prop_cd")) + SEPARATOR +
                addStringValue(record.get("i_prop_id")) + SEPARATOR +
                addStringValue(record.get("c_brand_cd")) + SEPARATOR +
                addStringValue(record.get("c_property_type")) + SEPARATOR +
                addStringValue(record.get("c_prop_desc")) + SEPARATOR +
                addStringValue(record.get("c_status")) + SEPARATOR +
                addDateValue(record.get("d_prop_open_dt")) + SEPARATOR +
                addStringValue(record.get("c_tr_flag")) + SEPARATOR +
                addStringValue(record.get("c_hotel_flag")) + SEPARATOR +
                addStringValue(record.get("c_event_flag")) + SEPARATOR +
                addNumericValue(record.get("i_end_dow")) + SEPARATOR +
                addStringValue(record.get("c_ppdb_auth_cd")) + SEPARATOR +
                addStringValue(record.get("c_prop_short")) + SEPARATOR +
                addNumericValue(record.get("i_susp_warn_days")) + SEPARATOR +
                addStringValue(record.get("c_quality_cd")) + ");";
    }
}
