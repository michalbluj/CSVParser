package com.company.caesars.generator;

import com.company.caesars.generator.concurrent.ConcurrentInsert;
import com.company.caesars.generator.concurrent.SQLInsertExecutor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Michal Bluj on 2017-07-03.
 */
public class ContactGSTrcBalanceSQLGenerator  extends SQLGeneratorBase implements SQLGenerator {

    private String readFilePath = "C:/Users/Michal Bluj//Desktop//UCR - Guest data//1to1/gst_rcbalance.csv";

    private static final String [] FILE_HEADER_MAPPING = {"i_dmid","f_balance","d_as_of","d_last_earned","d_last_expire	c","quality_cd","d_timestamp"};

    private static final String SEPARATOR = ",";


    Map<Integer,Connection> conPool = new HashMap<Integer,Connection>();

    public void generateSQLInsertsToFile() throws Exception {

    }

    public void insertRecordsToDatabase() throws Exception{

        Integer numberOfWorkers = 10;

        ExecutorService executor = Executors.newFixedThreadPool(numberOfWorkers);

        for(Integer i = 0; i < numberOfWorkers; i++){
            conPool.put(i,getConnection());
        }

        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(FILE_HEADER_MAPPING);

        FileReader fileReader = new FileReader(readFilePath);

        CSVParser csvFileParser = new CSVParser(fileReader, csvFileFormat);

        List<CSVRecord> csvRecords = csvFileParser.getRecords();



        Map<Integer,String> statements = new HashMap<Integer,String>();
        for(Integer i = 0; i< numberOfWorkers ;i++){
            statements.put(i,"");
        }

        Integer counter = 0;
        for (int i = 1; i < csvRecords.size(); i++) {
            CSVRecord record = csvRecords.get(i);
            String generatedLine = generateInsertLine(record);
            statements.put(counter % numberOfWorkers, statements.get(counter % numberOfWorkers) + generatedLine);
            counter ++;
        }

        //Executor executor = new SQLInsertExecutor();

        for(Integer key : statements.keySet()) {
            String stmt = statements.get(key);
            executor.execute(new ConcurrentInsert(key, stmt, conPool.get(key)));
        }
        executor.shutdown();
    }

    private String generateInsertLine(CSVRecord record) {
        return "Update salesforce.contact SET f_balance__c = " + addStringValue(record.get("f_balance")) +
                " , d_as_of__c = " + addDateValue(record.get("d_as_of")) +
                " where winet_id__c = " + addStringValue(record.get("i_dmid")) +";";
    }
}
