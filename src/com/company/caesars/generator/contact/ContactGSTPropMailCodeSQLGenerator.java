package com.company.caesars.generator.contact;

import com.company.caesars.generator.SQLGenerator;
import com.company.caesars.generator.SQLGeneratorBase;
import com.company.caesars.generator.concurrent.ConcurrentInsert;
import com.company.caesars.generator.concurrent.SQLInsertExecutor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Created by Michal Bluj on 2017-07-03.
 */
public class ContactGSTPropMailCodeSQLGenerator extends SQLGeneratorBase implements SQLGenerator {

    private String readFilePath = "C:/Users/Michal Bluj//Desktop//UCR - Guest data//1to1/gst_prop_mail_code.csv";

    private static final String [] FILE_HEADER_MAPPING = {"i_dmid","c_prop_mail_cd","c_quality_cd","d_timestamp"};

    private static final String SEPARATOR = ",";

    public void insertRecordsToDatabase() throws Exception{
    	Long start = System.currentTimeMillis();
        retrieveMailCodeTable();

        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(FILE_HEADER_MAPPING);

        FileReader fileReader = new FileReader(readFilePath);

        CSVParser csvFileParser = new CSVParser(fileReader, csvFileFormat);

        List<CSVRecord> csvRecords = csvFileParser.getRecords();

        Integer numberOfWorkers = 100;

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

        Executor executor = new SQLInsertExecutor();

        for(Integer key : statements.keySet()) {
            String stmt = statements.get(key);
            executor.execute(new ConcurrentInsert(key, stmt, connection, start));
        }
    }

    private String generateInsertLine(CSVRecord record) {
        return "Update salesforce.contact SET  c_prop_mail_cd__c = " + addStringValue(mailcodeMap.get(record.get("c_prop_mail_cd"))) +
                " where winet_id__c = " + addStringValue(record.get("i_dmid")) +";";
    }
}
