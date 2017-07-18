package com.company.caesars.generator;

import com.company.caesars.generator.concurrent.ConcurrentInsert;
import com.company.caesars.generator.concurrent.SQLInsertExecutor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Created by Michal Bluj on 2017-07-12.
 */
public class GSTInterestSQLGenerator extends SQLGeneratorBase implements SQLGenerator {

    private String readFilePath = "C://Users//Michal Bluj//Desktop//US1349/gst_interest.csv";

    private static final String[] FILE_HEADER_MAPPING = {"i_dmid","c_interest_cd","c_quality_cd","d_timestamp"};

    private static final String SEPARATOR = ",";

    public ObjectMapper mapper = new ObjectMapper();

    public void generateSQLInsertsToFile() throws Exception {

    }

    public void insertRecordsToDatabase() throws Exception {

        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(FILE_HEADER_MAPPING);

        FileReader fileReader = new FileReader(readFilePath);

        CSVParser csvFileParser = new CSVParser(fileReader, csvFileFormat);

        List<CSVRecord> csvRecords = csvFileParser.getRecords();

        Integer numberOfWorkers = 10;

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

        Executor executor = new SQLInsertExecutor();

        for (Integer key : statements.keySet()) {
            String stmt = statements.get(key);
            executor.execute(new ConcurrentInsert(key, stmt, connection));
        }
    }

    private String generateInsertLine(CSVRecord record) {
        try {
            Interest interest = new Interest(record.get("i_dmid"), record.get("c_interest_cd"), record.get("c_quality_cd"), record.get("d_timestamp"));

            return "Insert into caesars.customer_info (data, recordtype,i_dmid) VALUES (" +
                    "'"+mapper.writeValueAsString(interest)+ "'" + SEPARATOR +
                    "'Interest'," +
                    addStringValue(record.get("i_dmid")) +
                    ");";
        }catch(Exception e){

        }
        return null;
    }

    class Interest {
        public String i_dmid;
        public String c_interest_cd;
        public String c_quality_cd;
        public String d_timestamp;

        public Interest(String arg1,String arg2,String arg3,String arg4){
            i_dmid = arg1;
            c_interest_cd = arg2;
            c_quality_cd = arg3;
            d_timestamp = arg4;
        }
    }
}