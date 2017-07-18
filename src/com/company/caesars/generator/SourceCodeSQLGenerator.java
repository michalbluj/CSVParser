package com.company.caesars.generator;

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
 * Created by Michal Bluj on 2017-07-11.
 */
public class SourceCodeSQLGenerator extends SQLGeneratorBase implements SQLGenerator {

    private String readFilePath = "C://Users//Michal Bluj//Desktop//US1334/source_code.csv";

    private static final String[] FILE_HEADER_MAPPING = {"c_source_cd","c_source_group","c_prop_cd","c_source_cd_desc","c_quality_cd","d_timestamp"};

    private static final String SEPARATOR = ",";

    public void insertRecordsToDatabase() throws Exception {

        retrievePropertyTable();

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

        csvFileParser.close();
        
        Executor executor = new SQLInsertExecutor();

        for (Integer key : statements.keySet()) {
            String stmt = statements.get(key);
            executor.execute(new ConcurrentInsert(key, stmt, connection));
        }
    }

    private String generateInsertLine(CSVRecord record) {
        return "Insert into caesars.source_code (c_source_cd,c_source_group,c_prop_cd,c_prop_cd_fk,c_source_cd_desc,c_quality_cd,d_timestamp) VALUES (" +
                addStringValue(record.get("c_source_cd")) + SEPARATOR +
                addStringValue(record.get("c_source_group")) + SEPARATOR +
                addStringValue(record.get("c_prop_cd")) + SEPARATOR +
                addStringValue(propertyCodeKeyMap.get(record.get("c_prop_cd"))) + SEPARATOR +
                addStringValue(record.get("c_source_cd_desc")) + SEPARATOR +
                addStringValue(record.get("c_quality_cd")) + SEPARATOR +
                addDateValue(record.get("d_timestamp")) +
                ");";
    }
}