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
public class LMSSourceDescSDASQLGenereator extends SQLGeneratorBase implements SQLGenerator {

    private String readFilePath = "C://Users//Michal Bluj//Desktop//US1334/lms_source_desc_sda.csv";

    private static final String[] FILE_HEADER_MAPPING = {"c_prop_cd","c_source_group","c_source_cd","c_source_cd_desc","c_rms_group_source_cd","c_source_cat_sda","i_source_cat_sda_sort","c_source_cat1","i_source_cat1_sort","c_source_cat2","i_source_cat2_sort","c_source_cat3","i_source_cat3_sort","c_source_cat4","i_source_cat4_sort","c_source_cat5","i_source_cat5_sort","c_source_cat6","i_source_cat6_sort","c_user_flag1","c_user_flag2","c_user_flag3","c_user_flag4","c_user_flag5","c_user_flag6","c_user_flag7","c_user_flag8","i_user_score1","i_user_score2","i_user_score3","i_user_score4","f_user_amt1","f_user_amt2","f_user_amt3","f_user_amt4","d_last_update_ts","c_quality_cd","d_timestamp"};

    private static final String SEPARATOR = ",";

    public void insertRecordsToDatabase() throws Exception {
    	Long start = System.currentTimeMillis();
        retrievePropertyTable();
        retreiveSourceCodesMap();

        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(FILE_HEADER_MAPPING);

        FileReader fileReader = new FileReader(readFilePath);

        CSVParser csvFileParser = new CSVParser(fileReader, csvFileFormat);

        List<CSVRecord> csvRecords = csvFileParser.getRecords();

        Integer numberOfWorkers = 20;

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
            executor.execute(new ConcurrentInsert(key, stmt, connection, start));
        }
    }

    private String generateInsertLine(CSVRecord record) {
        return "Insert into caesars.lms_source_desc_sda (c_prop_cd,c_prop_cd_fk,c_source_group,c_source_cd,c_source_cd_fk,c_source_cd_desc,c_rms_group_source_cd,c_source_cat_sda,i_source_cat_sda_sort,c_source_cat1,i_source_cat1_sort,c_source_cat2,i_source_cat2_sort,c_source_cat3,i_source_cat3_sort,c_source_cat4,i_source_cat4_sort,c_source_cat5,i_source_cat5_sort,c_source_cat6,i_source_cat6_sort,c_user_flag1,c_user_flag2,c_user_flag3,c_user_flag4,c_user_flag5,c_user_flag6,c_user_flag7,c_user_flag8,i_user_score1,i_user_score2,i_user_score3,i_user_score4,f_user_amt1,f_user_amt2,f_user_amt3,f_user_amt4,d_last_update_ts,c_quality_cd,d_timestamp) VALUES (" +
                addStringValue(record.get("c_prop_cd")) + SEPARATOR +
                addStringValue(propertyCodeKeyMap.get(record.get("c_prop_cd"))) + SEPARATOR +
                addStringValue(record.get("c_source_group")) + SEPARATOR +
                addStringValue(record.get("c_source_cd")) + SEPARATOR +
                addNumericValue(sourceCodesMap.get(record.get("c_prop_cd") + record.get("c_source_cd")+record.get("c_source_group"))) + SEPARATOR +
                addStringValue(record.get("c_source_cd_desc")) + SEPARATOR +
                        addStringValue(record.get("c_rms_group_source_cd")) + SEPARATOR +
                                addStringValue(record.get("c_source_cat_sda")) + SEPARATOR +
                                        addNumericValue(record.get("i_source_cat_sda_sort")) + SEPARATOR +
                                        addStringValue(record.get("c_source_cat1")) + SEPARATOR +
                                                addStringValue(record.get("i_source_cat1_sort")) + SEPARATOR +
                                                addStringValue(record.get("c_source_cat2")) + SEPARATOR +
                                                        addStringValue(record.get("i_source_cat2_sort")) + SEPARATOR +
                                                        addStringValue(record.get("c_source_cat3")) + SEPARATOR +
                                                                addStringValue(record.get("i_source_cat3_sort")) + SEPARATOR +
                                                                addStringValue(record.get("c_source_cat4")) + SEPARATOR +
                                                                        addStringValue(record.get("i_source_cat4_sort")) + SEPARATOR +
                                                                        addStringValue(record.get("c_source_cat5")) + SEPARATOR +
                                                                                addStringValue(record.get("i_source_cat5_sort")) + SEPARATOR +
                                                                                addStringValue(record.get("c_source_cat6")) + SEPARATOR +
                                                                                        addStringValue(record.get("i_source_cat6_sort")) + SEPARATOR +
                                                                                        addStringValue(record.get("c_user_flag1")) + SEPARATOR +
                                                                                                addStringValue(record.get("c_user_flag2")) + SEPARATOR +
                                                                                                        addStringValue(record.get("c_user_flag3")) + SEPARATOR +
                                                                                                                addStringValue(record.get("c_user_flag4")) + SEPARATOR +
                                                                                                                        addStringValue(record.get("c_user_flag5")) + SEPARATOR +
                                                                                                                                addStringValue(record.get("c_user_flag6")) + SEPARATOR +
                                                                                                                                        addStringValue(record.get("c_user_flag7")) + SEPARATOR +
                                                                                                                                                addStringValue(record.get("c_user_flag8")) + SEPARATOR +
                                                                                                                                                        addStringValue(record.get("i_user_score1")) + SEPARATOR +
                                                                                                                                                                addStringValue(record.get("i_user_score2")) + SEPARATOR +
                                                                                                                                                                        addStringValue(record.get("i_user_score3")) + SEPARATOR +
                                                                                                                                                                                addStringValue(record.get("i_user_score4")) + SEPARATOR +
                                                                                                                                                                                        addStringValue(record.get("f_user_amt1")) + SEPARATOR +
                                                                                                                                                                                                addStringValue(record.get("f_user_amt2")) + SEPARATOR +
                                                                                                                                                                                                addStringValue(record.get("f_user_amt3")) + SEPARATOR +
                                                                                                                                                                                                addStringValue(record.get("f_user_amt4")) + SEPARATOR +
                                                                                                                                                                                                        addDateValue(record.get("d_last_update_ts")) + SEPARATOR +
                addStringValue(record.get("c_quality_cd")) + SEPARATOR +
                addDateValue(record.get("d_timestamp")) +
                ");";
    }
}
