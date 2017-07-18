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
 * Created by Michal Bluj on 2017-07-03.
 */
public class ContactGSTGame5SQLGenerator extends SQLGeneratorBase implements SQLGenerator {

    private String readFilePath = "C:/Users/Michal Bluj//Desktop//UCR - Guest data//1to1/guest_game5.csv";

    private static final String [] FILE_HEADER_MAPPING = {"i_dmid","c_game_type","c_group_1","c_group_2","c_group_3","c_group_4","c_group_5","i_top1_value","i_top5_value","i_total_value","i_groups","i_cards_top","i_cards_total","i_machines","f_denom_pref","i_denom","i_denom_pref","i_denom_total","c_user_flag_1","c_user_flag_2","c_user_flag_3","c_user_flag_4","c_user_flag_5","c_user_flag_6","c_user_flag_7","c_user_flag_8","c_user_flag_9","c_user_flag_10","i_user_score_1","i_user_score_2","i_user_score_3","i_user_score_4","i_user_score_5","i_user_score_6","i_user_score_7","i_user_score_8","i_user_score_9","i_user_score_10","f_user_amt_1","f_user_amt_2","f_user_amt_3","f_user_amt_4","f_user_amt_5","f_user_amt_6","f_user_amt_7","f_user_amt_8","f_user_amt_9","f_user_amt_10"};

    private static final String SEPARATOR = ",";

    public void generateSQLInsertsToFile() throws Exception {

    }

    public void insertRecordsToDatabase() throws Exception{

        retrieveTierCodeTable();

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
            executor.execute(new ConcurrentInsert(key, stmt, connection));
        }
    }

    private String generateInsertLine(CSVRecord record) {
        return "Update salesforce.contact SET "+
                " c_game_type__c = " + addStringValue(record.get("c_game_type")) +
                " , c_group_1__c = " + addStringValue(record.get("c_group_1")) +
                " , c_group_2__c = " + addStringValue(record.get("c_group_2")) +
                " , c_group_3__c = " + addStringValue(record.get("c_group_3")) +
                " , c_group_4__c = " + addStringValue(record.get("c_group_4")) +
                " , c_group_5__c = " + addStringValue(record.get("c_group_5")) +
                " where winet_id__c = " + addStringValue(record.get("i_dmid")) +";";
    }
}
