package com.company.caesars.generator;

import com.company.caesars.generator.concurrent.ConcurrentInsert;
import com.company.caesars.generator.concurrent.SQLInsertExecutor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Stream;
import java.util.HashMap;

/**
 * Created by Michal Bluj on 2017-06-22.
 */
public class OffersSentSQLGenerator extends SQLGeneratorBase implements SQLGenerator {

    private String readFilePath = "C://Users//Michal Bluj//Desktop//migration data 5.07.2017/gst_offer_sent_all.csv";

    private static final String [] FILE_HEADER_MAPPING = {"i_gst_offer_nbr","i_dmid","i_prospect_id","c_offer_id","c_recp_grp_id","c_offer_status","d_offer_status_dt","d_send_date","i_nbr_coup_redeem","i_paradb_list_id","i_mail_id","d_rpt_period","i_offer_counter","c_quality_cd","d_purge_date","c_purge_flag","d_timestamp"};

    private static final String SEPARATOR = ",";

    public void insertRecordsToDatabase() throws Exception{
    	Long start = System.currentTimeMillis();
        retrieveTierCodeTable();

        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(FILE_HEADER_MAPPING);

        FileReader fileReader = new FileReader(readFilePath);

        CSVParser csvFileParser = new CSVParser(fileReader, csvFileFormat);

        List<CSVRecord> csvRecords = csvFileParser.getRecords();

        Integer numberOfWorkers = 1000;

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
        return "Insert into caesars.offers_sent (i_gst_offer_nbr,i_dmid,i_prospect_id,c_offer_id,c_recp_grp_id,c_offer_status,d_offer_status_dt,d_send_date,i_nbr_coup_redeem,i_paradb_list_id,i_mail_id,d_rpt_period,i_offer_counter,c_quality_cd,d_purge_date,c_purge_flag,d_timestamp) VALUES (" +
                addNumericValue(record.get("i_gst_offer_nbr")) + SEPARATOR +
                addNumericValue(record.get("i_dmid")) + SEPARATOR +
                addNumericValue(record.get("i_prospect_id")) + SEPARATOR +
                addStringValue(record.get("c_offer_id")) + SEPARATOR +
                addStringValue(record.get("c_recp_grp_id")) + SEPARATOR +
                addStringValue(record.get("c_offer_status")) + SEPARATOR +
                addDateValue(record.get("d_offer_status_dt")) + SEPARATOR +
                addStringValue(record.get("d_send_date")) + SEPARATOR +
                addNumericValue(record.get("i_nbr_coup_redeem")) + SEPARATOR +
                addNumericValue(record.get("i_paradb_list_id")) + SEPARATOR +
                addNumericValue(record.get("i_mail_id")) + SEPARATOR +
                addDateValue(record.get("d_rpt_period")) + SEPARATOR +
                addNumericValue(record.get("i_offer_counter")) + SEPARATOR +
                addStringValue(record.get("c_quality_cd")) + SEPARATOR +
                addDateValue(record.get("d_purge_date")) + SEPARATOR +
                addStringValue(record.get("c_purge_flag")) + SEPARATOR +
                addDateValue(record.get("d_timestamp"))+
                ");";
    }

}