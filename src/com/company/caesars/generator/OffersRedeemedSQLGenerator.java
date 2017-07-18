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
public class OffersRedeemedSQLGenerator extends SQLGeneratorBase implements SQLGenerator {

    private String readFilePath = "C://Users//Michal Bluj//Desktop//migration data 5.07.2017/gst_offer_rsv_rdm_all.csv";

    private static final String [] FILE_HEADER_MAPPING = {"i_gst_offer_nbr","i_dmid","c_offer_id","c_collateral_id","c_coupon_type","c_coupon_id","f_rdm_amount","c_rdm_source","c_rdm_channel","c_valid_prop_cd","i_pdb_trip_id","i_coupon_key","c_offer_status","d_offer_status_dt","d_rpt_period","i_coupon_counter","c_logon","d_purge_date","c_purge_flag","c_quality_cd","d_timestamp","d_pdb_timestamp"};

    private static final String SEPARATOR = ",";

    public void insertRecordsToDatabase() throws Exception{

        retrieveTierCodeTable();

        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(FILE_HEADER_MAPPING);

        FileReader fileReader = new FileReader(readFilePath);

        CSVParser csvFileParser = new CSVParser(fileReader, csvFileFormat);

        List<CSVRecord> csvRecords = csvFileParser.getRecords();

        Integer numberOfWorkers = 500;

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
        return "Insert into caesars.offers_redeemed (i_gst_offer_nbr,i_dmid,c_offer_id,c_collateral_id,c_coupon_type,c_coupon_id,f_rdm_amount,c_rdm_source,c_rdm_channel,c_valid_prop_cd,i_pdb_trip_id,i_coupon_key,c_offer_status,d_offer_status_dt,d_rpt_period,i_coupon_counter,c_logon,d_purge_date,c_purge_flag,c_quality_cd,d_timestamp,d_pdb_timestamp) VALUES (" +
                addNumericValue(record.get("i_gst_offer_nbr")) + SEPARATOR +
                        addNumericValue(record.get("i_dmid")) + SEPARATOR +
                                addStringValue(record.get("c_offer_id")) + SEPARATOR +
                                        addStringValue(record.get("c_collateral_id")) + SEPARATOR +
                                                addStringValue(record.get("c_coupon_type")) + SEPARATOR +
                                                        addStringValue(record.get("c_coupon_id")) + SEPARATOR +
                                addNumericValue(record.get("f_rdm_amount")) + SEPARATOR +
                                        addStringValue(record.get("c_rdm_source")) + SEPARATOR +
                                                addStringValue(record.get("c_rdm_channel")) + SEPARATOR +
                                                        addStringValue(record.get("c_valid_prop_cd")) + SEPARATOR +
                                addNumericValue(record.get("i_pdb_trip_id")) + SEPARATOR +
                                addNumericValue(record.get("i_coupon_key")) + SEPARATOR +
                                        addStringValue(record.get("c_offer_status")) + SEPARATOR +
                                                addDateValue(record.get("d_offer_status_dt")) + SEPARATOR +
                                                        addDateValue(record.get("d_rpt_period")) + SEPARATOR +
                                        addNumericValue(record.get("i_coupon_counter")) + SEPARATOR +
                                                addStringValue(record.get("c_logon")) + SEPARATOR +
                                                        addDateValue(record.get("d_purge_date")) + SEPARATOR +
                                                        addStringValue(record.get("c_purge_flag")) + SEPARATOR +
                                                                addStringValue(record.get("c_quality_cd")) + SEPARATOR +
                                                                        addDateValue(record.get("d_timestamp")) + SEPARATOR +
                                                                                addDateValue(record.get("d_pdb_timestamp")) +
                ");";
    }

}