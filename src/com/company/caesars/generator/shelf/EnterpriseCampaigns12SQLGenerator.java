package com.company.caesars.generator.shelf;

import com.company.caesars.generator.SQLGenerator;
import com.company.caesars.generator.SQLGeneratorBase;
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
 * Created by Michal Bluj on 2017-07-07.
 */
public class EnterpriseCampaigns12SQLGenerator  extends SQLGeneratorBase implements SQLGenerator {

    private String readFilePath = "C://Users//Michal Bluj//Desktop/vedw.gst_market_sum_24mo_view.csv";

    private static final String[] FILE_HEADER_MAPPING = {"i_dmid","i_pdb_trips_all_mkts","i_pdb_rated_trips_all_mkts","i_cms_rated_trips_all_mkts","i_all_days_all_mkts","i_rated_days_all_mkts","f_theo_all_all_mkts","f_actual_all_all_mkts","i_hotel_trips_all_mkts","i_offer_trips_all_mkts","i_minutes_all_mkts","f_mdw_rated_all_mkts","f_mdw_all_all_mkts"};

    private static final String SEPARATOR = ",";

    public EnterpriseCampaigns12SQLGenerator(String file){
    	readFilePath = file;
    }

    Map<Integer, Connection> conPool = new HashMap<Integer, Connection>();

    public void insertRecordsToDatabase() throws Exception {

        Integer numberOfWorkers = 10;

        ExecutorService executor = Executors.newFixedThreadPool(numberOfWorkers);

        for (Integer i = 0; i < numberOfWorkers; i++) {
            conPool.put(i, getShelfConnection());
        }

        CSVFormat csvFileFormat =  CSVFormat.newFormat('|').withHeader();

        FileReader fileReader = new FileReader(readFilePath);

        CSVParser csvFileParser = new CSVParser(fileReader, csvFileFormat);

        List<CSVRecord> csvRecords = csvFileParser.getRecords();


        Map<Integer, String> statements = new HashMap<Integer, String>();
        for (Integer i = 0; i < 10000; i++) {
            statements.put(i, "");
        }

        Integer counter = 0;
        for (int i = 1; i < csvRecords.size(); i++) {
            CSVRecord record = csvRecords.get(i);
            String generatedLine = generateInsertLine(record);
            statements.put(counter % 10000, statements.get(counter % 10000) + generatedLine);
            counter++;
            System.out.println(counter);
        }
        
        csvFileParser.close();

        for (Integer key : statements.keySet()) {
            String stmt = statements.get(key);
            executor.execute(new ConcurrentInsert(key, stmt, conPool.get(key%numberOfWorkers)));
        }
        
        executor.shutdown();
    }

    private String generateInsertLine(CSVRecord record) {
        return "Insert into caesars.enterprise_campaign_12 (i_dmid,i_pdb_trips_all_mkts,i_pdb_rated_trips_all_mkts,i_cms_rated_trips_all_mkts,i_all_days_all_mkts,i_rated_days_all_mkts,f_theo_all_all_mkts,f_actual_all_all_mkts,i_hotel_trips_all_mkts,i_offer_trips_all_mkts,i_minutes_all_mkts,f_mdw_rated_all_mkts,f_mdw_all_all_mkts) VALUES (" +
                addStringValue(record.get("i_dmid")) + SEPARATOR +
                addNumericValue(record.get("i_pdb_trips_all_mkts")) + SEPARATOR +
                        addNumericValue(record.get("i_pdb_rated_trips_all_mkts")) + SEPARATOR +
                                addNumericValue(record.get("i_cms_rated_trips_all_mkts")) + SEPARATOR +
                                        addNumericValue(record.get("i_all_days_all_mkts")) + SEPARATOR +
                                                addNumericValue(record.get("i_rated_days_all_mkts")) + SEPARATOR +
                                                        addNumericValue(record.get("f_theo_all_all_mkts")) + SEPARATOR +
                                                                addNumericValue(record.get("f_actual_all_all_mkts")) + SEPARATOR +
                                                                        addNumericValue(record.get("i_hotel_trips_all_mkts")) + SEPARATOR +
                                                                                addNumericValue(record.get("i_offer_trips_all_mkts")) + SEPARATOR +
                                                                                        addNumericValue(record.get("i_minutes_all_mkts")) + SEPARATOR +
                                                                                                addNumericValue(record.get("f_mdw_rated_all_mkts")) + SEPARATOR +
                                                                                                        addNumericValue(record.get("f_mdw_all_all_mkts"))

                + ");";
    }
}