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
public class EnterpriseCampaignsSQLGenerator  extends SQLGeneratorBase implements SQLGenerator {

    private String readFilePath = "C://Users//Michal Bluj//Desktop/vedw.gst_market_sum_24mo_view.csv";

    private static final String[] FILE_HEADER_MAPPING = {"i_dmid","i_pdb_trips_all_mkts","i_pdb_rated_trips_all_mkts","i_cms_rated_trips_all_mkts","i_all_days_all_mkts","i_rated_days_all_mkts","f_theo_all_all_mkts","f_actual_all_all_mkts","i_hotel_trips_all_mkts","i_offer_trips_all_mkts","i_minutes_all_mkts","f_mdw_rated_all_mkts","f_mdw_all_all_mkts"};

    private static final String SEPARATOR = ",";


    Map<Integer, Connection> conPool = new HashMap<Integer, Connection>();

    public void insertRecordsToDatabase() throws Exception {

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
            //stmt += " Update caesars.enterprise_campaign set contact = (select sfid from salesforce.contact where winet_id__c = i_dmid);";
            executor.execute(new ConcurrentInsert(key, stmt, conPool.get(key)));
        }
        executor.shutdown();
    }

    private String generateInsertLine(CSVRecord record) {
        return "Insert into caesars.enterprise_campaign (i_dmid,i_pdb_trips_all_mkts,i_pdb_rated_trips_all_mkts,i_cms_rated_trips_all_mkts,i_all_days_all_mkts,i_rated_days_all_mkts,f_theo_all_all_mkts,f_actual_all_all_mkts,i_hotel_trips_all_mkts,i_offer_trips_all_mkts,i_minutes_all_mkts,f_mdw_rated_all_mkts,f_mdw_all_all_mkts) VALUES (" +
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