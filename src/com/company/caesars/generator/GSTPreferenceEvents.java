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
public class GSTPreferenceEvents extends SQLGeneratorBase implements SQLGenerator {

    private String readFilePath = "C://Users//Michal Bluj//Desktop//US1349/gst_pref_events.csv";

    private static final String[] FILE_HEADER_MAPPING = {"i_dmid","i_category_id","c_pref_category_name","i_preference_id","c_pref_type_name","i_response","c_derived_flag","c_data_source_cd","c_modifier","c_modifier_workstation","c_modifier_location","c_quality_cd","d_timestamp","c_pref_flag"};

    private static final String SEPARATOR = ",";

    public ObjectMapper mapper = new ObjectMapper();

    public void insertRecordsToDatabase() throws Exception {
    	Long start = System.currentTimeMillis();
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
            executor.execute(new ConcurrentInsert(key, stmt, connection, start));
        }
    }

    private String generateInsertLine(CSVRecord record) {
        try {
            GSTPreferenceEvents.Event event = new GSTPreferenceEvents.Event();
            event.i_dmid = record.get("i_dmid");
            event.i_category_id = record.get("i_category_id");
            event.c_pref_category_name = record.get("c_pref_category_name");
            event.i_preference_id = record.get("i_preference_id");
            event.c_pref_type_name = record.get("c_pref_type_name");
            event.i_response = record.get("i_response");
            event.c_derived_flag = record.get("c_derived_flag");
            event.c_data_source_cd = record.get("c_data_source_cd");
            event.c_modifier = record.get("c_modifier");
            event.c_modifier_workstation = record.get("c_modifier_workstation");
            event.c_modifier_location = record.get("c_modifier_location");
            event.c_quality_cd = record.get("c_quality_cd");
            event.d_timestamp = record.get("d_timestamp");
            event.c_pref_flag = record.get("c_pref_flag");
            return "Insert into caesars.customer_info (data, recordtype,i_dmid) VALUES (" +
                    "'"+mapper.writeValueAsString(event)+ "'" + SEPARATOR +
                    "'Event'," +
                    addStringValue(record.get("i_dmid")) +
                    ");";
        }catch(Exception e){

        }
        return null;
    }

    class Event {
        public String i_dmid;
        public String i_category_id;
        public String c_pref_category_name;
        public String i_preference_id;
        public String c_pref_type_name;
        public String i_response;
        public String c_derived_flag;
        public String c_data_source_cd;
        public String c_modifier;
        public String c_modifier_workstation;
        public String c_modifier_location;
        public String c_quality_cd;
        public String d_timestamp;
        public String c_pref_flag;
    }
}
