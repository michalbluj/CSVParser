package com.company.caesars.generator;

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
 * Created by Michal Bluj on 2017-07-04.
 */
public class PropertyDescriptionSDASQLGenerator   extends SQLGeneratorBase implements SQLGenerator {

    private String readFilePath = "C://Users//Michal Bluj//Desktop//code tables 4.07.2017/prop_desc_sda.csv";

    private static final String[] FILE_HEADER_MAPPING = {"c_prop_cd","c_prop_desc","i_prop_sort","c_prop_desc2","i_prop_sort2","c_prop_desc3","i_prop_sort3","c_prop_desc4","i_prop_sort4","c_boh_prop_cd","c_hr_prop_cd","c_rms_prop_cd","c_tpss_prop_cd","c_labor_prop_cd","c_prop_alt_cd","c_prop_alt2_cd","c_class_level","c_sub_division_desc","i_sub_division_sort","c_state_group","c_state_group_sort","c_division_desc","i_division_sort","c_country_desc","c_country_sort","c_brand_desc","c_brand_sort","f_latitude","f_longitude","c_zip_cd","c_county_name","c_city_name","c_address","c_phone_number","f_ggr_tax_rate","f_ggr_tax_rate_slot","f_ggr_tax_rate_table","f_ggr_tax_rate_other","c_rr_tax_flag","c_property_type","c_user_flag_1","c_user_flag_2","c_user_flag_3","c_user_flag_4","c_user_flag_5","c_user_flag_6","c_user_flag_7","c_user_flag_8","c_user_flag_9","c_user_flag_10","c_user_flag_11","i_user_score_1","i_user_score_2","i_user_score_3","i_user_score_4","f_user_amt_1","f_user_amt_2","f_user_amt_3","f_user_amt_4","f_user_amt_5","f_user_amt_6","f_user_amt_7","f_user_amt_8","f_user_amt_9","f_user_amt_10","f_user_amt_11","f_user_amt_12","f_user_amt_13","f_user_amt_14"
    };

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
            executor.execute(new ConcurrentInsert(key, stmt, conPool.get(key)));
        }
        executor.shutdown();
    }

    private String generateInsertLine(CSVRecord record) {
        return "Insert into salesforce.property_description_sda__c (name,c_prop_desc__c,i_prop_sort__c,c_prop_desc2__c,i_prop_sort2__c,c_prop_desc3__c,i_prop_sort3__c,c_prop_desc4__c,i_prop_sort4__c,c_boh_prop_cd__c,c_hr_prop_cd__c,c_rms_prop_cd__c,c_tpss_prop_cd__c,c_labor_prop_cd__c,c_prop_alt_cd__c,c_prop_alt2_cd__c,c_class_level__c,c_sub_division_desc__c,i_sub_division_sort__c,c_state_group__c,c_state_group_sort__c,c_division_desc__c,i_division_sort__c,c_country_desc__c,c_country_sort__c,c_brand_desc__c,c_brand_sort__c,f_latitude__c,f_longitude__c,c_zip_cd__c,c_county_name__c,c_city_name__c,c_address__c,c_phone_number__c,f_ggr_tax_rate__c,f_ggr_tax_rate_slot__c,f_ggr_tax_rate_table__c,f_ggr_tax_rate_other__c,c_rr_tax_flag__c,c_property_type__c,c_user_flag_1__c,c_user_flag_2__c,c_user_flag_3__c,c_user_flag_4__c,c_user_flag_5__c,c_user_flag_6__c,c_user_flag_7__c,c_user_flag_8__c,c_user_flag_9__c,c_user_flag_10__c,c_user_flag_11__c,i_user_score_1__c,i_user_score_2__c,i_user_score_3__c,i_user_score_4__c,f_user_amt_1__c,f_user_amt_2__c,f_user_amt_3__c,f_user_amt_4__c,f_user_amt_5__c,f_user_amt_6__c,f_user_amt_7__c,f_user_amt_8__c,f_user_amt_9__c,f_user_amt_10__c,f_user_amt_11__c,f_user_amt_12__c,f_user_amt_13__c,f_user_amt_14__c) VALUES (" +

                addStringValue(record.get("c_prop_cd")) + SEPARATOR +
                        addStringValue(record.get("c_prop_desc")) + SEPARATOR +
                                addNumericValue(record.get("i_prop_sort")) + SEPARATOR +
                                addStringValue(record.get("c_prop_desc2")) + SEPARATOR +
                                        addNumericValue(record.get("i_prop_sort2")) + SEPARATOR +
                                        addStringValue(record.get("c_prop_desc3")) + SEPARATOR +
                                                addNumericValue(record.get("i_prop_sort3")) + SEPARATOR +
                                                addStringValue(record.get("c_prop_desc4")) + SEPARATOR +
                                                        addNumericValue(record.get("i_prop_sort4")) + SEPARATOR +
                                                        addStringValue(record.get("c_boh_prop_cd")) + SEPARATOR +
                                                                addStringValue(record.get("c_hr_prop_cd")) + SEPARATOR +
                                                                addStringValue(record.get("c_rms_prop_cd")) + SEPARATOR +
                                                                        addStringValue(record.get("c_tpss_prop_cd")) + SEPARATOR +
                                                                        addStringValue(record.get("c_labor_prop_cd")) + SEPARATOR +
                                                                                addStringValue(record.get("c_prop_alt_cd")) + SEPARATOR +
                                                                                addStringValue(record.get("c_prop_alt2_cd")) + SEPARATOR +
                                                                                        addStringValue(record.get("c_class_level")) + SEPARATOR +
                                                                                        addStringValue(record.get("c_sub_division_desc")) + SEPARATOR +
                                                                                                addNumericValue(record.get("i_sub_division_sort")) + SEPARATOR +
                                                                                                addStringValue(record.get("c_state_group")) + SEPARATOR +
                                                                                                        addStringValue(record.get("c_state_group_sort")) + SEPARATOR +
                                                                                                                addStringValue(record.get("c_division_desc")) + SEPARATOR +
                                                                                                                        addNumericValue(record.get("i_division_sort")) + SEPARATOR +
                                                                                                                        addStringValue(record.get("c_country_desc")) + SEPARATOR +
                                                                                                                                addStringValue(record.get("c_country_sort")) + SEPARATOR +
                                                                                                                                        addStringValue(record.get("c_brand_desc")) + SEPARATOR +
                                                                                                                                                addStringValue(record.get("c_brand_sort")) + SEPARATOR +
                                                                                                                                                        addNumericValue(record.get("f_latitude")) + SEPARATOR +
                                                                                                                                                                addNumericValue(record.get("f_longitude")) + SEPARATOR +
                                                                                                                                                        addStringValue(record.get("c_zip_cd")) + SEPARATOR +
                                                                                                                                                                addStringValue(record.get("c_county_name")) + SEPARATOR +
                                                                                                                                                                        addStringValue(record.get("c_city_name")) + SEPARATOR +
                                                                                                                                                                                addStringValue(record.get("c_address")) + SEPARATOR +
                                                                                                                                                                                        addStringValue(record.get("c_phone_number")) + SEPARATOR +
                                                                                                                                                                                                addNumericValue(record.get("f_ggr_tax_rate")) + SEPARATOR +
                                                                                                                                                                                                        addNumericValue(record.get("f_ggr_tax_rate_slot")) + SEPARATOR +
                                                                                                                                                                                                                addNumericValue(record.get("f_ggr_tax_rate_table")) + SEPARATOR +
                                                                                                                                                                                                                        addNumericValue(record.get("f_ggr_tax_rate_other")) + SEPARATOR +
                                                                                                                                                                                                addStringValue(record.get("c_rr_tax_flag")) + SEPARATOR +
                                                                                                                                                                                                addStringValue(record.get("c_property_type")) + SEPARATOR +
                                                                                                                                                                                                        addStringValue(record.get("c_user_flag_1")) + SEPARATOR +
                                                                                                                                                                                                        addStringValue(record.get("c_user_flag_2")) + SEPARATOR +
                                                                                                                                                                                                        addStringValue(record.get("c_user_flag_3")) + SEPARATOR +
                                                                                                                                                                                                        addStringValue(record.get("c_user_flag_4")) + SEPARATOR +
                                                                                                                                                                                                        addStringValue(record.get("c_user_flag_5")) + SEPARATOR +
                                                                                                                                                                                                        addStringValue(record.get("c_user_flag_6")) + SEPARATOR +
                                                                                                                                                                                                        addStringValue(record.get("c_user_flag_7")) + SEPARATOR +
                                                                                                                                                                                                        addStringValue(record.get("c_user_flag_8")) + SEPARATOR +
                                                                                                                                                                                                                addStringValue(record.get("c_user_flag_9")) + SEPARATOR +
                                                                                                                                                                                                        addStringValue(record.get("c_user_flag_10")) + SEPARATOR +
                                                                                                                                                                                                        addStringValue(record.get("c_user_flag_11")) + SEPARATOR +
                                                                                                                                                                                                                addNumericValue(record.get("i_user_score_1")) + SEPARATOR +
                                                                                                                                                                                                                        addNumericValue(record.get("i_user_score_2")) + SEPARATOR +
                                                                                                                                                                                                                        addNumericValue(record.get("i_user_score_3")) + SEPARATOR +
                                                                                                                                                                                                                        addNumericValue(record.get("i_user_score_4")) + SEPARATOR +
                                                                                                                                                                                                                        addNumericValue(record.get("f_user_amt_1")) + SEPARATOR +
                                                                                                                                                                                                                        addNumericValue(record.get("f_user_amt_2")) + SEPARATOR +
                                                                                                                                                                                                                        addNumericValue(record.get("f_user_amt_3")) + SEPARATOR +
                                                                                                                                                                                                                        addNumericValue(record.get("f_user_amt_4")) + SEPARATOR +
                                                                                                                                                                                                                        addNumericValue(record.get("f_user_amt_5")) + SEPARATOR +
                                                                                                                                                                                                                        addNumericValue(record.get("f_user_amt_6")) + SEPARATOR +
                                                                                                                                                                                                                        addNumericValue(record.get("f_user_amt_7")) + SEPARATOR +
                                                                                                                                                                                                                        addNumericValue(record.get("f_user_amt_8")) + SEPARATOR +
                                                                                                                                                                                                                        addNumericValue(record.get("f_user_amt_9")) + SEPARATOR +
                                                                                                                                                                                                                        addNumericValue(record.get("f_user_amt_10")) + SEPARATOR +
                                                                                                                                                                                                                        addNumericValue(record.get("f_user_amt_11")) + SEPARATOR +
                                                                                                                                                                                                                        addNumericValue(record.get("f_user_amt_12")) + SEPARATOR +
                                                                                                                                                                                                                        addNumericValue(record.get("f_user_amt_13")) + SEPARATOR +
                                                                                                                                                                                                                        addNumericValue(record.get("f_user_amt_14"))

                + ");";
    }
}
