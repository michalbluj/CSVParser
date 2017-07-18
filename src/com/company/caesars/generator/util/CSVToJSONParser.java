package com.company.caesars.generator.util;

import org.apache.commons.csv.CSVRecord;

/**
 * Created by Michal Bluj on 2017-07-17.
 */
public class CSVToJSONParser {

    private String[] elements;
    private CSVRecord record;

    public CSVToJSONParser(String header, CSVRecord record){
        elements = header.split(",");
    }

    public String getJson(CSVRecord record){
        String jsonString = "{";
        for(String element : elements){
            jsonString = jsonString + "\""+element +"\":\""+ record.get(element)+"\",";
        }
        jsonString = jsonString.substring(0,jsonString.length() - 1);
        return jsonString + "}";
    }

}
