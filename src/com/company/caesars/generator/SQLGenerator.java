package com.company.caesars.generator;

/**
 * Created by Michal Bluj on 2017-06-22.
 */
public interface SQLGenerator {

    public void generateSQLInsertsToFile() throws Exception;
    public void insertRecordsToDatabase() throws Exception;

}
