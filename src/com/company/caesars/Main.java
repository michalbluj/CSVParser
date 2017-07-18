package com.company.caesars;

import com.company.caesars.generator.OffersRedeemedSQLGenerator;
import com.company.caesars.generator.SQLGenerator;

public class Main {

    public static void main(String[] args) throws Exception {
        SQLGenerator generator = new OffersRedeemedSQLGenerator(); // place generator implementation here.
        try {
            generator.insertRecordsToDatabase();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
