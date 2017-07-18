package com.company.caesars;

import com.company.caesars.generator.SQLGenerator;
import com.company.caesars.generator.contact.ContactSQLGenerator;

public class Main {

    public static void main(String[] args) throws Exception {
        SQLGenerator generator = new ContactSQLGenerator(); // place generator implementation here.
        try {
            generator.insertRecordsToDatabase();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
