package com.company.caesars;

import com.company.caesars.generator.*;
import com.company.caesars.pgp.Decryptor;

import java.io.*;
import java.util.Map;
import java.util.HashMap;

public class Main {

    public static void main(String[] args) throws Exception {

        SQLGenerator generator = new ContactSQLGenerator();
        try {
            generator.insertRecordsToDatabase();
        }catch(Exception e){
            e.printStackTrace();
        }

    }

}
