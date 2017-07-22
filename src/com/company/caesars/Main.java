package com.company.caesars;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.company.caesars.generator.concurrent.ConcurrentProcessor;
import com.company.caesars.generator.guest.ContactSQLGenerator;

public class Main {

    public static void main(String[] args) throws Exception {
    	
    	ExecutorService executor = Executors.newFixedThreadPool(3);
    	
    	//ConcurrentProcessor proc1 = new ConcurrentProcessor(new ContactSQLGenerator("C://Users//Michal Bluj//Downloads//FullGuestFile/guest2__60.txt"));
    	ConcurrentProcessor proc1 = new ConcurrentProcessor(new ContactSQLGenerator("data/devst.caesars.comTest2/2017-07-17_noCR/guest2__01.txt"));
    	
    	//ConcurrentProcessor proc2 = new ConcurrentProcessor(new ContactSQLGenerator("C://Users//Michal Bluj//Downloads//FullGuestFile/guest2__63.txt"));
    	//ConcurrentProcessor proc3 = new ConcurrentProcessor(new ContactSQLGenerator("C://Users//Michal Bluj//Downloads//FullGuestFile/guest2__27.txt"));
    	
    	executor.execute(proc1);
    	//executor.execute(proc2);
    	//executor.execute(proc3);
    	
    	//shutdown method will allow previously submitted tasks to execute before terminating
        executor.shutdown();
    	
    	
        /*SQLGenerator generator = new ContactSQLGenerator(); // place generator implementation here.
        try {
            generator.insertRecordsToDatabase();
        }catch(Exception e){
            e.printStackTrace();
        }*/
    }
}
