package com.company.caesars.generator.concurrent;

import com.company.caesars.generator.SQLGenerator;

public class ConcurrentProcessor implements Runnable{
	
	private SQLGenerator generator;
	
    public ConcurrentProcessor(SQLGenerator generator){
    	this.generator = generator;
    }

    public void run(){
    	try{
    		generator.insertRecordsToDatabase();
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    }
}
