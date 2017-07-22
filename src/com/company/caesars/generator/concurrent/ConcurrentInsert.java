package com.company.caesars.generator.concurrent;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by Michal Bluj on 2017-06-23.
 */
public class ConcurrentInsert implements Runnable{

    private String statement;
    private Connection connection;
    private Integer key;
    private Long start;

    public ConcurrentInsert(Integer key, String statement, Connection connection, Long start){
        this.statement = statement;
        this.connection = connection;
        this.key = key;
        this.start = start;
    }

    public void run(){
        try {
            //System.out.println("run started " + key + " " + statement);
            Statement st = connection.createStatement();
            st.executeUpdate(statement);
            st.close();
            System.out.println("run finished " + key +" Time from start : " + (System.currentTimeMillis() - start));
        }catch(SQLException e){
            e.printStackTrace();
            return;
        }
    }
}
