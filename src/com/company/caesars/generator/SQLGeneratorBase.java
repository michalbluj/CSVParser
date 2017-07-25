package com.company.caesars.generator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.company.caesars.generator.util.AppLog;

/**
 * Created by Michal Bluj on 2017-06-22.
 */
public class SQLGeneratorBase {

    protected Connection connection = getConnection();

    protected final Map<String,String> campaignCodeKeyMap  = new HashMap<String, String>();
    protected final Map<String,String> campaignTypeKeyMap  = new HashMap<String, String>();
    protected final Map<String,String> propertyCodeKeyMap  = new HashMap<String, String>();
    protected final Map<String,String> marketCodeKeyMap = new HashMap<String, String>();
    protected final Map<String,String> accountTypeCodeKeyMap = new HashMap<String, String>();
    protected final Map<String,String> tierCodeKeyMap = new HashMap<String, String>();
    protected final Map<String,String> contactMap = new HashMap<String, String>();
    protected final Map<String,String> associationReasonMap = new HashMap<String, String>();
    protected final Map<String,String> mailcodeMap = new HashMap<String, String>();
    protected final Map<String,String> sourceCodesMap = new HashMap<>();
    
    protected List<AppLog> errorlogs = new ArrayList<AppLog>();

    protected void addToErrorLog(String relatedRecord,String errorDescription){
    	errorlogs.add(new AppLog(relatedRecord,errorDescription));
    }
    
    protected void pushLogsToDB(){
    	if(!errorlogs.isEmpty()){
			Connection con = getConnection();
			String statement = "INSERT INTO public.applogs(level,msg,meta) VALUES ";
			for(AppLog aLog : errorlogs){
				
				System.out.println("error " + aLog.message + " " + aLog.record);
				
				statement += "('error','"+aLog.message+"','"+aLog.record+"'),";
			}
			statement = statement.substring(0,statement.length()-1);
			try {
		        Statement st = con.createStatement();
		        st.executeUpdate(statement);
		        st.close();
		        
		    }catch(SQLException e){
		        e.printStackTrace();
		        return;
		    }
    	}
    }
    
    protected String addNumericValue(String element){
        if(element == null) return "NULL";
        return element.equals("NULL") || element.equals("?") || element.equals("empty") ? "NULL" :  element.replaceAll(",", "").trim();
    }

    protected String addStringValue(String element){
        if(element == null) return "NULL";
       return element.equals("NULL") || element.equals("?") || element.equals("empty") ? "NULL" :  "'"  +element.replaceAll(",", "").replace("'", "''").trim() + "'";
    }

    protected String addDateValue(String element){
        if(element == null || element.equals("")) return "NULL";
        return element.equals("NULL") || element.equals("?") || element.equals("empty") ? "NULL" : "'" + element.replaceAll(",", "").trim() + "'";
    }

    protected String addBooleanValue(String element){
        if(element == null || element.equals("")) return "NULL";
        return element.equals("NULL") || element.equals("?") || element.equals("empty") ? "NULL" :  element.replace("Y", "true").replace("N", "false").trim() ;
    }

    protected String calculateWorthBreakMin(String worthBreak){
        String value = "";
        if(worthBreak != null){
            Integer positionStart = worthBreak.indexOf(" ");
            Integer positionEnd = worthBreak.indexOf("-");
            if(positionStart > 0 && positionEnd > 0) {
                value = worthBreak.substring(positionStart,positionEnd);
            }
        }
        return value;
    }

    protected String calculateWorthBreakMax(String worthBreak){
        String value = "";
        if(worthBreak != null){
            Integer position = worthBreak.indexOf("-");
            if(position > 0) {
                value = worthBreak.substring(position+1,worthBreak.length());
            }
        }
        return value;
    }

    public Connection getShelfConnection(){

        System.out.println("Establishing connection ...");

        try {
            Class.forName("org.postgresql.Driver");
        }catch(ClassNotFoundException e) {
            e.printStackTrace();
        }


        Connection connection = null;

        try {
            connection = DriverManager.getConnection(
                    "jdbc:postgresql://ec2-34-230-227-133.compute-1.amazonaws.com/d25gl9kunlhbi?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory",
                    "u66apiaok6b04k",
                    "pccce3c86b585ea8c53a75a2e80404027492d842a19befaf80cf8215e095ecf8d");

            System.out.println("Connection established !");
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

        return connection;
    }

    public Connection getConnection(){

        System.out.println("Establishing connection ...");

        try {
            Class.forName("org.postgresql.Driver");
        }catch(ClassNotFoundException e) {
            e.printStackTrace();
        }


        Connection connection = null;

        try {
        	//fulll
        	/*connection = DriverManager.getConnection(
                    "jdbc:postgresql://ec2-52-201-189-170.compute-1.amazonaws.com/d6ch7lh545s6lr?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory",
                    "u9je585fn3fp0v",
                    "p895ece32af622f692a080dbf05680ae1f2d043ebf78d97da20f88ab6e4e7ec1d");*/
        	
        	//other/ gold
        	/*connection = DriverManager.getConnection(
                    "jdbc:postgresql://ec2-52-207-205-187.compute-1.amazonaws.com/dq99krcltotqa?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory",
                    "u6i6j5ovv6b7ij",
                    "p5322793c9de3855cbbfe6c8bc089160e79ae35199919a8ff1a92b393efa141dc");*/
        	
        	//sprint1 :: database
        	connection = DriverManager.getConnection(
                    "jdbc:postgresql://ec2-50-16-46-227.compute-1.amazonaws.com/d1mb3cbiok048o?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory",
                    "u9377sb661ci7e",
                    "p64fe9f36c631d0673d907028443bbce517d27531d0619125d0ba27b9d42a0e3a");

            // QA1
            /*connection = DriverManager.getConnection(
                    "jdbc:postgresql://ec2-34-227-214-243.compute-1.amazonaws.com/dau03qomhvel67?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory",
                    "uahfh532bp67fe",
                    "p7c293c039febd914d3994d2ffc857e96a2718bdc490359c850e1db839ea801e9");*/
            
         // Demo1
            /*connection = DriverManager.getConnection(
                    "jdbc:postgresql://ec2-34-231-57-138.compute-1.amazonaws.com/d2q8kv12a8nhoj?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory",
                    "u4b4taf81tli2p",
                    "p7001bc07336c386ec90dc2be72046a4587343c402c8f60db7ee743a1e533526e");*/

            System.out.println("Connection established !");
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

        return connection;
    }

    public void retrievePropertyTable(){
        try {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT property_code__c, sfid from salesforce.account where recordtypename__c = 'Property'");
            while (rs.next()) {
                propertyCodeKeyMap.put(rs.getString(1),rs.getString(2));
            }
            System.out.println("Properties retrieved !");
            rs.close();
            st.close();
        }catch(SQLException e){
            e.printStackTrace();
            return;
        }
    }

    public void retrieveMarketTable(){
        try {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT name, sfid from salesforce.market__c");
            while (rs.next()) {
                marketCodeKeyMap.put(rs.getString(1),rs.getString(2));
            }
            System.out.println("Markets retrieved !");
            rs.close();
            st.close();
        }catch(SQLException e){
            e.printStackTrace();
            return;
        }
    }

    public  void retrieveAccountTypeCodeTable(){
        try {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT name, sfid from salesforce.account_type_code__c");
            while (rs.next()) {
                accountTypeCodeKeyMap.put(rs.getString(1),rs.getString(2));
            }
            System.out.println("Account types retrieved !");
            rs.close();
            st.close();
        }catch(SQLException e){
            e.printStackTrace();
            return;
        }
    }

    public  void retrieveTierCodeTable(){
        try {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT name, sfid from salesforce.tier_code__c");
            while (rs.next()) {
                tierCodeKeyMap.put(rs.getString(1),rs.getString(2));
            }
            System.out.println("Tier codes retrieved !");
            rs.close();
            st.close();
        }catch(SQLException e){
            e.printStackTrace();
            return;
        }
    }

    public  void retrieveCampaignCodeTable(){
        try {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT name, sfid from salesforce.campaign_code__c");
            while (rs.next()) {
                campaignCodeKeyMap.put(rs.getString(1),rs.getString(2));
            }
            System.out.println("Campaign codes retrieved !");
            rs.close();
            st.close();
        }catch(SQLException e){
            e.printStackTrace();
            return;
        }
    }

    public  void retrieveCampaignTypeTable(){
        try {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT name, sfid from salesforce.campaign_type__c");
            while (rs.next()) {
                campaignTypeKeyMap.put(rs.getString(1),rs.getString(2));
            }
            System.out.println("Campaign types retrieved !");
            rs.close();
            st.close();
        }catch(SQLException e){
            e.printStackTrace();
            return;
        }
    }

    public  void retrieveContactTable(){
        try {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT winet_id__c, sfid from salesforce.contact");
            while (rs.next()) {
                contactMap.put(rs.getString(1),rs.getString(2));
            }
            System.out.println("Contacts retrieved !");
            rs.close();
            st.close();
        }catch(SQLException e){
            e.printStackTrace();
            return;
        }
    }

    public  void retrieveAssociateionReasonTable(){
        try {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT name, sfid from salesforce.association_reason__c");
            while (rs.next()) {
                associationReasonMap.put(rs.getString(1),rs.getString(2));
            }
            System.out.println("Contacts retrieved !");
            rs.close();
            st.close();
        }catch(SQLException e){
            e.printStackTrace();
            return;
        }
    }

    public  void retrieveMailCodeTable(){
        try {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT name, sfid from salesforce.mail_code_standard__c");
            while (rs.next()) {
                mailcodeMap.put(rs.getString(1),rs.getString(2));
            }
            System.out.println("Contacts retrieved !");
            rs.close();
            st.close();
        }catch(SQLException e){
            e.printStackTrace();
            return;
        }
    }

    public void retreiveSourceCodesMap(){
        try {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT c_prop_cd,c_source_cd,c_source_group, id from caesars.source_code ");
            while (rs.next()) {
                sourceCodesMap.put(rs.getString(1)+rs.getString(2)+rs.getString(3),rs.getString(4));
            }
            System.out.println("source codes retrieved !");
            rs.close();
            st.close();
        }catch(SQLException e){
            e.printStackTrace();
            return;
        }
    }

}
