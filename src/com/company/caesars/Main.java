package com.company.caesars;

import com.company.caesars.generator.WinetIdSQLGenerator;

public class Main {

    public static void main(String[] args) throws Exception {
    	
    	WinetIdSQLGenerator generator = new WinetIdSQLGenerator();
    	generator.insertRecordsToDatabase();
    	
    	/*ExecutorService executor = Executors.newFixedThreadPool(1);
    	
    	ConcurrentProcessor proc1 = new ConcurrentProcessor(new EnterpriseCampaigns12SQLGenerator("D://Caesars/gst_market_sum_12mo_view-005.txt"));
    	ConcurrentProcessor proc2 = new ConcurrentProcessor(new EnterpriseCampaigns12SQLGenerator("D://Caesars/gst_market_sum_12mo_view-006.txt"));
    	ConcurrentProcessor proc3 = new ConcurrentProcessor(new EnterpriseCampaigns12SQLGenerator("D://Caesars/gst_market_sum_12mo_view-007.txt"));
    	ConcurrentProcessor proc4 = new ConcurrentProcessor(new EnterpriseCampaigns12SQLGenerator("D://Caesars/gst_market_sum_12mo_view-008.txt"));
    	ConcurrentProcessor proc5 = new ConcurrentProcessor(new EnterpriseCampaigns12SQLGenerator("D://Caesars/gst_market_sum_12mo_view-009.txt"));
    	*/
    	
    	//ConcurrentProcessor proc5 = new ConcurrentProcessor(new EnterpriseCampaignsSQLGenerator("D://Caesars/gst_market_sum_24mo_view-016.txt"));
    	/*ConcurrentProcessor proc2 = new ConcurrentProcessor(new MarketingLVMSQLGenerator("D://Caesars//marketing_lvm_split/marketing_lvm__82.txt"));
    	ConcurrentProcessor proc3 = new ConcurrentProcessor(new MarketingLVMSQLGenerator("D://Caesars//marketing_lvm_split/marketing_lvm__83.txt"));
    	ConcurrentProcessor proc4 = new ConcurrentProcessor(new MarketingLVMSQLGenerator("D://Caesars//marketing_lvm_split/marketing_lvm__84.txt"));
    	ConcurrentProcessor proc5 = new ConcurrentProcessor(new MarketingLVMSQLGenerator("D://Caesars//marketing_lvm_split/marketing_lvm__85.txt"));
    	
    	ConcurrentProcessor proc6 = new ConcurrentProcessor(new MarketingLVMSQLGenerator("D://Caesars//marketing_lvm_split/marketing_lvm__86.txt"));
    	ConcurrentProcessor proc7 = new ConcurrentProcessor(new MarketingLVMSQLGenerator("D://Caesars//marketing_lvm_split/marketing_lvm__87.txt"));
    	ConcurrentProcessor proc8 = new ConcurrentProcessor(new MarketingLVMSQLGenerator("D://Caesars//marketing_lvm_split/marketing_lvm__88.txt"));
    	ConcurrentProcessor proc9 = new ConcurrentProcessor(new MarketingLVMSQLGenerator("D://Caesars//marketing_lvm_split/marketing_lvm__89.txt"));
    	ConcurrentProcessor proc10 = new ConcurrentProcessor(new MarketingLVMSQLGenerator("D://Caesars//marketing_lvm_split/marketing_lvm__90.txt"));
    	
    	ConcurrentProcessor proc11 = new ConcurrentProcessor(new MarketingLVMSQLGenerator("D://Caesars//marketing_lvm_split/marketing_lvm__91.txt"));
    	ConcurrentProcessor proc12 = new ConcurrentProcessor(new MarketingLVMSQLGenerator("D://Caesars//marketing_lvm_split/marketing_lvm__92.txt"));
    	ConcurrentProcessor proc13 = new ConcurrentProcessor(new MarketingLVMSQLGenerator("D://Caesars//marketing_lvm_split/marketing_lvm__93.txt"));
    	ConcurrentProcessor proc14 = new ConcurrentProcessor(new MarketingLVMSQLGenerator("D://Caesars//marketing_lvm_split/marketing_lvm__94.txt"));
    	ConcurrentProcessor proc15 = new ConcurrentProcessor(new MarketingLVMSQLGenerator("D://Caesars//marketing_lvm_split/marketing_lvm__95.txt"));
    	
    	ConcurrentProcessor proc16 = new ConcurrentProcessor(new MarketingLVMSQLGenerator("D://Caesars//marketing_lvm_split/marketing_lvm__96.txt"));
    	ConcurrentProcessor proc17 = new ConcurrentProcessor(new MarketingLVMSQLGenerator("D://Caesars//marketing_lvm_split/marketing_lvm__97.txt"));
    	ConcurrentProcessor proc18 = new ConcurrentProcessor(new MarketingLVMSQLGenerator("D://Caesars//marketing_lvm_split/marketing_lvm__98.txt"));*/
    	
    	/*ConcurrentProcessor proc11 = new ConcurrentProcessor(new MarketingCampaignsSQLGenerator("D://Caesars//marketing_campaigns_split//marketing_cmapaigns_split/marketing_campaigns__25-000.txt"));
    	ConcurrentProcessor proc12 = new ConcurrentProcessor(new MarketingCampaignsSQLGenerator("D://Caesars//marketing_campaigns_split//marketing_cmapaigns_split/marketing_campaigns__25-001.txt"));
    	ConcurrentProcessor proc13 = new ConcurrentProcessor(new MarketingCampaignsSQLGenerator("D://Caesars//marketing_campaigns_split//marketing_cmapaigns_split/marketing_campaigns__25-002.txt"));
    	ConcurrentProcessor proc14 = new ConcurrentProcessor(new MarketingCampaignsSQLGenerator("D://Caesars//marketing_campaigns_split//marketing_cmapaigns_split/marketing_campaigns__25-003.txt"));
    	ConcurrentProcessor proc15 = new ConcurrentProcessor(new MarketingCampaignsSQLGenerator("D://Caesars//marketing_campaigns_split//marketing_cmapaigns_split/marketing_campaigns__25-004.txt"));
    	
    	ConcurrentProcessor proc16 = new ConcurrentProcessor(new MarketingCampaignsSQLGenerator("D://Caesars//marketing_campaigns_split//marketing_cmapaigns_split/marketing_campaigns__26-000.txt"));
    	ConcurrentProcessor proc17 = new ConcurrentProcessor(new MarketingCampaignsSQLGenerator("D://Caesars//marketing_campaigns_split//marketing_cmapaigns_split/marketing_campaigns__26-001.txt"));
    	ConcurrentProcessor proc18 = new ConcurrentProcessor(new MarketingCampaignsSQLGenerator("D://Caesars//marketing_campaigns_split//marketing_cmapaigns_split/marketing_campaigns__26-002.txt"));
    	ConcurrentProcessor proc19 = new ConcurrentProcessor(new MarketingCampaignsSQLGenerator("D://Caesars//marketing_campaigns_split//marketing_cmapaigns_split/marketing_campaigns__26-003.txt"));
    	ConcurrentProcessor proc20 = new ConcurrentProcessor(new MarketingCampaignsSQLGenerator("D://Caesars//marketing_campaigns_split//marketing_cmapaigns_split/marketing_campaigns__26-004.txt"));
    	*/
    	/*executor.execute(proc1);
    	executor.execute(proc2);
    	executor.execute(proc3);
    	executor.execute(proc4);
    	executor.execute(proc5);*/
    	
    	/*executor.execute(proc6);
    	executor.execute(proc7);
    	executor.execute(proc8);
    	executor.execute(proc9);
    	executor.execute(proc10);
    	
    	executor.execute(proc11);
    	executor.execute(proc12);
    	executor.execute(proc13);
    	executor.execute(proc14);
    	executor.execute(proc15);
    	
    	executor.execute(proc16);
    	executor.execute(proc17);
    	executor.execute(proc18);*/
    	
        //executor.shutdown();
    	
        /*SQLGenerator generator = new ContactSQLGenerator(); // place generator implementation here.
        try {
            generator.insertRecordsToDatabase();
        }catch(Exception e){
            e.printStackTrace();
        }*/
    }
}
