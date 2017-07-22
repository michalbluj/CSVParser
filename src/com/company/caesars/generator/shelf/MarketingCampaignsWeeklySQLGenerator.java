package com.company.caesars.generator.shelf;

import com.company.caesars.generator.SQLGenerator;
import com.company.caesars.generator.SQLGeneratorBase;
import com.company.caesars.generator.concurrent.ConcurrentInsert;
import com.company.caesars.generator.concurrent.SQLInsertExecutor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Created by Michal Bluj on 2017-06-22.
 */
public class MarketingCampaignsWeeklySQLGenerator extends SQLGeneratorBase implements SQLGenerator {

    private static final String SEPARATOR = ",";

    private String readFilePath = "C://Users//Michal Bluj//Desktop//US1127/marketing_campaigns_wk.csv";
    private String writeFilePath = "C://Users//Michal Bluj//Downloads//UCR - marketing_campaigns weekly and LVM/marketing_campaigns_weekly insert.txt";
    private String insertStatement = "INSERT INTO caesars.marketing_campaigns_weekly (i_dmid,c_campaign_type_fk,c_campaign_type,c_campaign_type_desc,c_campaign_cd_fk,c_campaign_cd,c_campaign_desc,d_campaign_score_dt,c_version_cd,c_acct_type_cd_fk,c_acct_type_cd,c_prop_market_cd_fk,c_prop_market_cd,c_dom_pref_prop_cd,c_tier_cd_fk,c_tier_cd,c_marketing_state_cd,c_regulatory_state_cd,i_random_1,i_random_2,i_random_static_yr,c_main_ethnic_type,c_ethnic_group1,c_preferred_written_language,d_min_est_dt,c_prop_mail_cd,c_emailable,c_mailable,c_channel_pref,c_uk_res_supress,c_id_verified,c_age_18_plus,c_age_19_plus,c_age_21_plus,c_ucl_supp_flag,c_uci_supp_flag,c_game_pref,c_video_poker_pref,c_baccarat_pref,i_profitable_pct,c_convienence_cd,c_drive_distance_cluster,c_frequency,d_last_activity_dt,i_lodging_pct,f_worth,c_worth_break,c_channel_rec,c_dom_market_cd,c_gst_prop_dm_optout_flag,c_tdc_supp_flag,c_prop_cd_1_fk,c_prop_cd_1,i_prop_pct_worth_1,c_prop_cd_2_fk,c_prop_cd_2,i_prop_pct_worth_2,c_pref_prop_host_cd,c_host_type,i_age,c_gender,c_dom_cd_prop,c_pref_prop_cd,c_geo_dm_zone,c_geo_rpt_zone,c_distance_cluster,c_msa_cd,c_msa,c_zip_7_pdb,d_create_dt_mkt,d_create_dt_ent,f_act_all_ap_pdb_12mn_plus,f_theo_all_ap_pdb_12mn_plus,i_days_rtd_ap_pdb_12mn_plus,i_trp_rtd_ap_pdb_12mn_plus,f_daily_theo_ap_pdb_12mnplus,f_daily_act_ap_pdb_12mn_plus,f_daily_wrth_ap_pdb_12mnplus,f_resp_rate_ap_pdb_12mn_plus,f_ofr_trp_pct_ap_pdb12mnplus,f_htlsty_pct_ap_pdb_12mnplus,f_slot_pct_ap_pdb_12mn_plus,f_trp_all_adj_sc_pdb_12mn,f_trp_rtd_adj_sc_pdb_12mn,f_act_all_ap_pdb_24mn_plus,f_theo_all_ap_pdb_24mn_plus,i_days_rtd_ap_pdb_24mn_plus,i_trp_rtd_ap_pdb_24mn_plus,f_daily_theo_ap_pdb_24mnplus,f_daily_act_ap_pdb_24mn_plus,f_daily_wrth_ap_pdb_24mnplus,f_resp_rate_ap_pdb_24mn_plus,f_ofr_trp_pct_ap_pdb24mnplus,f_htlsty_pct_ap_pdb_24mnplus,f_slot_pct_ap_pdb_24mn_plus,f_trp_all_adj_sc_pdb_24mn,f_trp_rtd_adj_sc_pdb_24mn,f_trp_all_adj_sc_pdb_36mn,f_trp_rtd_adj_sc_pdb_36mn,f_profit_pct_ent,c_profit_flag_ent,f_profit_pct_mkt,c_profit_flag_mkt,i_age_adj_ah,i_dob_mn_adj_ah,i_dob_yr_adj_ah,c_country_cd_ah,c_credit_status_flag,c_mkt_host_cd_adj_ah,c_dom_prop_host_cd_adj_ah,c_prop_cd_1_host_cd_adj_ah,c_zip_7_ah,f_tier_score_ah,f_lodging_pct_mkt_adj_ah,f_pct_bacarrat_mkt_ah,f_pct_blackjack_mkt_ah,f_pct_craps_mkt_ah,f_pct_other_mkt_ah,f_pct_paigow_mkt_ah,f_pct_roulette_mkt_ah,f_pct_video_poker_mkt_ah,c_future_res_flag_mkt_ah,f_worth_1mn_ap_ah,f_worth_3mn_ap_ah,f_worth_6mn_ap_ah,f_worth_12mn_ap_ah,f_worth_all_ytd_ap_ah,f_theo_all_1mn_ap_ah,f_theo_all_3mn_ap_ah,f_theo_all_6mn_ap_ah,f_theo_all_12mn_ap_ah,f_theo_all_ytd_ap_ah,f_theo_slot_1mn_ap_ah,f_theo_slot_3mn_ap_ah,f_theo_slot_6mn_ap_ah,f_theo_slot_12mn_ap_ah,f_theo_table_1mn_ap_ah,f_theo_table_3mn_ap_ah,f_theo_table_6mn_ap_ah,f_theo_table_12mn_ap_ah,f_act_all_1mn_ap_ah,f_act_all_3mn_ap_ah,f_act_all_6mn_ap_ah,f_act_all_12mn_ap_ah,f_actual_all_ytd_ap_ah,f_avgbet_game_1mn_ap_ah,f_avgbet_game_3mn_ap_ah,f_avgbet_game_6mn_ap_ah,f_avgbet_game_12mn_ap_ah,i_lop_game_1mn_ap_ah,i_lop_game_3mn_ap_ah,i_lop_game_6mn_ap_ah,i_lop_game_12mn_ap_ah,i_trp_all_cms_1mn_ap_ah,i_trp_all_cms_3mn_ap_ah,i_trp_all_cms_6mn_ap_ah,i_trp_all_cms_12mn_ap_ah,i_trp_rtd_1mn_ap_ah,i_trp_rtd_3mn_ap_ah,i_trp_rtd_6mn_ap_ah,i_trp_rtd_12mn_ap_ah,i_days_rtd_1mn_ap_ah,i_days_rtd_3mn_ap_ah,i_days_rtd_6mn_ap_ah,i_days_rtd_12mn_ap_ah,d_last_rated_dt_mkt_ah,f_rc_earned_1mn_mkt_ah,f_rc_earned_3mn_mkt_ah,f_rc_earned_6mn_mkt_ah,f_rc_earned_12mn_mkt_ah,d_first_gaming_dt_mkt_ah,d_first_gaming_dt_ent_ah,d_first_dt_played_mkt_ah,f_act_all_cms_first_hr_m_ah,f_act_all_cms_first_day_m_ah,f_act_all_cms_first_trp_m_ah,i_lop_first_hr_mkt_ah,i_lop_first_day_mkt_ah,i_lop_first_trp_mkt_ah,c_convienence_cd_new,contact) VALUES";

    private static final String [] FILE_HEADER_MAPPING = {"i_dmid","c_campaign_type","c_campaign_type_desc","c_campaign_cd","c_campaign_desc","d_campaign_score_dt","c_version_cd","c_acct_type_cd","c_prop_market_cd","c_dom_pref_prop_cd","c_tier_cd","c_marketing_state_cd","c_regulatory_state_cd","i_random_1","i_random_2","i_random_static_yr","c_main_ethnic_type","c_ethnic_group1","c_preferred_written_language","d_min_est_dt","c_prop_mail_cd","c_emailable","c_mailable","c_channel_pref","c_uk_res_supress","c_id_verified","c_age_18_plus","c_age_19_plus","c_age_21_plus","c_ucl_supp_flag","c_uci_supp_flag","c_game_pref","c_video_poker_pref","c_baccarat_pref","i_profitable_pct","c_convienence_cd","c_drive_distance_cluster","c_frequency","d_last_activity_dt","i_lodging_pct","f_worth","c_worth_break","c_channel_rec","c_dom_market_cd","c_gst_prop_dm_optout_flag","c_tdc_supp_flag","c_prop_cd_1","i_prop_pct_worth_1","c_prop_cd_2","i_prop_pct_worth_2","c_pref_prop_host_cd","c_host_type","i_age","c_gender","c_dom_cd_prop","c_pref_prop_cd","c_geo_dm_zone","c_geo_rpt_zone","c_distance_cluster","c_msa_cd","c_msa","c_zip_7_pdb","d_create_dt_mkt","d_create_dt_ent","f_act_all_ap_pdb_12mn_plus","f_theo_all_ap_pdb_12mn_plus","i_days_rtd_ap_pdb_12mn_plus","i_trp_rtd_ap_pdb_12mn_plus","f_daily_theo_ap_pdb_12mnplus","f_daily_act_ap_pdb_12mn_plus","f_daily_wrth_ap_pdb_12mnplus","f_resp_rate_ap_pdb_12mn_plus","f_ofr_trp_pct_ap_pdb12mnplus","f_htlsty_pct_ap_pdb_12mnplus","f_slot_pct_ap_pdb_12mn_plus","f_trp_all_adj_sc_pdb_12mn","f_trp_rtd_adj_sc_pdb_12mn","f_act_all_ap_pdb_24mn_plus","f_theo_all_ap_pdb_24mn_plus","i_days_rtd_ap_pdb_24mn_plus","i_trp_rtd_ap_pdb_24mn_plus","f_daily_theo_ap_pdb_24mnplus","f_daily_act_ap_pdb_24mn_plus","f_daily_wrth_ap_pdb_24mnplus","f_resp_rate_ap_pdb_24mn_plus","f_ofr_trp_pct_ap_pdb24mnplus","f_htlsty_pct_ap_pdb_24mnplus","f_slot_pct_ap_pdb_24mn_plus","f_trp_all_adj_sc_pdb_24mn","f_trp_rtd_adj_sc_pdb_24mn","f_trp_all_adj_sc_pdb_36mn","f_trp_rtd_adj_sc_pdb_36mn","f_profit_pct_ent","c_profit_flag_ent","f_profit_pct_mkt","c_profit_flag_mkt","i_age_adj_ah","i_dob_mn_adj_ah","i_dob_yr_adj_ah","c_country_cd_ah","c_credit_status_flag","c_mkt_host_cd_adj_ah","c_dom_prop_host_cd_adj_ah","c_prop_cd_1_host_cd_adj_ah","c_zip_7_ah","f_tier_score_ah","f_lodging_pct_mkt_adj_ah","f_pct_bacarrat_mkt_ah","f_pct_blackjack_mkt_ah","f_pct_craps_mkt_ah","f_pct_other_mkt_ah","f_pct_paigow_mkt_ah","f_pct_roulette_mkt_ah","f_pct_video_poker_mkt_ah","c_future_res_flag_mkt_ah","f_worth_1mn_ap_ah","f_worth_3mn_ap_ah","f_worth_6mn_ap_ah","f_worth_12mn_ap_ah","f_worth_all_ytd_ap_ah","f_theo_all_1mn_ap_ah","f_theo_all_3mn_ap_ah","f_theo_all_6mn_ap_ah","f_theo_all_12mn_ap_ah","f_theo_all_ytd_ap_ah","f_theo_slot_1mn_ap_ah","f_theo_slot_3mn_ap_ah","f_theo_slot_6mn_ap_ah","f_theo_slot_12mn_ap_ah","f_theo_table_1mn_ap_ah","f_theo_table_3mn_ap_ah","f_theo_table_6mn_ap_ah","f_theo_table_12mn_ap_ah","f_act_all_1mn_ap_ah","f_act_all_3mn_ap_ah","f_act_all_6mn_ap_ah","f_act_all_12mn_ap_ah","f_actual_all_ytd_ap_ah","f_avgbet_game_1mn_ap_ah","f_avgbet_game_3mn_ap_ah","f_avgbet_game_6mn_ap_ah","f_avgbet_game_12mn_ap_ah","i_lop_game_1mn_ap_ah","i_lop_game_3mn_ap_ah","i_lop_game_6mn_ap_ah","i_lop_game_12mn_ap_ah","i_trp_all_cms_1mn_ap_ah","i_trp_all_cms_3mn_ap_ah","i_trp_all_cms_6mn_ap_ah","i_trp_all_cms_12mn_ap_ah","i_trp_rtd_1mn_ap_ah","i_trp_rtd_3mn_ap_ah","i_trp_rtd_6mn_ap_ah","i_trp_rtd_12mn_ap_ah","i_days_rtd_1mn_ap_ah","i_days_rtd_3mn_ap_ah","i_days_rtd_6mn_ap_ah","i_days_rtd_12mn_ap_ah","d_last_rated_dt_mkt_ah","f_rc_earned_1mn_mkt_ah","f_rc_earned_3mn_mkt_ah","f_rc_earned_6mn_mkt_ah","f_rc_earned_12mn_mkt_ah","d_first_gaming_dt_mkt_ah","d_first_gaming_dt_ent_ah","d_first_dt_played_mkt_ah","f_act_all_cms_first_hr_m_ah","f_act_all_cms_first_day_m_ah","f_act_all_cms_first_trp_m_ah","i_lop_first_hr_mkt_ah","i_lop_first_day_mkt_ah","i_lop_first_trp_mkt_ah","c_convienence_cd_new"};


    public MarketingCampaignsWeeklySQLGenerator(){}

    public MarketingCampaignsWeeklySQLGenerator(String readFilePath){}

    public MarketingCampaignsWeeklySQLGenerator(String readFilePath, String writeFilePath){
        this.readFilePath = readFilePath;
        this.writeFilePath = writeFilePath;
    }

    public void insertRecordsToDatabase() throws Exception{
    	Long start = System.currentTimeMillis();
        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(FILE_HEADER_MAPPING);


        retrieveCampaignCodeTable();
        retrieveCampaignTypeTable();
        retrievePropertyTable();
        retrieveTierCodeTable();
        retrieveMarketTable();
        retrieveAccountTypeCodeTable();

        FileReader fileReader = new FileReader(readFilePath);

        CSVParser csvFileParser = new CSVParser(fileReader, csvFileFormat);

        List<CSVRecord> csvRecords = csvFileParser.getRecords();

        Integer numberOfWorkers = 5;

        Map<Integer,String> statements = new HashMap<Integer,String>();
        for(Integer i = 0; i< numberOfWorkers ;i++){
            statements.put(i,"");
        }

        Integer counter = 0;
        for (int i = 1; i < csvRecords.size(); i++) {
            CSVRecord record = csvRecords.get(i);
            String generatedLine = generateInsertLine(record);
            statements.put(counter % numberOfWorkers, statements.get(counter % numberOfWorkers) + generatedLine);
            counter ++;
        }
        //connection = getShelfConnection();
        Executor executor = new SQLInsertExecutor();

        for(Integer key : statements.keySet()) {
            String stmt = insertStatement + statements.get(key);
            stmt = stmt.substring(0, stmt.length() - 1);
            executor.execute(new ConcurrentInsert(key, stmt, connection, start));
        }

    }

    private String generateInsertLine(CSVRecord record) {
        return "("+
                addNumericValue(record.get("i_dmid")) + SEPARATOR
                +addStringValue(campaignTypeKeyMap.get(record.get("c_campaign_type")))+ SEPARATOR
                +addStringValue(record.get("c_campaign_type")) + SEPARATOR
                +addStringValue(record.get("c_campaign_type_desc")) + SEPARATOR
                +addStringValue(campaignCodeKeyMap.get(record.get("c_campaign_cd"))) + SEPARATOR
                        +addStringValue(record.get("c_campaign_cd")) + SEPARATOR
                                +addStringValue(record.get("c_campaign_desc")) + SEPARATOR
                                        +addDateValue(record.get("d_campaign_score_dt")) + SEPARATOR
                                       + addStringValue(record.get("c_version_cd")) + SEPARATOR
                +addStringValue(accountTypeCodeKeyMap.get(record.get("c_acct_type_cd"))) + SEPARATOR
                                                +addStringValue(record.get("c_acct_type_cd")) + SEPARATOR
                +addStringValue(marketCodeKeyMap.get(record.get("c_prop_market_cd"))) + SEPARATOR
                                                        +addStringValue(record.get("c_prop_market_cd")) + SEPARATOR
                                                                +addStringValue(record.get("c_dom_pref_prop_cd")) + SEPARATOR
                +addStringValue(tierCodeKeyMap.get(record.get("c_tier_cd"))) + SEPARATOR
                                                                        +addStringValue(record.get("c_tier_cd")) + SEPARATOR
                                                                                +addStringValue(record.get("c_marketing_state_cd")) + SEPARATOR
                                                                                        +addStringValue(record.get("c_regulatory_state_cd")) + SEPARATOR
        +addNumericValue(record.get("i_random_1")) + SEPARATOR
                +addNumericValue(record.get("i_random_2")) + SEPARATOR
                        +addNumericValue(record.get("i_random_static_yr")) + SEPARATOR
                                                                                                +addStringValue(record.get("c_main_ethnic_type")) + SEPARATOR
                                                                                                        +addStringValue(record.get("c_ethnic_group1")) + SEPARATOR
                                                                                                                +addStringValue(record.get("c_preferred_written_language")) + SEPARATOR
                                                                                                                        +addDateValue(record.get("d_min_est_dt")) + SEPARATOR
                                                                                                                        +addStringValue(record.get("c_prop_mail_cd")) + SEPARATOR
                                                                                                                                +addStringValue(record.get("c_emailable")) + SEPARATOR
                                                                                                                                        +addStringValue(record.get("c_mailable")) + SEPARATOR
                                                                                                                                                +addStringValue(record.get("c_channel_pref")) + SEPARATOR
                                                                                                                                                        +addStringValue(record.get("c_uk_res_supress")) + SEPARATOR
                                                                                                                                                                +addStringValue(record.get("c_id_verified")) + SEPARATOR
                                                                                                                                                                        +addStringValue(record.get("c_age_18_plus")) + SEPARATOR
                                                                                                                                                                                +addStringValue(record.get("c_age_19_plus")) + SEPARATOR
                                                                                                                                                                                        +addStringValue(record.get("c_age_21_plus")) + SEPARATOR
                                                                                                                                                                                                +addStringValue(record.get("c_ucl_supp_flag")) + SEPARATOR
        +addStringValue(record.get("c_uci_supp_flag")) + SEPARATOR
                +addStringValue(record.get("c_game_pref")) + SEPARATOR
                        +addStringValue(record.get("c_video_poker_pref")) + SEPARATOR
                                +addStringValue(record.get("c_baccarat_pref")) + SEPARATOR
                                +addNumericValue(record.get("i_profitable_pct")) + SEPARATOR
                                        +addStringValue(record.get("c_convienence_cd")) + SEPARATOR
                                                +addStringValue(record.get("c_drive_distance_cluster")) + SEPARATOR
                                                        +addStringValue(record.get("c_frequency")) + SEPARATOR
                                                                +addDateValue(record.get("d_last_activity_dt")) + SEPARATOR
                                        +addNumericValue(record.get("i_lodging_pct")) + SEPARATOR
                                                +addNumericValue(record.get("f_worth")) + SEPARATOR
                                                                +addStringValue(record.get("c_worth_break")) + SEPARATOR
                                                                        +addStringValue(record.get("c_channel_rec")) + SEPARATOR
                                                                                +addStringValue(record.get("c_dom_market_cd")) + SEPARATOR
                                                                                        +addStringValue(record.get("c_gst_prop_dm_optout_flag")) + SEPARATOR
                                                                                                +addStringValue(record.get("c_tdc_supp_flag")) + SEPARATOR
                +addStringValue(propertyCodeKeyMap.get(record.get("c_prop_cd_1"))) + SEPARATOR
                                                                                                        +addStringValue(record.get("c_prop_cd_1")) + SEPARATOR
                                                        +addNumericValue(record.get("i_prop_pct_worth_1")) + SEPARATOR
                +addStringValue(propertyCodeKeyMap.get(record.get("c_prop_cd_2"))) + SEPARATOR
                                                                                                                +addStringValue(record.get("c_prop_cd_2")) + SEPARATOR
                                                                +addNumericValue(record.get("i_prop_pct_worth_2")) + SEPARATOR
                                                                                                                        +addStringValue(record.get("c_pref_prop_host_cd")) + SEPARATOR
                                                                                                                                +addStringValue(record.get("c_host_type")) + SEPARATOR
                                                                        +addNumericValue(record.get("i_age")) + SEPARATOR
                                                                                                                                        +addStringValue(record.get("c_gender")) + SEPARATOR
                                                                                                                                                +addStringValue(record.get("c_dom_cd_prop")) + SEPARATOR
                                                                                                                                                        +addStringValue(record.get("c_pref_prop_cd")) + SEPARATOR
                                                                                                                                                                +addStringValue(record.get("c_geo_dm_zone")) + SEPARATOR
                                                                                                                                                                        +addStringValue(record.get("c_geo_rpt_zone")) + SEPARATOR
                                                                                                                                                                                +addStringValue(record.get("c_distance_cluster")) + SEPARATOR
                                                                                                                                                                                        +addStringValue(record.get("c_msa_cd")) + SEPARATOR
                                                                                                                                                                                                +addStringValue(record.get("c_msa")) + SEPARATOR
                                                                                                                                                                                                        +addStringValue(record.get("c_zip_7_pdb")) + SEPARATOR
                                                                                                                                                                                                                +addDateValue(record.get("d_create_dt_mkt")) + SEPARATOR
                                                                                                                                                                                                                +addDateValue(record.get("d_create_dt_ent")) + SEPARATOR
                                                                                +addNumericValue(record.get("f_act_all_ap_pdb_12mn_plus")) + SEPARATOR
                                                                                        +addNumericValue(record.get("f_theo_all_ap_pdb_12mn_plus")) + SEPARATOR
                                                                                                +addNumericValue(record.get("i_days_rtd_ap_pdb_12mn_plus")) + SEPARATOR
                                                                                                        +addNumericValue(record.get("i_trp_rtd_ap_pdb_12mn_plus")) + SEPARATOR
                                                                                                                +addNumericValue(record.get("f_daily_theo_ap_pdb_12mnplus")) + SEPARATOR
                                                                                                                        +addNumericValue(record.get("f_daily_act_ap_pdb_12mn_plus")) + SEPARATOR
                                                                                                                                +addNumericValue(record.get("f_daily_wrth_ap_pdb_12mnplus")) + SEPARATOR
                                                                                                                                        +addNumericValue(record.get("f_resp_rate_ap_pdb_12mn_plus")) + SEPARATOR
                                                                                                                                                +addNumericValue(record.get("f_ofr_trp_pct_ap_pdb12mnplus")) + SEPARATOR
                                                                                                                                                        +addNumericValue(record.get("f_htlsty_pct_ap_pdb_12mnplus")) + SEPARATOR
                                                                                                                                                                +addNumericValue(record.get("f_slot_pct_ap_pdb_12mn_plus")) + SEPARATOR
                                                                                                                                                                        +addNumericValue(record.get("f_trp_all_adj_sc_pdb_12mn")) + SEPARATOR
                                                                                                                                                                                +addNumericValue(record.get("f_trp_rtd_adj_sc_pdb_12mn")) + SEPARATOR
                                                                                                                                                                                        +addNumericValue(record.get("f_act_all_ap_pdb_24mn_plus")) + SEPARATOR
                                                                                                                                                                                                +addNumericValue(record.get("f_theo_all_ap_pdb_24mn_plus")) + SEPARATOR
                                                                                                                                                                                                        +addNumericValue(record.get("i_days_rtd_ap_pdb_24mn_plus")) + SEPARATOR
                                                                                                                                                                                                                +addNumericValue(record.get("i_trp_rtd_ap_pdb_24mn_plus")) + SEPARATOR
                                                                                +addNumericValue(record.get("f_daily_theo_ap_pdb_24mnplus")) + SEPARATOR
                                                                        +addNumericValue(record.get("f_daily_act_ap_pdb_24mn_plus")) + SEPARATOR
                                                                +addNumericValue(record.get("f_daily_wrth_ap_pdb_24mnplus")) + SEPARATOR
                                                        +addNumericValue(record.get("f_resp_rate_ap_pdb_24mn_plus")) + SEPARATOR
                                                +addNumericValue(record.get("f_ofr_trp_pct_ap_pdb24mnplus")) + SEPARATOR
                                        +addNumericValue(record.get("f_htlsty_pct_ap_pdb_24mnplus")) + SEPARATOR
                                +addNumericValue(record.get("f_slot_pct_ap_pdb_24mn_plus")) + SEPARATOR
                        +addNumericValue(record.get("f_trp_all_adj_sc_pdb_24mn")) + SEPARATOR
                +addNumericValue(record.get("f_trp_rtd_adj_sc_pdb_24mn")) + SEPARATOR
                +addNumericValue(record.get("f_trp_all_adj_sc_pdb_36mn")) + SEPARATOR
                +addNumericValue(record.get("f_trp_rtd_adj_sc_pdb_36mn")) + SEPARATOR
                +addNumericValue(record.get("f_profit_pct_ent")) + SEPARATOR
        +addStringValue(record.get("c_profit_flag_ent")) + SEPARATOR
                +addNumericValue(record.get("f_profit_pct_mkt")) + SEPARATOR
                +addStringValue(record.get("c_profit_flag_mkt")) + SEPARATOR
                        +addNumericValue(record.get("i_age_adj_ah")) + SEPARATOR
                +addNumericValue(record.get("i_dob_mn_adj_ah")) + SEPARATOR
                +addNumericValue(record.get("i_dob_yr_adj_ah")) + SEPARATOR
                        +addStringValue(record.get("c_country_cd_ah")) + SEPARATOR
                                +addStringValue(record.get("c_credit_status_flag")) + SEPARATOR
                                        +addStringValue(record.get("c_mkt_host_cd_adj_ah")) + SEPARATOR
                                                +addStringValue(record.get("c_dom_prop_host_cd_adj_ah")) + SEPARATOR
                                                        +addStringValue(record.get("c_prop_cd_1_host_cd_adj_ah")) + SEPARATOR
                                                                +addStringValue(record.get("c_zip_7_ah")) + SEPARATOR
                                                                        +addNumericValue(record.get("f_tier_score_ah")) + SEPARATOR
                +addNumericValue(record.get("f_lodging_pct_mkt_adj_ah")) + SEPARATOR
                +addNumericValue(record.get("f_pct_bacarrat_mkt_ah")) + SEPARATOR
                +addNumericValue(record.get("f_pct_blackjack_mkt_ah")) + SEPARATOR
                +addNumericValue(record.get("f_pct_craps_mkt_ah")) + SEPARATOR
                +addNumericValue(record.get("f_pct_other_mkt_ah")) + SEPARATOR
                +addNumericValue(record.get("f_pct_paigow_mkt_ah")) + SEPARATOR
                +addNumericValue(record.get("f_pct_roulette_mkt_ah")) + SEPARATOR
                +addNumericValue(record.get("f_pct_video_poker_mkt_ah")) + SEPARATOR
                                                                        +addStringValue(record.get("c_future_res_flag_mkt_ah")) + SEPARATOR
                                                                                +addNumericValue(record.get("f_worth_1mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_worth_3mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_worth_6mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_worth_12mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_worth_all_ytd_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_theo_all_1mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_theo_all_3mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_theo_all_6mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_theo_all_12mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_theo_all_ytd_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_theo_slot_1mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_theo_slot_3mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_theo_slot_6mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_theo_slot_12mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_theo_table_1mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_theo_table_3mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_theo_table_6mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_theo_table_12mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_act_all_1mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_act_all_3mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_act_all_6mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_act_all_12mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_actual_all_ytd_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_avgbet_game_1mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_avgbet_game_3mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_avgbet_game_6mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("f_avgbet_game_12mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("i_lop_game_1mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("i_lop_game_3mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("i_lop_game_6mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("i_lop_game_12mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("i_trp_all_cms_1mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("i_trp_all_cms_3mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("i_trp_all_cms_6mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("i_trp_all_cms_12mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("i_trp_rtd_1mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("i_trp_rtd_3mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("i_trp_rtd_6mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("i_trp_rtd_12mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("i_days_rtd_1mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("i_days_rtd_3mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("i_days_rtd_6mn_ap_ah")) + SEPARATOR
                +addNumericValue(record.get("i_days_rtd_12mn_ap_ah")) + SEPARATOR
                                                                                +addDateValue(record.get("d_last_rated_dt_mkt_ah")) + SEPARATOR
                +addNumericValue(record.get("f_rc_earned_1mn_mkt_ah")) + SEPARATOR
                +addNumericValue(record.get("f_rc_earned_3mn_mkt_ah")) + SEPARATOR
                +addNumericValue(record.get("f_rc_earned_6mn_mkt_ah")) + SEPARATOR
                +addNumericValue(record.get("f_rc_earned_12mn_mkt_ah")) + SEPARATOR
                +addDateValue(record.get("d_first_gaming_dt_mkt_ah")) + SEPARATOR
                +addDateValue(record.get("d_first_gaming_dt_ent_ah")) + SEPARATOR
                +addDateValue(record.get("d_first_dt_played_mkt_ah")) + SEPARATOR
                +addNumericValue(record.get("f_act_all_cms_first_hr_m_ah")) + SEPARATOR
                +addNumericValue(record.get("f_act_all_cms_first_day_m_ah")) + SEPARATOR
                +addNumericValue(record.get("f_act_all_cms_first_trp_m_ah")) + SEPARATOR
                +addNumericValue(record.get("i_lop_first_hr_mkt_ah")) + SEPARATOR
                +addNumericValue(record.get("i_lop_first_day_mkt_ah")) + SEPARATOR
        +addNumericValue(record.get("i_lop_first_trp_mkt_ah")) + SEPARATOR
         +addStringValue(record.get("c_convienence_cd_new")) + SEPARATOR
                +addStringValue(contactMap.get(record.get("i_dmid"))) +
                "),";

    }

}