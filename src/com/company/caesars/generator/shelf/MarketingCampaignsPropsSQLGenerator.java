package com.company.caesars.generator.shelf;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.company.caesars.generator.SQLGenerator;
import com.company.caesars.generator.SQLGeneratorBase;

import java.io.File;
import java.io.FileReader;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * Created by Michal Bluj on 2017-06-22.
 */
public class MarketingCampaignsPropsSQLGenerator extends SQLGeneratorBase implements SQLGenerator {

    private final NumberFormat nf = NumberFormat.getNumberInstance(Locale.US);
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    private String readFilePath = "D://Caesars//marketing_campaigns_prop_split/marketing_campaigns_prop__01-000.txt";

    private final String insertStatement = "INSERT INTO caesars.marketing_campaigns_properties " +
            "(i_dmid, c_campaign_type,c_campaign_type_desc,c_campaign_cd_fk,c_campaign_desc,d_campaign_score_dt," +
            "c_version_cd,c_acct_type_cd_fk,c_prop_market_cd_fk,c_dom_pref_prop_cd,c_tier_cd_fk,c_marketing_state_cd," +
            "c_regulatory_state_cd,i_random_1,i_random_2,i_random_static_yr,c_main_ethnic_type,c_ethnic_group1," +
            "c_preferred_written_language,d_min_est_dt,c_prop_mail_cd,c_emailable,c_mailable,c_channel_pref," +
            "c_uk_res_supress,c_id_verified,c_age_18_plus,c_age_19_plus,c_age_21_plus,c_ucl_supp_flag,c_uci_supp_flag," +
            "c_game_pref,c_video_poker_pref,c_baccarat_pref,i_profitable_pct,c_convienence_cd,c_drive_distance_cluster," +
            "c_frequency,d_last_activity_dt,i_lodging_pct,f_worth,c_worth_break,c_channel_rec,c_dom_market_cd," +
            "c_gst_pref_mail_flag,c_tdc_supp_flag,c_prop_cd_1,i_prop_pct_worth_1,c_prop_cd_2,i_prop_pct_worth_2," +
            "c_pref_prop_host_cd,c_host_type,i_age,c_gender,c_dom_cd_prop,c_pref_prop_cd,c_geo_dm_zone,c_geo_rpt_zone," +
            "c_distance_cluster,c_msa_cd,c_msa,c_zip_7_pdb,d_create_dt_mkt,d_create_dt_ent,f_act_all_ap_pdb_12mn_plus," +
            "f_theo_all_ap_pdb_12mn_plus,i_days_rtd_ap_pdb_12mn_plus,i_trp_rtd_ap_pdb_12mn_plus,f_daily_theo_ap_pdb_12mnplus," +
            "f_daily_act_ap_pdb_12mn_plus,f_daily_wrth_ap_pdb_12mnplus,f_resp_rate_ap_pdb_12mn_plus,f_ofr_trp_pct_ap_pdb12mnplus," +
            "f_htlsty_pct_ap_pdb_12mnplus,f_slot_pct_ap_pdb_12mn_plus,f_trp_all_adj_sc_pdb_12mn,f_trp_rtd_adj_sc_pdb_12mn," +
            "f_act_all_ap_pdb_24mn_plus,f_theo_all_ap_pdb_24mn_plus,i_days_rtd_ap_pdb_24mn_plus,i_trp_rtd_ap_pdb_24mn_plus," +
            "f_daily_theo_ap_pdb_24mnplus,f_daily_act_ap_pdb_24mn_plus,f_daily_wrth_ap_pdb_24mnplus," +
            "f_resp_rate_ap_pdb_24mn_plus,f_ofr_trp_pct_ap_pdb24mnplus,f_htlsty_pct_ap_pdb_24mnplus," +
            "f_slot_pct_ap_pdb_24mn_plus,f_trp_all_adj_sc_pdb_24mn,f_trp_rtd_adj_sc_pdb_24mn,f_trp_all_adj_sc_pdb_36mn," +
            "f_trp_rtd_adj_sc_pdb_36mn,f_profit_pct_ent,c_profit_flag_ent,f_profit_pct_mkt,c_profit_flag_mkt,i_age_adj_ah," +
            "i_dob_mn_adj_ah,i_dob_yr_adj_ah,c_country_cd_ah,c_credit_suppress_flag_m_ah,c_mkt_host_cd_adj_ah," +
            "c_dom_prop_host_cd_adj_ah,c_prop_cd_1_host_cd_adj_ah,c_zip_7_ah,f_tier_score_ah,f_lodging_pct_mkt_adj_ah," +
            "f_pct_bacarrat_mkt_ah,f_pct_blackjack_mkt_ah,f_pct_craps_mkt_ah,f_pct_other_mkt_ah,f_pct_paigow_mkt_ah," +
            "f_pct_roulette_mkt_ah,f_pct_video_poker_mkt_ah,c_future_res_flag_mkt_ah,f_worth_1mn_ap_ah,f_worth_3mn_ap_ah," +
            "f_worth_6mn_ap_ah,f_worth_12mn_ap_ah,f_worth_all_ytd_ap_ah,f_theo_all_1mn_ap_ah,f_theo_all_3mn_ap_ah," +
            "f_theo_all_6mn_ap_ah,f_theo_all_12mn_ap_ah,f_theo_all_ytd_ap_ah,f_theo_slot_1mn_ap_ah,f_theo_slot_3mn_ap_ah," +
            "f_theo_slot_6mn_ap_ah,f_theo_slot_12mn_ap_ah,f_theo_table_1mn_ap_ah,f_theo_table_3mn_ap_ah," +
            "f_theo_table_6mn_ap_ah,f_theo_table_12mn_ap_ah,f_act_all_1mn_ap_ah,f_act_all_3mn_ap_ah,f_act_all_6mn_ap_ah," +
            "f_act_all_12mn_ap_ah,f_actual_all_ytd_ap_ah,f_avgbet_game_1mn_ap_ah,f_avgbet_game_3mn_ap_ah," +
            "f_avgbet_game_6mn_ap_ah,f_avgbet_game_12mn_ap_ah,i_lop_game_1mn_ap_ah,i_lop_game_3mn_ap_ah," +
            "i_lop_game_6mn_ap_ah,i_lop_game_12mn_ap_ah,i_trp_all_cms_1mn_ap_ah,i_trp_all_cms_3mn_ap_ah," +
            "i_trp_all_cms_6mn_ap_ah,i_trp_all_cms_12mn_ap_ah,i_trp_rtd_1mn_ap_ah,i_trp_rtd_3mn_ap_ah," +
            "i_trp_rtd_6mn_ap_ah,i_trp_rtd_12mn_ap_ah,i_days_rtd_1mn_ap_ah,i_days_rtd_3mn_ap_ah,i_days_rtd_6mn_ap_ah," +
            "i_days_rtd_12mn_ap_ah,d_last_rated_dt_mkt_ah,f_rc_earned_1mn_mkt_ah,f_rc_earned_3mn_mkt_ah," +
            "f_rc_earned_6mn_mkt_ah,f_rc_earned_12mn_mkt_ah,d_first_gaming_dt_mkt_ah,d_first_gaming_dt_ent_ah," +
            "d_first_dt_played_mkt_ah,f_act_all_cms_first_hr_m_ah,f_act_all_cms_first_day_m_ah,f_act_all_cms_first_trp_m_ah," +
            "i_lop_first_hr_mkt_ah,i_lop_first_day_mkt_ah,i_lop_first_trp_mkt_ah,c_convienence_cd_new,c_prop_cd," +
            "f_property_worth,d_last_activity_dt_prop,c_property_frequency,c_recency,c_frequency_tc,c_frequency_ttm_24," +
            "c_tierscore_desc,f_hotel_adw,c_hotel_adw_break,c_frequency_ttm,c_avg_day_hotel_flag,i_days_since_last," +
            "f_trip_cycle_48,c_frequency_tc_12,f_trip_cycle_12,i_ttm_trip_cnt_12,i_ttm_trip_cnt_24,c_adw_trip_break_12," +
            "f_adw_trip_12,c_adw_trip_break_6,f_adw_trip_6,c_adw_break_48,f_adw_48,c_adw_break_24,f_adw_24,c_adw_break_15," +
            "f_adw_15,c_adw_break_12,f_adw_12,c_hotel_adw_break_48,f_hotel_adw_48,c_hotel_adw_break_24,f_hotel_adw_24," +
            "c_hotel_adw_break_15,f_hotel_adw_15,c_hotel_adw_break_12,f_hotel_adw_12,c_last_gaming_trip," +
            "i_days_since_gaming,i_email_decile)" +
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    public MarketingCampaignsPropsSQLGenerator() {
    }

    public MarketingCampaignsPropsSQLGenerator(String readFilePath) {
    	this.readFilePath = readFilePath;
    }

    public MarketingCampaignsPropsSQLGenerator(String readFilePath, String writeFilePath) {
        this.readFilePath = readFilePath;

    }

    private Integer getInt(String s) throws ParseException {
    	if(s != null && s.trim().equals("")) return null;
        return isInvalid(s) ? null : nf.parse(s.trim()).intValue();
    }

    private Double getDouble(String s) throws ParseException {
        return isInvalid(s) ? null : nf.parse(s.trim()).doubleValue();
    }

    private String getStr(String s) {
        return isInvalid(s) ? null : s.trim();
    }

    private Date getDate(String s) throws ParseException {
    	if(s != null && s.trim().equals("")) return null;
        return isInvalid(s) ? null : new Date(dateFormat.parse(s.trim()).getTime());
    }

    private Long getLong(String s) throws ParseException {
    	if(s != null && s.trim().equals("")) return null;
        return isInvalid(s) ? null : nf.parse(s.trim()).longValue();
    }

    private boolean isInvalid(String s) {
        if (s == null) {
            return true;
        }
        return "NULL".equals(s) || "?".equals(s) || "empty".equals(s);
    }

    public void insertRecordsToDatabase() throws Exception {
    	long start = System.currentTimeMillis();
        retrieveCampaignCodeTable();
        retrieveCampaignTypeTable();
        retrievePropertyTable();
        retrieveTierCodeTable();
        retrieveMarketTable();
        retrieveAccountTypeCodeTable();

        FileReader fileReader = new FileReader(new File(readFilePath));
        CSVFormat csvFileFormat =  CSVFormat.newFormat('|').withHeader();
        Iterable<CSVRecord> records = csvFileFormat.parse(fileReader);
        connection = getShelfConnection();
        final int batchSize = 500;
        int count = 0;
        PSWrapper ps = new PSWrapper(connection.prepareStatement(insertStatement));
        for (CSVRecord record : records) {
            ps.setString(1, String.valueOf(getLong(record.get("i_dmid"))));
            ps.setString(2, getStr(record.get("c_campaign_type")));
            ps.setString(3, getStr(record.get("c_campaign_type_desc")));
            ps.setString(4, getStr(campaignCodeKeyMap.get(record.get("c_campaign_cd"))));
            ps.setString(5, getStr(record.get("c_campaign_desc")));
            ps.setDate(6, getDate(record.get("d_campaign_score_dt")));
            ps.setInt(7, getInt(record.get("c_version_cd")));
            ps.setString(8, getStr(accountTypeCodeKeyMap.get(record.get("c_acct_type_cd"))));
            ps.setString(9, getStr(marketCodeKeyMap.get(record.get("c_prop_market_cd"))));
            ps.setString(10, getStr(record.get("c_dom_pref_prop_cd")));
            ps.setString(11, getStr(tierCodeKeyMap.get(record.get("c_tier_cd"))));
            ps.setString(12, getStr(record.get("c_marketing_state_cd")));
            ps.setString(13, getStr(record.get("c_regulatory_state_cd")));
            ps.setInt(14, getInt(record.get("i_random_1")));
            ps.setInt(15, getInt(record.get("i_random_2")));
            ps.setInt(16, getInt(record.get("i_random_static_yr")));
            ps.setString(17, getStr(record.get("c_main_ethnic_type")));
            ps.setString(18, getStr(record.get("c_ethnic_group1")));
            ps.setString(19, getStr(record.get("c_preferred_written_language")));
            ps.setDate(20, getDate(record.get("d_min_est_dt")));
            ps.setString(21, getStr(record.get("c_prop_mail_cd")));
            ps.setString(22, getStr(record.get("c_emailable")));
            ps.setString(23, getStr(record.get("c_mailable")));
            ps.setString(24, getStr(record.get("c_channel_pref")));
            ps.setString(25, getStr(record.get("c_uk_res_supress")));
            ps.setString(26, getStr(record.get("c_id_verified")));
            ps.setString(27, getStr(record.get("c_age_18_plus")));
            ps.setString(28, getStr(record.get("c_age_19_plus")));
            ps.setString(29, getStr(record.get("c_age_21_plus")));
            ps.setString(30, getStr(record.get("c_ucl_supp_flag")));
            ps.setString(31, getStr(record.get("c_uci_supp_flag")));
            ps.setString(32, getStr(record.get("c_game_pref")));
            ps.setString(33, getStr(record.get("c_video_poker_pref")));
            ps.setString(34, getStr(record.get("c_baccarat_pref")));
            ps.setInt(35, getInt(record.get("i_profitable_pct")));
            ps.setString(36, getStr(record.get("c_convienence_cd")));
            ps.setString(37, getStr(record.get("c_drive_distance_cluster")));
            ps.setString(38, getStr(record.get("c_frequency")));
            ps.setDate(39, getDate(record.get("d_last_activity_dt")));
            ps.setInt(40, getInt(record.get("i_lodging_pct")));
            ps.setDouble(41, getDouble(record.get("f_worth")));
            ps.setString(42, getStr(record.get("c_worth_break")));
            ps.setString(43, getStr(record.get("c_channel_rec")));
            ps.setString(44, getStr(record.get("c_dom_market_cd")));
            ps.setString(45, getStr(record.get("c_gst_pref_mail_flag")));
            ps.setString(46, getStr(record.get("c_tdc_supp_flag")));
            ps.setString(47, getStr(record.get("c_prop_cd_1")));
            ps.setInt(48, getInt(record.get("i_prop_pct_worth_1")));
            ps.setString(49, getStr(record.get("c_prop_cd_2")));
            ps.setInt(50, getInt(record.get("i_prop_pct_worth_2")));
            ps.setString(51, getStr(record.get("c_pref_prop_host_cd")));
            ps.setString(52, getStr(record.get("c_host_type")));
            ps.setInt(53, getInt(record.get("i_age")));
            ps.setString(54, getStr(record.get("c_gender")));
            ps.setString(55, getStr(record.get("c_dom_cd_prop")));
            ps.setString(56, getStr(record.get("c_pref_prop_cd")));
            ps.setString(57, getStr(record.get("c_geo_dm_zone")));
            ps.setString(58, getStr(record.get("c_geo_rpt_zone")));
            ps.setString(59, getStr(record.get("c_distance_cluster")));
            ps.setString(60, getStr(record.get("c_msa_cd")));
            ps.setString(61, getStr(record.get("c_msa")));
            ps.setString(62, getStr(record.get("c_zip_7_pdb")));
            ps.setDate(63, getDate(record.get("d_create_dt_mkt")));
            ps.setDate(64, getDate(record.get("d_create_dt_ent")));
            ps.setDouble(65, getDouble(record.get("f_act_all_ap_pdb_12mn_plus")));
            ps.setDouble(66, getDouble(record.get("f_theo_all_ap_pdb_12mn_plus")));
            ps.setInt(67, getInt(record.get("i_days_rtd_ap_pdb_12mn_plus")));
            ps.setInt(68, getInt(record.get("i_trp_rtd_ap_pdb_12mn_plus")));
            ps.setDouble(69, getDouble(record.get("f_daily_theo_ap_pdb_12mnplus")));
            ps.setDouble(70, getDouble(record.get("f_daily_act_ap_pdb_12mn_plus")));
            ps.setDouble(71, getDouble(record.get("f_daily_wrth_ap_pdb_12mnplus")));
            ps.setDouble(72, getDouble(record.get("f_resp_rate_ap_pdb_12mn_plus")));
            ps.setDouble(73, getDouble(record.get("f_ofr_trp_pct_ap_pdb12mnplus")));
            ps.setDouble(74, getDouble(record.get("f_htlsty_pct_ap_pdb_12mnplus")));
            ps.setDouble(75, getDouble(record.get("f_slot_pct_ap_pdb_12mn_plus")));
            ps.setDouble(76, getDouble(record.get("f_trp_all_adj_sc_pdb_12mn")));
            ps.setDouble(77, getDouble(record.get("f_trp_rtd_adj_sc_pdb_12mn")));
            ps.setDouble(78, getDouble(record.get("f_act_all_ap_pdb_24mn_plus")));
            ps.setDouble(79, getDouble(record.get("f_theo_all_ap_pdb_24mn_plus")));
            ps.setInt(80, getInt(record.get("i_days_rtd_ap_pdb_24mn_plus")));
            ps.setInt(81, getInt(record.get("i_trp_rtd_ap_pdb_24mn_plus")));
            ps.setDouble(82, getDouble(record.get("f_daily_theo_ap_pdb_24mnplus")));
            ps.setDouble(83, getDouble(record.get("f_daily_act_ap_pdb_24mn_plus")));
            ps.setDouble(84, getDouble(record.get("f_daily_wrth_ap_pdb_24mnplus")));
            ps.setDouble(85, getDouble(record.get("f_resp_rate_ap_pdb_24mn_plus")));
            ps.setDouble(86, getDouble(record.get("f_ofr_trp_pct_ap_pdb24mnplus")));
            ps.setDouble(87, getDouble(record.get("f_htlsty_pct_ap_pdb_24mnplus")));
            ps.setDouble(88, getDouble(record.get("f_slot_pct_ap_pdb_24mn_plus")));
            ps.setDouble(89, getDouble(record.get("f_trp_all_adj_sc_pdb_24mn")));
            ps.setDouble(90, getDouble(record.get("f_trp_rtd_adj_sc_pdb_24mn")));
            ps.setDouble(91, getDouble(record.get("f_trp_all_adj_sc_pdb_36mn")));
            ps.setDouble(92, getDouble(record.get("f_trp_rtd_adj_sc_pdb_36mn")));
            ps.setDouble(93, getDouble(record.get("f_profit_pct_ent")));
            ps.setString(94, getStr(record.get("c_profit_flag_ent")));
            ps.setDouble(95, getDouble(record.get("f_profit_pct_mkt")));
            ps.setString(96, getStr(record.get("c_profit_flag_mkt")));
            ps.setInt(97, getInt(record.get("i_age_adj_ah")));
            ps.setInt(98, getInt(record.get("i_dob_mn_adj_ah")));
            ps.setInt(99, getInt(record.get("i_dob_yr_adj_ah")));
            ps.setString(100, getStr(record.get("c_country_cd_ah")));
            ps.setString(101, getStr(record.get("c_credit_suppress_flag_m_ah")));
            ps.setString(102, getStr(record.get("c_mkt_host_cd_adj_ah")));
            ps.setString(103, getStr(record.get("c_dom_prop_host_cd_adj_ah")));
            ps.setString(104, getStr(record.get("c_prop_cd_1_host_cd_adj_ah")));
            ps.setString(105, getStr(record.get("c_zip_7_ah")));
            ps.setDouble(106, getDouble(record.get("f_tier_score_ah")));
            ps.setDouble(107, getDouble(record.get("f_lodging_pct_mkt_adj_ah")));
            ps.setDouble(108, getDouble(record.get("f_pct_bacarrat_mkt_ah")));
            ps.setDouble(109, getDouble(record.get("f_pct_blackjack_mkt_ah")));
            ps.setDouble(110, getDouble(record.get("f_pct_craps_mkt_ah")));
            ps.setDouble(111, getDouble(record.get("f_pct_other_mkt_ah")));
            ps.setDouble(112, getDouble(record.get("f_pct_paigow_mkt_ah")));
            ps.setDouble(113, getDouble(record.get("f_pct_roulette_mkt_ah")));
            ps.setDouble(114, getDouble(record.get("f_pct_video_poker_mkt_ah")));
            ps.setString(115, getStr(record.get("c_future_res_flag_mkt_ah")));
            ps.setDouble(116, getDouble(record.get("f_worth_1mn_ap_ah")));
            ps.setDouble(117, getDouble(record.get("f_worth_3mn_ap_ah")));
            ps.setDouble(118, getDouble(record.get("f_worth_6mn_ap_ah")));
            ps.setDouble(119, getDouble(record.get("f_worth_12mn_ap_ah")));
            ps.setDouble(120, getDouble(record.get("f_worth_all_ytd_ap_ah")));
            ps.setDouble(121, getDouble(record.get("f_theo_all_1mn_ap_ah")));
            ps.setDouble(122, getDouble(record.get("f_theo_all_3mn_ap_ah")));
            ps.setDouble(123, getDouble(record.get("f_theo_all_6mn_ap_ah")));
            ps.setDouble(124, getDouble(record.get("f_theo_all_12mn_ap_ah")));
            ps.setDouble(125, getDouble(record.get("f_theo_all_ytd_ap_ah")));
            ps.setDouble(126, getDouble(record.get("f_theo_slot_1mn_ap_ah")));
            ps.setDouble(127, getDouble(record.get("f_theo_slot_3mn_ap_ah")));
            ps.setDouble(128, getDouble(record.get("f_theo_slot_6mn_ap_ah")));
            ps.setDouble(129, getDouble(record.get("f_theo_slot_12mn_ap_ah")));
            ps.setDouble(130, getDouble(record.get("f_theo_table_1mn_ap_ah")));
            ps.setDouble(131, getDouble(record.get("f_theo_table_3mn_ap_ah")));
            ps.setDouble(132, getDouble(record.get("f_theo_table_6mn_ap_ah")));
            ps.setDouble(133, getDouble(record.get("f_theo_table_12mn_ap_ah")));
            ps.setDouble(134, getDouble(record.get("f_act_all_1mn_ap_ah")));
            ps.setDouble(135, getDouble(record.get("f_act_all_3mn_ap_ah")));
            ps.setDouble(136, getDouble(record.get("f_act_all_6mn_ap_ah")));
            ps.setDouble(137, getDouble(record.get("f_act_all_12mn_ap_ah")));
            ps.setDouble(138, getDouble(record.get("f_actual_all_ytd_ap_ah")));
            ps.setDouble(139, getDouble(record.get("f_avgbet_game_1mn_ap_ah")));
            ps.setDouble(140, getDouble(record.get("f_avgbet_game_3mn_ap_ah")));
            ps.setDouble(141, getDouble(record.get("f_avgbet_game_6mn_ap_ah")));
            ps.setDouble(142, getDouble(record.get("f_avgbet_game_12mn_ap_ah")));
            ps.setInt(143, getInt(record.get("i_lop_game_1mn_ap_ah")));
            ps.setInt(144, getInt(record.get("i_lop_game_3mn_ap_ah")));
            ps.setInt(145, getInt(record.get("i_lop_game_6mn_ap_ah")));
            ps.setInt(146, getInt(record.get("i_lop_game_12mn_ap_ah")));
            ps.setInt(147, getInt(record.get("i_trp_all_cms_1mn_ap_ah")));
            ps.setInt(148, getInt(record.get("i_trp_all_cms_3mn_ap_ah")));
            ps.setInt(149, getInt(record.get("i_trp_all_cms_6mn_ap_ah")));
            ps.setInt(150, getInt(record.get("i_trp_all_cms_12mn_ap_ah")));
            ps.setInt(151, getInt(record.get("i_trp_rtd_1mn_ap_ah")));
            ps.setInt(152, getInt(record.get("i_trp_rtd_3mn_ap_ah")));
            ps.setInt(153, getInt(record.get("i_trp_rtd_6mn_ap_ah")));
            ps.setInt(154, getInt(record.get("i_trp_rtd_12mn_ap_ah")));
            ps.setInt(155, getInt(record.get("i_days_rtd_1mn_ap_ah")));
            ps.setInt(156, getInt(record.get("i_days_rtd_3mn_ap_ah")));
            ps.setInt(157, getInt(record.get("i_days_rtd_6mn_ap_ah")));
            ps.setInt(158, getInt(record.get("i_days_rtd_12mn_ap_ah")));
            ps.setDate(159, getDate(record.get("d_last_rated_dt_mkt_ah")));
            ps.setDouble(160, getDouble(record.get("f_rc_earned_1mn_mkt_ah")));
            ps.setDouble(161, getDouble(record.get("f_rc_earned_3mn_mkt_ah")));
            ps.setDouble(162, getDouble(record.get("f_rc_earned_6mn_mkt_ah")));
            ps.setDouble(163, getDouble(record.get("f_rc_earned_12mn_mkt_ah")));
            ps.setDate(164, getDate(record.get("d_first_gaming_dt_mkt_ah")));
            ps.setDate(165, getDate(record.get("d_first_gaming_dt_ent_ah")));
            ps.setDate(166, getDate(record.get("d_first_dt_played_mkt_ah")));
            ps.setDouble(167, getDouble(record.get("f_act_all_cms_first_hr_m_ah")));
            ps.setDouble(168, getDouble(record.get("f_act_all_cms_first_day_m_ah")));
            ps.setDouble(169, getDouble(record.get("f_act_all_cms_first_trp_m_ah")));
            ps.setInt(170, getInt(record.get("i_lop_first_hr_mkt_ah")));
            ps.setInt(171, getInt(record.get("i_lop_first_day_mkt_ah")));
            ps.setInt(172, getInt(record.get("i_lop_first_trp_mkt_ah")));
            ps.setString(173, getStr(record.get("c_convienence_cd_new")));
            ps.setString(174, getStr(record.get("c_prop_cd")));
            ps.setDouble(175, getDouble(record.get("f_property_worth")));
            ps.setDate(176, getDate(record.get("d_last_activity_dt_prop")));
            ps.setString(177, getStr(record.get("c_property_frequency")));
            ps.setString(178, getStr(record.get("c_recency")));
            ps.setString(179, getStr(record.get("c_frequency_tc")));
            ps.setString(180, getStr(record.get("c_frequency_ttm_24")));
            ps.setString(181, getStr(record.get("c_tierscore_desc")));
            ps.setDouble(182, getDouble(record.get("f_hotel_adw")));
            ps.setString(183, getStr(record.get("c_hotel_adw_break")));
            ps.setString(184, getStr(record.get("c_frequency_ttm")));
            ps.setString(185, getStr(record.get("c_avg_day_hotel_flag")));
            ps.setInt(186, getInt(record.get("i_days_since_last")));
            ps.setDouble(187, getDouble(record.get("f_trip_cycle_48")));
            ps.setString(188, getStr(record.get("c_frequency_tc_12")));
            ps.setDouble(189, getDouble(record.get("f_trip_cycle_12")));
            ps.setInt(190, getInt(record.get("i_ttm_trip_cnt_12")));
            ps.setInt(191, getInt(record.get("i_ttm_trip_cnt_24")));
            ps.setString(192, getStr(record.get("c_adw_trip_break_12")));
            ps.setDouble(193, getDouble(record.get("f_adw_trip_12")));
            ps.setString(194, getStr(record.get("c_adw_trip_break_6")));
            ps.setDouble(195, getDouble(record.get("f_adw_trip_6")));
            ps.setString(196, getStr(record.get("c_adw_break_48")));
            ps.setDouble(197, getDouble(record.get("f_adw_48")));
            ps.setString(198, getStr(record.get("c_adw_break_24")));
            ps.setDouble(199, getDouble(record.get("f_adw_24")));
            ps.setString(200, getStr(record.get("c_adw_break_15")));
            ps.setDouble(201, getDouble(record.get("f_adw_15")));
            ps.setString(202, getStr(record.get("c_adw_break_12")));
            ps.setDouble(203, getDouble(record.get("f_adw_12")));
            ps.setString(204, getStr(record.get("c_hotel_adw_break_48")));
            ps.setDouble(205, getDouble(record.get("f_hotel_adw_48")));
            ps.setString(206, getStr(record.get("c_hotel_adw_break_24")));
            ps.setDouble(207, getDouble(record.get("f_hotel_adw_24")));
            ps.setString(208, getStr(record.get("c_hotel_adw_break_15")));
            ps.setDouble(209, getDouble(record.get("f_hotel_adw_15")));
            ps.setString(210, getStr(record.get("c_hotel_adw_break_12")));
            ps.setDouble(211, getDouble(record.get("f_hotel_adw_12")));
            ps.setString(212, getStr(record.get("c_last_gaming_trip")));
            ps.setInt(213, getInt(record.get("i_days_since_gaming")));
            ps.setInt(214, getInt(record.get("i_email_decile")));
            //add contact mapping

            ps.getPreparedStatement().addBatch();

            if (++count % batchSize == 0) {
                ps.getPreparedStatement().executeBatch();
                System.err.println("Done : " + count);
            }
        }

        ps.getPreparedStatement().executeBatch(); // insert remaining records
        ps.getPreparedStatement().close();
        connection.close();

        System.err.println("Done in: " + (System.currentTimeMillis() - start) / 100);
    }

    /*
    * Call setNull if needed
    */
    private class PSWrapper {
        private final PreparedStatement ps;

        private PSWrapper(PreparedStatement ps) {
            this.ps = ps;
        }

        public void setInt(int id, Integer v) throws SQLException {
            if (v == null) {
                ps.setNull(id, Types.INTEGER);
            } else {
                ps.setInt(id, v);
            }
        }

        public void setDouble(int id, Double v) throws SQLException {
            if (v == null) {
                ps.setNull(id, Types.DOUBLE);
            } else {
                ps.setDouble(id, v);
            }
        }

        public void setString(int id, String v) throws SQLException {
            ps.setString(id, v);
        }

        public void setDate(int id, Date v) throws SQLException {
            ps.setDate(id, v);
        }

        public PreparedStatement getPreparedStatement() {
            return ps;
        }


    }


}
