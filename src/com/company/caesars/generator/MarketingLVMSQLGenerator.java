package com.company.caesars.generator;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.sql.*;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class MarketingLVMSQLGenerator extends SQLGeneratorBase implements SQLGenerator {

    private static final String SEPARATOR = "";

    private final NumberFormat nf = NumberFormat.getNumberInstance(Locale.US);
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    private String readFilePath = "C://Users//Michal Bluj//Desktop//US1127/marketing_lvm.csv";
    private String writeFilePath = "/tmp/tempcsv.csv";
    private String insertStatement = "INSERT INTO caesars.marketing_lvm (" +
            "i_dmid,c_prop_market_cd_fk,c_prop_market_cd,d_campaign_score_dt,c_recency," +
            "i_days_since_last,c_frequency_tc_48,f_trip_cycle_48,c_frequency_tc_12,f_trip_cycle_12," +
            "c_frequency_ttm,i_ttm_trip_cnt_12,c_frequency_ttm_24,i_ttm_trip_cnt_24,c_adw_trip_break_12," +
            "f_adw_trip_12,c_adw_trip_break_6,f_adw_trip_6,c_adw_break_48,f_adw_48,c_adw_break_24," +
            "f_adw_24,c_adw_break_15,f_adw_15,c_adw_break_12,f_adw_12,c_hotel_adw_break,f_hotel_adw," +
            "c_hotel_adw_break_48,f_hotel_adw_48,c_hotel_adw_break_24,f_hotel_adw_24,c_hotel_adw_break_15," +
            "f_hotel_adw_15,c_hotel_adw_break_12,f_hotel_adw_12,c_last_gaming_trip,i_days_since_gaming," +
            "c_avg_day_hotel_flag,f_tier_score,c_tierscore_desc,i_email_decile,c_acct_type_cd_fk," +
            "c_acct_type_cd,c_tier_cd_fk,c_tier_cd,c_mailable,c_emailable,c_prop_mail_cd," +
            "c_dom_pref_prop_cd_fk,c_dom_pref_prop_cd,c_dom_cd_prop,c_channel_pref,i_age_adj_ah," +
            "i_dob_mn_adj_ah,c_country_cd_ah,c_zip_7_pdb,c_distance_cluster,c_main_ethnic_type," +
            "c_ethnic_group1,c_game_pref,i_days_rtd_ap_pdb_12mn_plus,i_trp_rtd_ap_pdb_12mn_plus," +
            "f_act_all_ap_pdb_12mn_plus,f_theo_all_ap_pdb_12mn_plus,f_daily_theo_ap_pdb_12mnplus," +
            "f_daily_act_ap_pdb_12mn_plus,f_slot_pct_ap_pdb_12mn_plus,f_trp_all_adj_sc_pdb_12mn," +
            "f_theo_slot_pm12,i_rated_days_p_pm15,i_nbr_rtd_trips_p_pm15,f_actual_all_p_pm15," +
            "f_theo_all_p_pm15,f_theo_slot_pm15,f_ada_p_pm15,f_adt_p_pm15,f_slot_pct_pm15,d_min_est_dt," +
            "i_cas_acct_blv,i_cas_acct_clv,i_cas_acct_flv,i_cas_acct_ilv,i_cas_acct_las,i_cas_acct_phv," +
            "i_cas_acct_plv,i_cas_acct_rlv,c_rep_id_blv,c_rep_id_clv,c_rep_id_flv,c_rep_id_ilv," +
            "c_rep_id_las,c_rep_id_phv,c_rep_id_plv,c_rep_id_rlv,i_nbr_trips_p_pm15,i_nbr_trips_p_pm24," +
            "i_random_1,i_random_2,i_random_static_yr,LVM12moHV,LVM15moHV,LVM24moHV,Max_Exp_Date_SIN_12mo," +
            "Max_Exp_Date_INA_12mo,Max_Exp_Date_EXH_12mo,Probability_12mo,Predicted_Bucket_12mo," +
            "Max_Exp_Date_SIN_24mo,Max_Exp_Date_INA_24mo,Max_Exp_Date_EXH_24mo,Probability_24mo," +
            "Predicted_Bucket_24mo,c_credit_status_flag) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";


    public MarketingLVMSQLGenerator() {
    }

    public MarketingLVMSQLGenerator(String readFilePath) {
    }

    public MarketingLVMSQLGenerator(String readFilePath, String writeFilePath) {
        this.readFilePath = readFilePath;
        this.writeFilePath = writeFilePath;
    }

    private Integer getInt(String s) throws ParseException {
        return isInvalid(s) ? null : nf.parse(s.trim()).intValue();
    }

    private Double getDouble(String s) throws ParseException {
        return isInvalid(s) ? null : nf.parse(s.trim()).doubleValue();
    }

    private String getStr(String s) {
        return isInvalid(s) ? null : s.trim();
    }

    private Date getDate(String s) throws ParseException {
        return isInvalid(s) ? null : new Date(dateFormat.parse(s.trim()).getTime());
    }

    private boolean isInvalid(String s) {
        if (s == null) {
            return true;
        }
        return "NULL".equals(s) || "?".equals(s) || "empty".equals(s);
    }

    private Long getLong(String s) throws ParseException {
        return isInvalid(s) ? null : nf.parse(s.trim()).longValue();
    }

    public void insertRecordsToDatabase() throws Exception {
        long start = System.currentTimeMillis();
        retrieveTierCodeTable();
        retrieveMarketTable();
        retrieveAccountTypeCodeTable();


        FileReader fileReader = new FileReader(new File(readFilePath));
        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(fileReader);
        //connection = getShelfConnection();
        final int batchSize = 6000;
        int count = 0;
        PSWrapper ps = new PSWrapper(connection.prepareStatement(insertStatement));
        for (CSVRecord record : records) {

            ps.setString(1, String.valueOf(getLong(record.get("i_dmid"))));
            ps.setString(2, getStr(marketCodeKeyMap.get(record.get("c_prop_market_cd"))));
            ps.setString(3, getStr(record.get("c_prop_market_cd")));
            ps.setDate(4, getDate(record.get("d_campaign_score_dt")));
            ps.setString(5, getStr(record.get("c_recency")));
            ps.setInt(6, getInt(record.get("i_days_since_last")));
            ps.setString(7, getStr(record.get("c_frequency_tc_48")));
            ps.setInt(8, getInt(record.get("f_trip_cycle_48")));
            ps.setString(9, getStr(record.get("c_frequency_tc_12")));
            ps.setInt(10, getInt(record.get("f_trip_cycle_12")));
            ps.setString(11, getStr(record.get("c_frequency_ttm")));
            ps.setInt(12, getInt(record.get("i_ttm_trip_cnt_12")));
            ps.setString(13, getStr(record.get("c_frequency_ttm_24")));
            ps.setInt(14, getInt(record.get("i_ttm_trip_cnt_24")));
            ps.setString(15, getStr(record.get("c_adw_trip_break_12")));
            ps.setInt(16, getInt(record.get("f_adw_trip_12")));
            ps.setString(17, getStr(record.get("c_adw_trip_break_6")));
            ps.setInt(18, getInt(record.get("f_adw_trip_6")));
            ps.setString(19, getStr(record.get("c_adw_break_48")));
            ps.setInt(20, getInt(record.get("f_adw_48")));
            ps.setString(21, getStr(record.get("c_adw_break_24")));
            ps.setInt(22, getInt(record.get("f_adw_24")));
            ps.setString(23, getStr(record.get("c_adw_break_15")));
            ps.setInt(24, getInt(record.get("f_adw_15")));
            ps.setString(25, getStr(record.get("c_adw_break_12")));
            ps.setInt(26, getInt(record.get("f_adw_12")));
            ps.setString(27, getStr(record.get("c_hotel_adw_break")));
            ps.setInt(28, getInt(record.get("f_hotel_adw")));
            ps.setString(29, getStr(record.get("c_hotel_adw_break_48")));
            ps.setInt(30, getInt(record.get("f_hotel_adw_48")));
            ps.setString(31, getStr(record.get("c_hotel_adw_break_24")));
            ps.setInt(32, getInt(record.get("f_hotel_adw_24")));
            ps.setString(33, getStr(record.get("c_hotel_adw_break_15")));
            ps.setInt(34, getInt(record.get("f_hotel_adw_15")));
            ps.setString(35, getStr(record.get("c_hotel_adw_break_12")));
            ps.setInt(36, getInt(record.get("f_hotel_adw_12")));
            ps.setString(37, getStr(record.get("c_last_gaming_trip")));
            ps.setInt(38, getInt(record.get("i_days_since_gaming")));
            ps.setString(39, getStr(record.get("c_avg_day_hotel_flag")));
            ps.setInt(40, getInt(record.get("f_tier_score")));
            ps.setString(41, getStr(record.get("c_tierscore_desc")));
            ps.setInt(42, getInt(record.get("i_email_decile")));
            ps.setString(43, getStr(accountTypeCodeKeyMap.get(record.get("c_acct_type_cd"))));
            ps.setString(44, getStr(record.get("c_acct_type_cd")));
            ps.setString(45, getStr(tierCodeKeyMap.get(record.get("c_tier_cd"))));
            ps.setString(46, getStr(record.get("c_tier_cd")));
            ps.setString(47, getStr(record.get("c_mailable")));
            ps.setString(48, getStr(record.get("c_emailable")));
            ps.setString(49, getStr(record.get("c_prop_mail_cd")));
            ps.setString(50, getStr(propertyCodeKeyMap.get(record.get("c_dom_pref_prop_cd"))));
            ps.setString(51, getStr(record.get("c_dom_pref_prop_cd")));
            ps.setString(52, getStr(record.get("c_dom_cd_prop")));
            ps.setString(53, getStr(record.get("c_channel_pref")));
            ps.setInt(54, getInt(record.get("i_age_adj_ah")));
            ps.setInt(55, getInt(record.get("i_dob_mn_adj_ah")));
            ps.setString(56, getStr(record.get("c_country_cd_ah")));
            ps.setString(57, getStr(record.get("c_zip_7_pdb")));
            ps.setString(58, getStr(record.get("c_distance_cluster")));
            ps.setString(59, getStr(record.get("c_main_ethnic_type")));
            ps.setString(60, getStr(record.get("c_ethnic_group1")));
            ps.setString(61, getStr(record.get("c_game_pref")));
            ps.setInt(62, getInt(record.get("i_days_rtd_ap_pdb_12mn_plus")));
            ps.setInt(63, getInt(record.get("i_trp_rtd_ap_pdb_12mn_plus")));
            ps.setInt(64, getInt(record.get("f_act_all_ap_pdb_12mn_plus")));
            ps.setInt(65, getInt(record.get("f_theo_all_ap_pdb_12mn_plus")));
            ps.setInt(66, getInt(record.get("f_daily_theo_ap_pdb_12mnplus")));
            ps.setInt(67, getInt(record.get("f_daily_act_ap_pdb_12mn_plus")));
            ps.setInt(68, getInt(record.get("f_slot_pct_ap_pdb_12mn_plus")));
            ps.setInt(69, getInt(record.get("f_trp_all_adj_sc_pdb_12mn")));
            ps.setInt(70, getInt(record.get("f_theo_slot_pm12")));
            ps.setInt(71, getInt(record.get("i_rated_days_p_pm15")));
            ps.setInt(72, getInt(record.get("i_nbr_rtd_trips_p_pm15")));
            ps.setInt(73, getInt(record.get("f_actual_all_p_pm15")));
            ps.setInt(74, getInt(record.get("f_theo_all_p_pm15")));
            ps.setInt(75, getInt(record.get("f_theo_slot_pm15")));
            ps.setInt(76, getInt(record.get("f_ada_p_pm15")));
            ps.setInt(77, getInt(record.get("f_adt_p_pm15")));
            ps.setInt(78, getInt(record.get("f_slot_pct_pm15")));
            ps.setDate(79, getDate(record.get("d_min_est_dt")));
            ps.setInt(80, getInt(record.get("i_cas_acct_blv")));
            ps.setInt(81, getInt(record.get("i_cas_acct_clv")));
            ps.setInt(82, getInt(record.get("i_cas_acct_flv")));
            ps.setInt(83, getInt(record.get("i_cas_acct_ilv")));
            ps.setInt(84, getInt(record.get("i_cas_acct_las")));
            ps.setInt(85, getInt(record.get("i_cas_acct_phv")));
            ps.setInt(86, getInt(record.get("i_cas_acct_plv")));
            ps.setInt(87, getInt(record.get("i_cas_acct_rlv")));
            ps.setString(88, getStr(record.get("c_rep_id_blv")));
            ps.setString(89, getStr(record.get("c_rep_id_clv")));
            ps.setString(90, getStr(record.get("c_rep_id_flv")));
            ps.setString(91, getStr(record.get("c_rep_id_ilv")));
            ps.setString(92, getStr(record.get("c_rep_id_las")));
            ps.setString(93, getStr(record.get("c_rep_id_phv")));
            ps.setString(94, getStr(record.get("c_rep_id_plv")));
            ps.setString(95, getStr(record.get("c_rep_id_rlv")));
            ps.setInt(96, getInt(record.get("i_nbr_trips_p_pm15")));
            ps.setInt(97, getInt(record.get("i_nbr_trips_p_pm24")));
            ps.setInt(98, getInt(record.get("i_random_1")));
            ps.setInt(99, getInt(record.get("i_random_2")));
            ps.setInt(100, getInt(record.get("i_random_static_yr")));
            ps.setInt(101, getInt(record.get("LVM12moHV")));
            ps.setInt(102, getInt(record.get("LVM15moHV")));
            ps.setInt(103, getInt(record.get("LVM24moHV")));
            ps.setInt(104, getInt(record.get("Max_Exp_Date_SIN_12mo")));
            ps.setInt(105, getInt(record.get("Max_Exp_Date_INA_12mo")));
            ps.setInt(106, getInt(record.get("Max_Exp_Date_EXH_12mo")));
            ps.setInt(107, getInt(record.get("Probability_12mo")));
            ps.setString(108, getStr(record.get("Predicted_Bucket_12mo")));
            ps.setInt(109, getInt(record.get("Max_Exp_Date_SIN_24mo")));
            ps.setInt(110, getInt(record.get("Max_Exp_Date_INA_24mo")));
            ps.setInt(111, getInt(record.get("Max_Exp_Date_EXH_24mo")));
            ps.setInt(112, getInt(record.get("Probability_24mo")));
            ps.setString(113, getStr(record.get("Predicted_Bucket_24mo")));
            ps.setString(114, getStr(record.get("c_credit_status_flag")));
            // add contact mapping

            ps.getPreparedStatement().addBatch();

            if (++count % batchSize == 0) {
                ps.getPreparedStatement().executeBatch();
                System.err.println("Done :" + count);
            }
        }

        ps.getPreparedStatement().executeBatch(); // insert remaining records
        ps.getPreparedStatement().close();
        connection.close();

        System.err.println("Done in:" + (System.currentTimeMillis() - start) / 100);


    }

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