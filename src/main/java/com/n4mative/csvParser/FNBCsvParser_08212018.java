package com.n4mative.csvParser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.core.env.Environment;

import com.n4mative.database.ConnectionPool;
//import com.fasterxml.jackson.databind.ObjectMapper;


public class FNBCsvParser_08212018 {
	
	
	private static Environment env;
	static String host = "192.168.1.71";//env.getProperty("mysql.host");
    static String db = "clear_water_test";//env.getProperty("mysql.db");
    static String strDelim="|";
    public static void main(String[] args) throws IOException {
    	
    	//String csvFile = "/Users/surbhi/Documents/workspace/Clear_Water/src/main/resources/ClearWaterInvitees.csv";
    	String csvFile = "/Users/saurabh/N4Mative/FNB/Video - 20 Sample.csv"; 
    	csvFile = "/Users/saurabh/Downloads/n4mative - CSV - 20 Sample.csv";
    	csvFile = "/Users/saurabh/N4Mative/FNB/n4mative - 20 Sample - NumberFormats - CSV Pipe.csv";
    	
    	readInitialFile(csvFile);
    	
    		
    }
    
    private static void readInitialFile(String csvFile) throws IOException{
    	BufferedReader br = null;
        String line = "";
        String cvsSplitBy = "\\"+strDelim; //",";
        String csvOutFile = "/Users/saurabh/N4Mative/FNB/csvVideoOut20180816.csv";
        String ttt=   "0              1              2           3             4             5                 				6               7       8              9            10                             11                      12         13                      14                       15             16               17                      ";
        //String header="customer_id,customer_name,Account_Type,current_date,amount_money_in,amount_money_out,amount_status,dates_from_until,date_to,date_from,amount_shortfall,amount_possible_shortfall,amount_upcoming_payments,amount_cash,amount_overdraft_limit,current_available_balance,recipient's_email_address_(optional),email_parameter(optional)";
        String header="customer_id,customer_name,Account_type,current_date,amount_money_in,amount_money_out,amount_status,dates_from_until,date_from,date_to,possible_cash,amount_possible_shortfall,AVAIL_BALANCE,Upcoming_payments"; //,cash_scale,shortfall_scale,avail_scale,payments_scale";
        header="customer_id|customer_name|Account_type|current_date|amount_money_in|amount_money_out|amount_status|dates_from_until|date_from|date_to|possible_cash|amount_possible_shortfall|AVAIL_BALANCE|Upcoming_payments|cash_scale|shortfall_scale|avail_scale|payments_scale|cash_number|short_number|avail_number|payment_number";

        //"customer_id,customer_name,Account_type,current_date,amount_money_in,amount_money_out,date_from,date_to,possible_cash,amount_possible_shortfall,AVAIL_BALANCE,Upcoming_payments"
        String headerOrig="Customer_ID,first_name,Account_Type,Date of Current Balance,Total_Money_in,Total_Money_Out,Date From,Date_To,Current_Balance,Current_Available_Balance,Cash_Position,App_Software_Version";
        FileWriter fw = new FileWriter(csvOutFile);
        fw.append(header);
    	fw.append('\n');
        try {

            br = new BufferedReader(new FileReader(csvFile));
            String headerline = br.readLine();
            while ((line = br.readLine()) != null) {
            	String dataRow="";
            	line=line.trim();
            	if (!line.endsWith("|"))
            			line+=strDelim;
            	String[] data1 = line.split(cvsSplitBy);
            	String[] data;
            	if (data1.length < 12){
            		line+="R0" + strDelim ;
            		data = line.split(cvsSplitBy);
            	}else{
            		data = data1.clone();
            		//data=data+strDelim;
            	}
            	
                dataRow+=data[0].trim()+strDelim+data[1].trim()+strDelim+data[2].trim()+strDelim+data[3].trim()+strDelim+data[4].trim()+strDelim+data[5].trim()+strDelim;
                //If money out is greater than Money In, then add Red to string else green
                String data5="Green";
                String amtIn=data[4].replaceAll("R", "").replaceAll(",", "");
                String amtOut=data[5].replaceAll("R", "").replaceAll(",", "");
                if (!data[4].equalsIgnoreCase("NULL") && !data[5].equalsIgnoreCase("NULL")){
                	data5 = Float.valueOf(amtOut)-Float.valueOf(amtIn)>0?"Red":"Green";
                } 
                /*if (!data[4].equalsIgnoreCase("NULL") && !data[5].equalsIgnoreCase("NULL")){
                 data5=Float.valueOf(data[5])-Float.valueOf(data[4])>0?"Red":"Green";
                }*/
                
                String abc = data[8].equalsIgnoreCase("")?"0":data[8];
                
                //dataRow+=data5+","+data[6].substring(0, data[6].indexOf(" "))+" - "+data[7].substring(0, data[7].indexOf(" "))+","+data[6].substring(0, data[6].indexOf(" "))+","+data[7].substring(0, data[7].indexOf(" "))+",";
                dataRow+=data5+strDelim+data[6]+" - "+data[7]+strDelim+data[6]+strDelim+data[7]+strDelim;
                //Possible Cash
                dataRow+= data[8].equalsIgnoreCase("")?"R0"+strDelim:data[8]+strDelim;
                //Possible Shortfall
                dataRow+= data[9].equalsIgnoreCase("")?"R0"+strDelim:data[9]+strDelim;
                //Available Balance
                dataRow+= data[10].equalsIgnoreCase("")?"R0"+strDelim:data[10]+strDelim;
                //Upcoming payments
                dataRow+= data[11].equalsIgnoreCase("")?"R0"+strDelim:data[11]+strDelim;
                
                Float possCash = Float.valueOf(data[8].equalsIgnoreCase("")?"0":data[8].replaceAll("R", "").replaceAll(",", ""));
                Float possShort = Float.valueOf(data[9].equalsIgnoreCase("")?"0":data[9].replaceAll("R", "").replaceAll(",", ""));
                Float availBal = Float.valueOf(data[10].equalsIgnoreCase("")?"0":data[10].replaceAll("R", "").replaceAll(",", ""));
                Float upPay = Float.valueOf(data[11].equalsIgnoreCase("")?"0":data[11].replaceAll("R", "").replaceAll(",", ""));
                
                Float Max=possCash;
                if (Max<upPay)
                	Max=upPay;
                if (Max<availBal)
                	Max=availBal;
                if (Max<possShort*-1)
                	Max=possShort*-1;
                
                
                String PC = String.format("%03d",Math.round(possCash/Max*100));
                String PS = String.format("%03d",Math.round(possShort*-1/Max*100));
                String AB = String.format("%03d",Math.round(availBal/Max*100));
                String UP = String.format("%03d",Math.round(upPay/Max*100));
                dataRow+=PC+strDelim+PS+strDelim+AB+strDelim+UP+strDelim+String.valueOf(possCash)+strDelim+String.valueOf(possShort)+strDelim+String.valueOf(availBal)+strDelim+String.valueOf(upPay);
                fw.append(dataRow);
                fw.append("\n");
                
                String[] dataShow = dataRow.split(cvsSplitBy);
                String[] dataHeader = header.split(cvsSplitBy);
                int i=0;
                for (String dl : dataShow){
                	System.out.println(dataHeader[i] + " - " + dl);
                	i++;
                }
            }
            fw.flush();
		    fw.close();
           // createInitialCsv();
            
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }
    
   
}
