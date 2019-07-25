package com.n4mative.csvParser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import com.n4mative.csvParser.Invitee;
import org.springframework.core.env.Environment;


import com.n4mative.database.ConnectionPool;

public class Fnb {
	
	private static Environment env;
	static String host = "192.168.1.71";
    static String db = "NPV_QA";
    
    public static void main(String[] args) {
    	
    	//String csvFile = args[1];//"/Users/surbhi/Documents/workspace/NPV/src/main/resources/n4mative - CSV - 20 Sample.csv";
    	String csvFile= "/Users/surbhi/git/NPV/src/main/resources/InputData.csv";//"/Users/surbhi/Documents/workspace/NPV/src/main/resources/20190409_Video_Final_v1.csv";
    	String purlFile= "/Users/surbhi/Documents/workspace/NPV/src/main/resources/purl_04092019_9900.csv";
    	String dt = new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance().getTime());
    	
    	if (args[0].equalsIgnoreCase("initialFile")){
    		readInitialFile(csvFile);
    		//createInitialCsv("*");
    		createInitialCsv_noDateDiff("*",dt);
    		
    	}else if (args[0].equalsIgnoreCase("purlFile")){
    		//readPurlFile(purlFile);
    		createFinalDispatchCsv("*",dt);
    	}
    }
    
  
    private static void readInitialFile(String csvFile){
    	BufferedReader br = null;
        String line = "";
        String csvSplitBy = "\\|";
        //String firstName="";
        try {
        	
        	int count= 1;
            br = new BufferedReader(new FileReader(csvFile));
            String headerline = br.readLine();
            while ((line = br.readLine()) != null) {
            	
            	String[] data = line.split(csvSplitBy);
            	String customer_id=data.length > 0 ? data[0].trim() : "";
            	String customer_name=data.length > 1 ? data[1].trim() : "";
            	String data_string= line;
                                
                //System.out.println("Customer_Id= " + customer_id + " , firstName= " + customer_name);
                //System.out.println("Remain = " + data_string);
                
                saveClient(count,customer_id,customer_name,data_string);
                count = count+1;
            }
            
            preprocess();
            //createInitialCsv();
           
             
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
    
    private static void saveClient(int count, String customer_id, String customer_name,  String data_string) {
		
    	System.out.println("host: " + host + "  db: " + db);
        CallableStatement stmt = null;
		String strSQL = "{call sp_InsertFnbInfo(?,?,?,?,?,?,?)}";
		
		try (Connection conn = ConnectionPool.getConnection(host, db)) {
			  	
				stmt = conn.prepareCall(strSQL);
				// set all the preparedstatement parameters
				stmt.setInt(1, 1);
				stmt.setInt(2, 1);
				stmt.setInt(3, 2);
				stmt.setString(4, customer_id);
				stmt.setString(5, customer_name);
				stmt.setString(6, data_string);
				stmt.setString(7, "Record Recieved");
				
			    // execute the preparedstatement insert
				stmt.executeQuery();
			    //st.close();
				System.out.println("Record Received");
			    //return true;
		  } 
		  catch (Exception e)
		  {
			  //throw new RuntimeException(e);
			  e.printStackTrace();
		  } finally {
	            try {
	            	if (stmt != null) {
	            		stmt.close();
	            	}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		
	}

	private static void preprocess() {
		String initialLetter = "*";
		List<Client> clients = getClient(initialLetter);
		String strDelim="|";
		String cvsSplitBy = "\\"+strDelim;
		for(Client clt : clients){
			String line=clt.getData_string();
			
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
        	//String custName = data[1].trim().replace("'", "");
            dataRow+=data[0].trim()+strDelim+data[1].trim()+strDelim+data[1].trim().replace("'", "-")+strDelim+data[2].trim()+strDelim+data[3].trim()+strDelim+data[4].trim()+strDelim+data[5].trim()+strDelim;
            //If money out is greater than Money In, then add Red to string else green
            String data5="Green";
            data[4]= data[4].equalsIgnoreCase("")?"R0":data[4];
            data[5]= data[5].equalsIgnoreCase("")?"R0":data[5];
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
            String upcPay = data[11].equalsIgnoreCase("")?"R0":data[11];
            if (!upcPay.equalsIgnoreCase("R0")) upcPay="-"+upcPay;
            dataRow+=upcPay+strDelim;
            System.out.println(dataRow);
            
            Float possCash = Float.valueOf(data[8].equalsIgnoreCase("")?"0":data[8].replaceAll("R", "").replaceAll(",", ""));
            Float possShort = Float.valueOf(data[9].equalsIgnoreCase("")?"0":data[9].replaceAll("R", "").replaceAll(",", ""));
            Float availBal = Float.valueOf(data[10].equalsIgnoreCase("")?"0":data[10].replaceAll("R", "").replaceAll(",", ""));
            Float upPay = Float.valueOf(data[11].equalsIgnoreCase("")?"0":data[11].replaceAll("R", "").replaceAll(",", ""));
            
            if (availBal<0) availBal=availBal*-1;
            
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
            if (AB.equalsIgnoreCase("001")) AB="003";
            if (AB.equalsIgnoreCase("002")) AB="003";
            String UP = String.format("%03d",Math.round(upPay/Max*100));
            
            //Get the Amount_in and Amount_Out scales
            String AI="000";
            String AO="100";
            if (Integer.valueOf(amtIn) > 0) AI="100"; //String.format("%03d",Integer.valueOf(amtIn));
            
            if (Float.valueOf(amtIn) > Float.valueOf(amtOut)){
            	AO = String.format("%03d",Math.round(Float.valueOf(amtOut)/Float.valueOf(amtIn)*100));
            	if (Integer.valueOf(AO)<=3 && Integer.valueOf(AO) >0) AO="003";
            }
            String AI_FULL="100";
            String AO_FULL="100";
            if (AI.equalsIgnoreCase("000")) AI_FULL="000";
            if (AO.equalsIgnoreCase("000")) AO_FULL="000";
            
            System.out.println(data[1]+strDelim+PC+strDelim+PS+strDelim+AB+strDelim+UP+strDelim+String.valueOf(possCash)+strDelim+String.valueOf(possShort)+strDelim+String.valueOf(availBal)+strDelim+String.valueOf(upPay)+strDelim+AI+strDelim+AO+strDelim+amtIn+strDelim+amtOut+strDelim+AI_FULL+strDelim+AO_FULL);
            dataRow+=PC+strDelim+PS+strDelim+AB+strDelim+UP+strDelim+String.valueOf(possCash)+strDelim+String.valueOf(possShort)+strDelim+String.valueOf(availBal)+strDelim+String.valueOf(upPay)+strDelim+AI+strDelim+AO+strDelim+amtIn+strDelim+amtOut+strDelim+AI_FULL+strDelim+AO_FULL;
            
            
                   
            update_status_PreProcess(clt.getClient_id(),clt.getProject_id(),clt.getGenerate_id(),clt.getCustomer_id(),dataRow);
      	}
            
			System.out.println(" Pre Process completed successfully.");
	}

	private static void update_status_PreProcess(int client_id, int project_id, int generate_id, String customer_id, String dataRow) {

    	CallableStatement stmt = null;
		String strSQL = "{call sp_Update_status_preprocess(?,?,?,?,?)}";
		
		try (Connection conn = ConnectionPool.getConnection(host, db)) {
			  	
				stmt = conn.prepareCall(strSQL);
				// set all the preparedstatement parameters
				
				stmt.setInt(1, client_id);
				stmt.setInt(2, project_id);
				stmt.setInt(3, generate_id);
				stmt.setString(4, customer_id);
				stmt.setString(5, dataRow);
				
			    // execute the preparedstatement insert
				stmt.executeQuery();
			    //st.close();
				System.out.println("Status updated with pre process completed");
			    //return true;
		  } 
		  catch (Exception e)
		  {
			  //throw new RuntimeException(e);
			  e.printStackTrace();
		  } finally {
	            try {
	            	if (stmt != null) {
	            		stmt.close();
	            	}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}

	}
 
	public static List<Client> getClient(String initialLetter) {
		PreparedStatement ps = null;
		CallableStatement stmt = null;
		ResultSet rs = null;
		List<Client> clients = new ArrayList<Client>();
		try (java.sql.Connection conn = ConnectionPool.getConnection(host, db)) {

        	String strSQL = "{call sp_GetFnbInfo(?)}";
        	stmt = conn.prepareCall(strSQL);
        	stmt.setString(1, initialLetter);
            boolean hadResults = stmt.execute();
            
            while (hadResults) {

            	rs = stmt.getResultSet();
            	while (rs.next()) { 
            		Client clt= new Client();
            		clt.setClient_id(rs.getInt("client_id"));
            		clt.setProject_id(rs.getInt("project_id"));
            		clt.setGenerate_id(rs.getInt("generate_id"));
            		clt.setCustomer_id(rs.getString("customer_id"));
            		clt.setCustomer_name(rs.getString("customer_name"));
            		clt.setData_string(rs.getString("data_string"));
            		clt.setVideo_link(rs.getString("video_link"));
            		clt.setVideo_file(rs.getString("video_file"));
            		clt.setThumbnail_link(rs.getString("thumbnail_link"));
            		clt.setThumbnail_link1(rs.getString("thumbnail_link1"));
            		
            		clients.add(clt);
            		
             	}
                hadResults = stmt.getMoreResults();
            }
            
            return clients;
        }
        catch(SQLException e) {
        	System.err.println(e.getMessage());
        } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        finally{
        	try {
            	if (ps != null) {
            		ps.close();
            	}
            	if (rs != null) {
            		rs.close();
            	}

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
		
		return clients;
	}
	
    private static void createInitialCsv( String initialLetter){
    	//String initialLetter = "*";
	    String filename = "/Users/surbhi/Documents/workspace/NPV/src/main/resources/output/ParsedVideoFile"+initialLetter+".csv"; 
	    try (Connection conn = ConnectionPool.getConnection(host, db)){
	    	
	    	FileWriter fw = new FileWriter(filename);
	    	List<Client> clients = getClient(initialLetter);
	    	
	        String header="customer_id|customer_name|customer_filename|Account_type|current_date|amount_money_in|amount_money_out|amount_status|dates_from_until|date_from|date_to|possible_cash|amount_possible_shortfall|AVAIL_BALANCE|Upcoming_payments|cash_scale|shortfall_scale|avail_scale|payments_scale|cash_number|short_number|avail_number|payment_number|AI_scale|AO_scale|AI_number|AO_number|AI_FULL|AO_FULL";
        	fw.append(header);
        	fw.append('\n');
          	for(Client clt : clients){
			    //fw.append(clt.getCustomer_id());
			    //fw.append(',');
			    //fw.append(clt.getCustomer_name());
			    //fw.append(',');
			    fw.append(clt.getData_string());
			    
			    fw.append('\n');
			    update_statusCsvCreated_initial(clt.getClient_id(), clt.getProject_id(), clt.getGenerate_id(), clt.getCustomer_id());
          	}
	            System.out.println(" Parsed CSV File is created successfully.");
          	
	            fw.flush();
			    fw.close();
			    conn.close();
          	
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
    }
    
    private static void createInitialCsv_noDateDiff( String initialLetter, String dt){
        //String initialLetter = “*”;
        String filename = "/Users/surbhi/Documents/workspace/NPV/src/main/resources/output/ParsedVideoFile"+initialLetter+dt+".csv";
        //String filename1 = "/Users/saurabh/N4Mative/idomoo/redpepper/FNB_V3/InputFile/remainingRecords/ParsedVideoFile"+initialLetter+".csv";
        try (Connection conn = ConnectionPool.getConnection(host, db)){
            
            FileWriter fw = new FileWriter(filename);
            //FileWriter fw1 = new FileWriter(filename1);
            List<Client> clients = getClient(initialLetter);
            
            String header="customer_id|customer_name|customer_filename|Account_type|current_date|amount_money_in|amount_money_out|amount_status|dates_from_until|date_from|date_to|possible_cash|amount_possible_shortfall|AVAIL_BALANCE|Upcoming_payments|cash_scale|shortfall_scale|avail_scale|payments_scale|cash_number|short_number|avail_number|payment_number|AI_scale|AO_scale|AI_number|AO_number|AI_FULL|AO_FULL";
            fw.append(header);
            fw.append('\n');
              for(Client clt : clients){
            	  //String allDataString = clt.getData_string();
                  String[] data = clt.getData_string().split("\\|");
                  Date dateFrom = new Date(data[9]);
                  Date dateTo = new Date(data[10]);
                  
                 if (dateFrom.after(dateTo)){
                      
                	 data[10]=data[9];
                	 data[8]=data[9]+"-"+data[10];
                	 String UpdatedString=data[0]+'|'+data[1]+'|'+data[2]+'|'+data[3]+'|'+data[4]+'|'+data[5]+'|'+data[6]+'|'+data[7]+'|'+data[8]+'|'+data[9]+'|'+data[10]+'|'+data[11]+'|'+data[12]+'|'+data[13]+'|'+data[14]+'|'+data[15]+'|'+data[16]+'|'+data[17]+'|'+data[18]+'|'+data[19]+'|'+data[20]+'|'+data[21]+'|'+data[22]+'|'+data[23]+'|'+data[24]+'|'+data[25]+'|'+data[26]+'|'+data[27]+'|'+data[28];
                	 //fw.append(clt.getData_string());
                	 fw.append(UpdatedString);
                	 fw.append('\n');
                	 update_statusCsvCreated_initial(clt.getClient_id(), clt.getProject_id(), clt.getGenerate_id(), clt.getCustomer_id());

                 }else{
                	fw.append(clt.getData_string());
                	fw.append('\n');
                    update_statusCsvCreated_initial(clt.getClient_id(), clt.getProject_id(), clt.getGenerate_id(), clt.getCustomer_id());

                 }
                 
                 //else{
                    //  fw1.append(clt.getData_string());
                    
                    
                    //fw1.append('\n');
                  //}
                  
             }
                System.out.println("Parsed CSV File is created successfully.");
              
                fw.flush();
                fw.close();
                //fw1.flush();
                //fw1.close();
                conn.close();
              
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void update_statusCsvCreated_initial(int clientId, int projectId, int generateID, String customer_id) {
    	CallableStatement stmt = null;
		String strSQL = "{call sp_UpdateCsvCreated_initial(?,?,?,?)}";
		
		try (Connection conn = ConnectionPool.getConnection(host, db)) {
			  	
				stmt = conn.prepareCall(strSQL);
				// set all the preparedstatement parameters
				
				stmt.setInt(1, clientId);
				stmt.setInt(2, projectId);
				stmt.setInt(3, generateID);
				stmt.setString(4, customer_id);
				
			    // execute the preparedstatement insert
				stmt.executeQuery();
			    //st.close();
				System.out.println("Record updated with initial csv Created status");
			    //return true;
		  } 
		  catch (Exception e)
		  {
			  //throw new RuntimeException(e);
			  e.printStackTrace();
		  } finally {
	            try {
	            	if (stmt != null) {
	            		stmt.close();
	            	}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
    
    private static void readPurlFile(String csvFile){
    	BufferedReader br = null;
        String line = "";
        String csvSplitBy = "\\|";
        //String firstName="";
        try {
        	
        	br = new BufferedReader(new FileReader(csvFile));
            String headerline = br.readLine();
            int customer_name=0;
            int customer_id = 0;
            int video_link=0;
            int thumbnail=0;
            int thumbnail_1=0;
            String[] header=headerline.split(csvSplitBy);
            Map<String,Integer> indx = new HashMap<String,Integer>();
        	for (int i=0; i< header.length;i++){
        		if (header[i].toUpperCase().equals("CUSTOMER_NAME")){
        			customer_name=i;
        		}else if(header[i].toUpperCase().equals("CUSTOMER_ID")){
        			customer_id=i;
        		}else if(header[i].toUpperCase().equals("VIDEO")){
        			video_link=i;
        		}
        		else if(header[i].toUpperCase().equals("THUMBNAIL")){
        			thumbnail=i;
        		}
        		else if(header[i].toUpperCase().equals("THUMBNAIL_1")){
        			thumbnail_1=i;
        		}
        	    indx.put(header[i], i);
       	}
            while ((line = br.readLine()) != null) {
            	
            	
            	String[] data = line.split(csvSplitBy);
            	
            	String customer_id_1=data[customer_id];
            	//String customer_name_1=data[customer_name];
            	String video_link_1=data[video_link];
            	String thumbnail_s=data[thumbnail];
            	String thumbnail_1_1=data[thumbnail_1];
            	String video_file= video_link_1.substring(video_link_1.lastIndexOf("/")+1);
            	
            	String purl_string= line;
               
            	/*List<Client> clients = getClient();
            	for(Client clt : clients){
            		
    			    int client_id=clt.getClient_id();
    			    int project_id=clt.getProject_id();
    			    int generate_id=clt.getGenerate_id();*/
    			    
    			    //upadteClient_purlReceived(client_id,project_id,generate_id,customer_id_1,video_link_1,video_file,thumbnail_s,thumbnail_1_1,purl_string);
            	String strDelim="|";
            	String[] data1 = line.split(csvSplitBy);
            	String outLine="";
            	//System.out.println(data1[data1.length-3]);
            	outLine+=data1[indx.get("customer_id")]+strDelim;
            	outLine+=data1[indx.get("customer_name")]+strDelim;
            	outLine+=data1[indx.get("current_date")]+strDelim;
            	outLine+=data1[indx.get("amount_money_in")]+strDelim;
            	outLine+=data1[indx.get("amount_money_out")]+strDelim;
            	outLine+=data1[indx.get("dates_from_until")]+strDelim;
            	outLine+=data1[indx.get("amount_possible_shortfall")]+strDelim;
            	outLine+=data1[indx.get("possible_cash")]+strDelim;
            	outLine+=data1[indx.get("Upcoming_payments")]+strDelim;
            	outLine+=data1[indx.get("AVAIL_BALANCE")]+strDelim;
            	outLine+=data1[indx.get("Account_type")]+strDelim;
            	outLine+=data1[indx.get("amount_status")]+strDelim;
            	outLine+=data1[indx.get("video")];
            	System.out.println(outLine);
    			    upadteClient_purlReceived(1,1,2,customer_id_1,video_link_1,video_file,thumbnail_s,thumbnail_1_1,purl_string, outLine);

            	}
            //}
            
         //createFinalDispatchCsv();
           
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
    
    private static void upadteClient_purlReceived(int clientId, int projectId, int generateID, String customer_id, 
    											String video_link,String video_file, String thumbnail, String thumbnail1, String purl_string, String QA_Purl) {
    	CallableStatement stmt = null;
		String strSQL = "{call sp_UpdatePurlReceived(?,?,?,?,?,?,?,?,?,?)}";
		
		try (Connection conn = ConnectionPool.getConnection(host, db)) {
			  	
				stmt = conn.prepareCall(strSQL);
				// set all the preparedstatement parameters
				
				stmt.setInt(1, clientId);
				stmt.setInt(2, projectId);
				stmt.setInt(3, generateID);
				stmt.setString(4, customer_id);
				stmt.setString(5, video_link);
				stmt.setString(6, video_file);
				stmt.setString(7, thumbnail);
				stmt.setString(8, thumbnail1);
				stmt.setString(9, purl_string);
				stmt.setString(10, QA_Purl);
				
			    // execute the preparedstatement insert
				stmt.executeQuery();
			    //st.close();
				System.out.println("Record updated with Purl File Received status");
			    //return true;
		  } 
		  catch (Exception e)
		  {
			  //throw new RuntimeException(e);
			  e.printStackTrace();
		  } finally {
	            try {
	            	if (stmt != null) {
	            		stmt.close();
	            	}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	
	}
    
    private static void createFinalDispatchCsv(String initialLetter, String dt){
    	
    	//String initialLetter = "*";
	    String filename = "/Users/surbhi/Documents/workspace/NPV/src/main/resources/output/FinalDispatch"+initialLetter+dt+".csv"; 
	    try (Connection conn = ConnectionPool.getConnection(host, db)){
	    	
	    	FileWriter fw = new FileWriter(filename);
	    	List<Client> clients = getClient(initialLetter);
	    	
        	String header="Customer_id|Customer_name|Video|ThumbNail_link";
        	fw.append(header);
        	fw.append('\n');
          	for(Client clt : clients){
			    fw.append(clt.getCustomer_id());
			    fw.append('|');
			    fw.append(clt.getCustomer_name());
			    fw.append('|');
			    fw.append(clt.getVideo_file());
			    fw.append('|');
			    fw.append(clt.getThumbnail_link());
			    
			    fw.append('\n');
			    update_status_FinalCsv_create(clt.getClient_id(), clt.getProject_id(), clt.getGenerate_id(), clt.getCustomer_id());
          	}
	            System.out.println(" Final Dispatched CSV is created successfully.");
          	
	            fw.flush();
			    fw.close();
			    conn.close();
          	
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
    }
    
    private static void update_status_FinalCsv_create(int clientId, int projectId, int generateID, String customer_id) {
    	CallableStatement stmt = null;
		String strSQL = "{call sp_update_status_FinalCsv_create(?,?,?,?)}";
		
		try (Connection conn = ConnectionPool.getConnection(host, db)) {
			  	
				stmt = conn.prepareCall(strSQL);
				// set all the preparedstatement parameters
				
				stmt.setInt(1, clientId);
				stmt.setInt(2, projectId);
				stmt.setInt(3, generateID);
				stmt.setString(4, customer_id);
				
			    // execute the preparedstatement insert
				stmt.executeQuery();
			    //st.close();
				System.out.println("Record updated with Final dispatched Csv status");
			    //return true;
		  } 
		  catch (Exception e)
		  {
			  //throw new RuntimeException(e);
			  e.printStackTrace();
		  } finally {
	            try {
	            	if (stmt != null) {
	            		stmt.close();
	            	}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		
		
	}
}

