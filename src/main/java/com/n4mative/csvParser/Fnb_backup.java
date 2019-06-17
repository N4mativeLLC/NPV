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
import java.util.ArrayList;
import java.util.List;

//import com.n4mative.csvParser.Invitee;
import org.springframework.core.env.Environment;


import com.n4mative.database.ConnectionPool;

public class Fnb_backup {
	
	
	private static Environment env;
	static String host = "192.168.1.71";
    static String db = "NPV";
    
    public static void main(String[] args) {
    	
    	String csvFile = args[1];//"/Users/surbhi/Documents/workspace/NPV/src/main/resources/n4mative - CSV - 20 Sample.csv";
    	
    	if (args[0].equalsIgnoreCase("initialFile")){
    		readInitialFile(csvFile);
    	}else if (args[0].equalsIgnoreCase("purlFile")){
    		//readCsvWithLonglink(csvFile);
    	}
    }
    
    private static void readInitialFile(String csvFile){
    	BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        //String firstName="";
        try {
        	
        	int count= 1;
            br = new BufferedReader(new FileReader(csvFile));
            String headerline = br.readLine();
            while ((line = br.readLine()) != null) {
            	
            	String[] data = line.split(cvsSplitBy);
            	String cutomer_id=data.length > 0 ? data[0].trim() : "";
            	String customer_name=data.length > 1 ? data[1].trim() : "";
            	String data_string= line;
                                
                //System.out.println("Customer_Id= " + cutomer_id + " , firstName= " + customer_name);
                //System.out.println("Remain = " + data_string);
                
                saveClient(count,cutomer_id,customer_name,data_string);
                count = count+1;
            }
            
            preprocess();
            createInitialCsv();
           
             
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
    
    private static void saveClient(int count, String cutomer_id, String customer_name,  String data_string) {
		
    	System.out.println("host: " + host + "  db: " + db);
        CallableStatement stmt = null;
		String strSQL = "{call sp_InsertFnbInfo(?,?,?,?,?,?,?)}";
		
		try (Connection conn = ConnectionPool.getConnection(host, db)) {
			  	
				stmt = conn.prepareCall(strSQL);
				// set all the preparedstatement parameters
				stmt.setInt(1, 1);
				stmt.setInt(2, 1);
				stmt.setInt(3, 1);
				stmt.setString(4, cutomer_id);
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
		
		List<Client> clients = getClient();
		String cvsSplitBy = ",";
		for(Client clt : clients){
			String data_string=clt.getData_string();

        	String dataRow="";
        	
        	String[] data1 = data_string.split(cvsSplitBy);
        	String[] data;
        	if (data1.length < 12){
        		data_string+="0,";
        		data = data_string.split(cvsSplitBy);
        	}else{
        		data = data1.clone();
        	}
        	
            dataRow+=data[0].trim()+","+data[1].trim()+","+data[2].trim()+","+data[3].trim()+","+data[4].trim()+","+data[5].trim()+",";
            //If money out is greater than Money In, then add Red to string else green
            String data5="Green";
            if (!data[4].equalsIgnoreCase("NULL") && !data[5].equalsIgnoreCase("NULL")){
             data5=Float.valueOf(data[5])-Float.valueOf(data[4])>0?"Red":"Green";
            }
            String abc = data[8].equalsIgnoreCase("")?"0":data[8];
            
            //dataRow+=data5+","+data[6].substring(0, data[6].indexOf(" "))+" - "+data[7].substring(0, data[7].indexOf(" "))+","+data[6].substring(0, data[6].indexOf(" "))+","+data[7].substring(0, data[7].indexOf(" "))+",";
            dataRow+=data5+","+data[6]+" - "+data[7]+","+data[6]+","+data[7]+",";
            dataRow+= data[8].equalsIgnoreCase("")?"0,":data[8]+",";
            dataRow+= data[9].equalsIgnoreCase("")?"0,":data[9]+",";
            dataRow+= data[10].equalsIgnoreCase("")?"0,":data[10]+",";
            dataRow+= data[11].equalsIgnoreCase("")?"0,":data[11];
            
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
 
	public static List<Client> getClient() {
		PreparedStatement ps = null;
		CallableStatement stmt = null;
		ResultSet rs = null;
		List<Client> clients = new ArrayList<Client>();
		try (java.sql.Connection conn = ConnectionPool.getConnection()) {

        	String strSQL = "{call sp_GetFnbInfo()}";
        	stmt = conn.prepareCall(strSQL);
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
	
    private static void createInitialCsv(){
    	
	    String filename = "/Users/surbhi/Documents/workspace/NPV/src/main/resources/output/ParsedVideoFile.csv"; 
	    try (Connection conn = ConnectionPool.getConnection(host, db)){
	    	
	    	FileWriter fw = new FileWriter(filename);
	    	List<Client> clients = getClient();
	    	
        	String header="customer_id,customer_name,Account_type,current_date,amount_money_in,amount_money_out,amount_status,dates_from_until,date_from,date_to,possible_cash,amount_possible_shortfall,AVAIL_BALANCE,Upcoming_payments";
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
 
}

