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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.text.WordUtils;

//import org.springframework.core.env.Environment;
import com.n4mative.database.ConnectionPool;
//import com.n4mative.database.ConnectionPool;

public class absa {
	
	//static String host = "192.168.1.71";
    //static String db = "NPV_QA";
    //static String clientName="absa";
    
    public static void main(String[] args) {
    	
    	/*String inputFile= args[1];
    	//String purlFile= args[1];
    	String outputFilePath=args[2];
    	String host = args[3];
        String db = args[4];
        String clientName=args[5];*/
        //String inputFile="/Users/surbhi/git/NPV/src/main/resources/InputData.csv";
    	String inputFile= "/Users/surbhi/git/NPV/src/main/resources/purl-20190718.csv";
    	String outputFilePath="/Users/surbhi/git/NPV/src/main/resources/output/absa";
    	String host = "192.168.1.71";
        String db = "NPV_QA";
        String clientName="absa";
    	String dt = new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance().getTime());
    	
    	if (args[0].equalsIgnoreCase("initialFile")){
    		readInitialFile(inputFile,host,db,clientName);
    		createInitialCsv_noDateDiff("*",dt,outputFilePath,host,db,clientName);
    		
    	}else if (args[0].equalsIgnoreCase("purlFile")){
    		readPurlFile(inputFile,host,db,clientName);
    		createFinalDispatchCsv("*",dt,outputFilePath,host,db,clientName);
    	}
    }
    
    private static void readInitialFile(String csvFile, String host, String db, String clientName){
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
            	
            	saveClient(count,customer_id,customer_name,data_string,host,db,clientName);
                count = count+1;
            }
            
            preprocess(host,db,clientName);
            
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
    
    private static void saveClient(int count, String customer_id, String customer_name,  String data_string, String host, String db, String clientName) {
		
    	//System.out.println("host: " + host + "  db: " + db);
        CallableStatement stmt = null;
		String strSQL = "{call sp_InsertClientInfo(?,?,?,?,?,?,?,?)}";
		
		try (Connection conn = ConnectionPool.getConnection(host, db)) {
			  	
				stmt = conn.prepareCall(strSQL);
				// set all the preparedstatement parameters
				stmt.setString(1, clientName);
				stmt.setInt(2, 4);
				stmt.setInt(3, 1);
				stmt.setInt(4, 3);
				stmt.setString(5, customer_id);
				stmt.setString(6, customer_name);
				stmt.setString(7, data_string);
				stmt.setString(8, "Record Recieved");
				
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

	private static void preprocess(String host, String db, String clientName) {
		String initialLetter = "*";
		List<Client> clients = getClient(initialLetter,host,db,clientName);
		String strDelim="|";
		String cvsSplitBy = "\\"+strDelim;
		for(Client clt : clients){
			String line=clt.getData_string();
			
			String dataRow="";
			String parsedFileName="";
        	line=line.trim();
        	if (!line.endsWith("|"))
        			line+=strDelim;
        	String[] data = line.split(cvsSplitBy);
        	String parsedName="Hello, "+WordUtils.capitalizeFully(data[1]);
        	parsedFileName=data[1].replaceAll("-", "_");
        
        	dataRow+=data[0]+ strDelim +WordUtils.capitalizeFully(data[1])+strDelim+ parsedName
        			+strDelim+parsedFileName+ strDelim+data[2];         
        	
        	update_status_PreProcess(clt.getClient_id(),clt.getProject_id(),
        			clt.getGenerate_id(),clt.getCustomer_id(),dataRow,host,db,clientName);
      	}
            
			//System.out.println(" Pre Process completed successfully.");
	}

	private static void update_status_PreProcess(int client_id, int project_id, int generate_id, String customer_id, String dataRow, String host, String db, String clientName) {

    	CallableStatement stmt = null;
		String strSQL = "{call sp_Update_status_preprocess_v1(?,?,?,?,?,?)}";
		
		try (Connection conn = ConnectionPool.getConnection(host, db)) {
			  	
				stmt = conn.prepareCall(strSQL);
				// set all the preparedstatement parameters
				
				stmt.setString(1, clientName);
				stmt.setInt(2, client_id);
				stmt.setInt(3, project_id);
				stmt.setInt(4, generate_id);
				stmt.setString(5, customer_id);
				stmt.setString(6, dataRow);
				
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
 
	public static List<Client> getClient(String initialLetter, String host, String db, String clientName) {
		PreparedStatement ps = null;
		CallableStatement stmt = null;
		ResultSet rs = null;
		List<Client> clients = new ArrayList<Client>();
		try (java.sql.Connection conn = ConnectionPool.getConnection(host, db)) {

        	String strSQL = "{call sp_GetClientInfo(?,?)}";
        	stmt = conn.prepareCall(strSQL);
        	stmt.setString(1, initialLetter);
        	stmt.setString(2, clientName);
        	
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
	
	private static void createInitialCsv_noDateDiff( String initialLetter, String dt, String outputFilePath, String host, String db, String clientName){
      
    	//String filename = "/Users/surbhi/Documents/workspace/NPV/src/main/resources/output/ParsedVideoFile"+initialLetter+dt+".csv";
    	String filename = outputFilePath+"parsedvideoFile"+initialLetter+dt+".csv";

        try (Connection conn = ConnectionPool.getConnection(host, db)){
            
            FileWriter fw = new FileWriter(filename);
            //FileWriter fw1 = new FileWriter(filename1);
            List<Client> clients = getClient(initialLetter,host,db,clientName);
            
            String header="Customer_id|CustomerName|HelloName|FileName|ABSA_Rewards_Member";
            fw.append(header);
            fw.append('\n');
              for(Client clt : clients){
            	  String allDataString = clt.getData_string();
            	  fw.append(allDataString);
                	 fw.append('\n');
                	 update_statusCsvCreated_initial(clt.getClient_id(), clt.getProject_id(), 
                			 clt.getGenerate_id(), clt.getCustomer_id(),host,db,clientName);

                
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
	
    private static void update_statusCsvCreated_initial(int clientId, int projectId, int generateID, String customer_id, String host, String db, String clientName) {
    	CallableStatement stmt = null;
		String strSQL = "{call sp_UpdateCsvCreated_initial_v1(?,?,?,?,?)}";
		
		try (Connection conn = ConnectionPool.getConnection(host, db)) {
			  	
				stmt = conn.prepareCall(strSQL);
				// set all the preparedstatement parameters
				
				stmt.setString(1, clientName);
				stmt.setInt(2, clientId);
				stmt.setInt(3, projectId);
				stmt.setInt(4, generateID);
				stmt.setString(5, customer_id);
				
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
    
    private static void readPurlFile(String csvFile, String host, String db, String clientName){
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
               
            	String strDelim="|";
            	String[] data1 = line.split(csvSplitBy);
            	String outLine="";
            	//System.out.println(data1[data1.length-3]);
            	System.out.println(indx.get("Customer_id"));
            	System.out.println(data1[indx.get("Customer_id")]);
            	outLine+=data1[indx.get("Customer_id")]+strDelim;
            	outLine+=data1[indx.get("CustomerName")]+strDelim;
            	outLine+=data1[indx.get("video")];
            	System.out.println(outLine);
    			    upadteClient_purlReceived(4,1,3,customer_id_1,video_link_1,video_file,thumbnail_s,
    			    		thumbnail_1_1,purl_string, outLine,host,db,clientName);

            	}
         
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
    											String video_link,String video_file, String thumbnail, 
    											String thumbnail1, String purl_string, String QA_Purl, 
    											String host, String db, String clientName) {
    	CallableStatement stmt = null;
		String strSQL = "{call sp_UpdatePurlReceived_v1(?,?,?,?,?,?,?,?,?,?,?)}";
		
		try (Connection conn = ConnectionPool.getConnection(host, db)) {
			  	
				stmt = conn.prepareCall(strSQL);
				// set all the preparedstatement parameters
				
				stmt.setString(1, clientName);
				stmt.setInt(2, clientId);
				stmt.setInt(3, projectId);
				stmt.setInt(4, generateID);
				stmt.setString(5, customer_id);
				stmt.setString(6, video_link);
				stmt.setString(7, video_file);
				stmt.setString(8, thumbnail);
				stmt.setString(9, thumbnail1);
				stmt.setString(10, purl_string);
				stmt.setString(11, QA_Purl);
				
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
    
    private static void createFinalDispatchCsv(String initialLetter, String dt, String outputFilePath, String host, String db, String clientName){
    	
    	//String filename = "/Users/surbhi/Documents/workspace/NPV/src/main/resources/output/FinalDispatch"+initialLetter+dt+".csv"; 
    	String filename = outputFilePath+"finalDispatch"+initialLetter+dt+".csv"; 

    	try (Connection conn = ConnectionPool.getConnection(host, db)){
	    	
	    	FileWriter fw = new FileWriter(filename);
	    	List<Client> clients = getClient(initialLetter,host,db,clientName);
	    	
        	String header="Customer_id|Customer_name|Video";//|ThumbNail_link";
        	fw.append(header);
        	fw.append('\n');
          	for(Client clt : clients){
			    fw.append(clt.getCustomer_id());
			    fw.append('|');
			    fw.append(clt.getCustomer_name());
			    fw.append('|');
			    fw.append("https://absa.nvidyo.co.za/n4mvp/v2/testVideo/videoJsAutoPlayNm.html?v=/videos/absa/wimi_afm/"+
			    			clt.getVideo_file());
			    //fw.append('|');
			    //fw.append(clt.getThumbnail_link());
			    
			    fw.append('\n');
			    update_status_FinalCsv_create(clt.getClient_id(), clt.getProject_id(), 
			    		clt.getGenerate_id(), clt.getCustomer_id(),host,db,clientName);
          	}
	            System.out.println(" Final Dispatched CSV is created successfully.");
          	
	            fw.flush();
			    fw.close();
			    conn.close();
          	
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
    }
    
    private static void update_status_FinalCsv_create(int clientId, int projectId, int generateID, String customer_id, String host, String db, String clientName) {
    	CallableStatement stmt = null;
		String strSQL = "{call sp_update_status_FinalCsv_create_v1(?,?,?,?,?)}";
		
		try (Connection conn = ConnectionPool.getConnection(host, db)) {
			  	
				stmt = conn.prepareCall(strSQL);
				// set all the preparedstatement parameters
				
				stmt.setString(1, clientName);
				stmt.setInt(2, clientId);
				stmt.setInt(3, projectId);
				stmt.setInt(4, generateID);
				stmt.setString(5, customer_id);
				
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

