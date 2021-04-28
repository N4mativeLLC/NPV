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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

//import org.springframework.core.env.Environment;
import com.n4mative.database.ConnectionPool;
//import com.n4mative.database.ConnectionPool;

public class centimark {
	
	//static String host = "192.168.1.71";
    //static String db = "NPV_QA";
    //static String clientName="centimark";
    
    public static void main(String[] args) throws ParseException {
    	
    	/*String inputFile= args[1];
    	String outputFilePath=args[2];
    	String host = args[3];
        String db = args[4];
        String clientName=args[5];
        String username=args[6];*/
        
    	String inputFile= "/Users/surbhi/git/NPV/src/main/resources/Centimark/INPUT/purl_NewRoof.csv";
    	String outputFilePath="/Users/surbhi/git/NPV/src/main/resources/Centimark/OUTPUT/";
    	String host = "35.156.191.67";//"192.168.1.71";
        String db = "NPV_Analytics";//"NPV_QA";
        String clientName="centimark";
        String username= "root";//"liamapp";
    	String dt = new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance().getTime());
    	
    	if (args[0].equalsIgnoreCase("initialFile")){
    		readInitialFile(inputFile,host,db,clientName,username);
    	}else if (args[0].equalsIgnoreCase("createInitial")){
    		createInitialCsv_noDateDiff("*",dt,outputFilePath,host,db,clientName,username);
    	}else if (args[0].equalsIgnoreCase("purlFile")){
    		readPurlFile(inputFile,host,db,clientName,username);
    	}else if (args[0].equalsIgnoreCase("createFinal")){
    		createFinalDispatchCsv("*",dt,outputFilePath,host,db,clientName,username);
    	}
    }
    
    private static void readInitialFile(String csvFile, String host, String db, String clientName, String username) throws ParseException{
    	BufferedReader br = null;
        String line = "";
        String csvSplitBy = "\\|";
        //String firstName="";
        try {
        	
        	int count= 1;
            br = new BufferedReader(new FileReader(csvFile));
            String headerline = br.readLine();
            //while ((line = br.readLine()) != null && count <=10) {
            while ((line = br.readLine()) != null) {
            	String[] data = line.split(csvSplitBy);
            	//String customer_id=data.length > 0 ? data[5].trim() : "";
            	String customer_id=data.length > 0 ? data[5].trim()+"_"+data[7] : "";
            	String customer_name=data.length > 1 ? data[3].trim() : "";
            	String data_string= line;
            	
            	saveClient(count,customer_id,customer_name,data_string,host,db,clientName,username);
                count = count+1;
            }
            System.out.println(count);
            
            preprocess(host,db,clientName,username);
            
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
    
    private static void saveClient(int count, String customer_id, String customer_name,  String data_string, String host, String db, String clientName, String username) {
		
    	//System.out.println("host: " + host + "  db: " + db);
        CallableStatement stmt = null;
		String strSQL = "{call sp_InsertClientInfo(?,?,?,?,?,?,?,?)}";
		
		try (Connection conn = ConnectionPool.getConnection(host, db,username)) {
			  	
				stmt = conn.prepareCall(strSQL);
				// set all the preparedstatement parameters
				stmt.setString(1, clientName);
				stmt.setInt(2, 6);
				stmt.setInt(3, 2);
				stmt.setInt(4, 10);
				stmt.setString(5, customer_id);
				stmt.setString(6, customer_name);
				stmt.setString(7, data_string);
				stmt.setString(8, "Record Recieved");
				
			    // execute the preparedstatement insert
				stmt.executeQuery();
			    //st.close();
				//System.out.println("Record Received");
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

	private static void preprocess(String host, String db, String clientName, String username) throws ParseException {
		String initialLetter = "*";
		List<Client> clients = getClient(initialLetter,host,db,clientName,username,"Record Recieved");
		String strDelim="|";
		String cvsSplitBy = "\\"+strDelim;
		for(Client clt : clients){
            String line=clt.getData_string();
           
            String dataRow="";
			line=line.trim();
        	if (!line.endsWith("|"))
        			line+=strDelim;
        	String[] data = line.split(cvsSplitBy);
        
        	String PM_FIRST_CAP=data[0].toUpperCase();
        	String CONTACT_FIRST_CAP=data[3].toUpperCase();
        	String serviceFullName = data[8]+" "+data[9];
        	String Service_Full_CAP= (data[8]+"_"+data[9]).toUpperCase();
        	
        	SimpleDateFormat month_date = new SimpleDateFormat("MMMM yyyy dd", Locale.ENGLISH);
        	SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yy");
        	Date date = sdf.parse(data[7]);
        	String fullDate = month_date.format(date);
        	String Inv_Date[]=fullDate.split(" ");
        	String Inv_Year=Inv_Date[1];
        	String Inv_Month=Inv_Date[0];
        	String Inv_Day=Inv_Date[2];
        	String Month_Year = Inv_Month.toUpperCase()+" "+Inv_Year;
        	String client_uniqueIdentity = data[5]+"_"+data[7]; 
        	
        	dataRow+=data[0]+strDelim+PM_FIRST_CAP+strDelim+data[1]+strDelim+data[2]+strDelim+data[3]
        			+strDelim+CONTACT_FIRST_CAP+strDelim+data[4]+strDelim+data[5]+strDelim+client_uniqueIdentity
        			+strDelim+data[6]+strDelim+data[7]+strDelim+data[8]+strDelim+data[9]+strDelim+serviceFullName
        			+strDelim+Service_Full_CAP+strDelim+Inv_Month+strDelim+Inv_Day+strDelim+Inv_Year
        			+strDelim+Month_Year;
	    
	     update_status_PreProcess(clt.getClient_id(),clt.getProject_id(),
	            clt.getGenerate_id(),clt.getCustomer_id(),dataRow,host,db,clientName,username);
	}
            
			//System.out.println(" Pre Process completed successfully.");
	}

	private static void update_status_PreProcess(int client_id, int project_id, int generate_id, String customer_id, String dataRow, String host, String db, String clientName, String username) {

    	CallableStatement stmt = null;
		String strSQL = "{call sp_Update_status_preprocess_v1(?,?,?,?,?,?)}";
		
		try (Connection conn = ConnectionPool.getConnection(host, db,username)) {
			  	
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
				//System.out.println("Status updated with pre process completed");
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
 
	public static List<Client> getClient(String initialLetter, String host, String db, String clientName,String username,String status) {
		PreparedStatement ps = null;
		CallableStatement stmt = null;
		ResultSet rs = null;
		List<Client> clients = new ArrayList<Client>();
		try (java.sql.Connection conn = ConnectionPool.getConnection(host, db,username)) {

        	String strSQL = "{call sp_GetClientInfo(?,?,?)}";
        	stmt = conn.prepareCall(strSQL);
        	stmt.setString(1, initialLetter);
        	stmt.setString(2, clientName);
        	stmt.setString(3, status);
        	
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
	
	private static void createInitialCsv_noDateDiff( String initialLetter, String dt, String outputFilePath, String host, String db, String clientName, String username){
      
    	//String filename = "/Users/surbhi/Documents/workspace/NPV/src/main/resources/output/ParsedVideoFile"+initialLetter+dt+".csv";
    	String filename = outputFilePath+"parsedvideoFile"+initialLetter+dt+".csv";

        try (Connection conn = ConnectionPool.getConnection(host, db,username)){
            
            FileWriter fw = new FileWriter(filename);
            //FileWriter fw1 = new FileWriter(filename1);
            List<Client> clients = getClient(initialLetter,host,db,clientName,username,"preprocess completed");
            
            //String header="MemberNumber,FirstName,firstnamefixed,YearsMember,StatementYear,YearOfJoining,ProfShareBal,ClosingBal,OperativeProfit,InvReturns,Country,Occupation,NumberOfProducts,AgeAsAtEndOfBonusYear,PPS_Sickness_and_Permanent_Incapacity_Benefit,Professional_Life_Provider,Critical_Illness_Cover,Professional_Disability_Provider,Accidental_Death_Benefit,Education_Cover,PPSi_Bonus_Total,MedAid_Bonus_Total,Advisor,Advisor_Email,curr1,curr2,curr3,curr4,curr5,curr6,curr7,Rec1,Rec2,Rec3,Rec4,Rec5,Rec6,Rec7,Rec8,RecCount";
            
            String header="PM_First|PM_FIRST_CAP|PM_Last|Client_Account|client_first|CONTACT_FIRST_CAP|"
            		+ "Client_Last|Client_Email|Client_uniqueIdentity|Inv_Amt|Inv_Date|Service_First|Service_last|Service_Full|"
            		+ "Service_Full_CAP|Inv_Month|Inv_Date|Inv_Year|Month_Year";
            
            fw.append(header);
            fw.append('\n');
              for(Client clt : clients){
            	  String allDataString = clt.getData_string();
            	  fw.append(allDataString);
                	 fw.append('\n');
                	 update_statusCsvCreated_initial(clt.getClient_id(), clt.getProject_id(), 
                			 clt.getGenerate_id(), clt.getCustomer_id(),host,db,clientName,username);

                
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
	
    private static void update_statusCsvCreated_initial(int clientId, int projectId, int generateID, String customer_id, String host, String db, String clientName, String username) {
    	CallableStatement stmt = null;
		String strSQL = "{call sp_UpdateCsvCreated_initial_v1(?,?,?,?,?)}";
		
		try (Connection conn = ConnectionPool.getConnection(host, db,username)) {
			  	
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
				//System.out.println("Record updated with initial csv Created status");
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
    
    private static void readPurlFile(String csvFile, String host, String db, String clientName, String username){
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
        		if (header[i].toUpperCase().equals("CONTACT_FIRST_CAP")){
        			customer_name=i;
        		}else if(header[i].toUpperCase().equals("Client_uniqueIdentity")){
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
            	
            	//System.out.println(data1[indx.get("MemberNumber")]);
            	outLine+=data1[indx.get("Client_uniqueIdentity")]+strDelim;
            	outLine+=data1[indx.get("CONTACT_FIRST_CAP")]+strDelim;
            	outLine+=data1[indx.get("PM_FIRST_CAP")]+strDelim;
            	outLine+=data1[indx.get("PM_First")]+strDelim;
            	outLine+=data1[indx.get("client_first")]+strDelim;
            	outLine+=data1[indx.get("Inv_Date")]+strDelim;
            	outLine+=data1[indx.get("Inv_Year")]+strDelim;
            	outLine+=data1[indx.get("Inv_Month")]+strDelim;
            	outLine+=data1[indx.get("Client_Account")]+strDelim;
            	outLine+=data1[indx.get("Inv_Amt")]+strDelim;
            	outLine+=data1[indx.get("Service_Full_CAP")]+strDelim;
            	outLine+=data1[indx.get("Service_Full")]+strDelim;
            	outLine+=data1[indx.get("Service_First")]+strDelim;
            	outLine+=data1[indx.get("video")]+strDelim;
            	outLine+=data1[indx.get("thumbnail_1")];
            	
            	System.out.println(outLine);
    			    upadteClient_purlReceived(6,2,10,customer_id_1,video_link_1,video_file,thumbnail_s,
    			    		thumbnail_1_1,purl_string, outLine,host,db,clientName,username);

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
    											String host, String db, String clientName, String username) {
    	CallableStatement stmt = null;
		String strSQL = "{call sp_UpdatePurlReceived_v1(?,?,?,?,?,?,?,?,?,?,?)}";
		
		try (Connection conn = ConnectionPool.getConnection(host, db,username)) {
			  	
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
    
    private static void createFinalDispatchCsv(String initialLetter, String dt, String outputFilePath, String host, String db, String clientName, String username){
    	
    	//String filename = "/Users/surbhi/Documents/workspace/NPV/src/main/resources/output/FinalDispatch"+initialLetter+dt+".csv"; 
    	String filename = outputFilePath+"finalDispatch"+initialLetter+dt+".csv"; 

    	try (Connection conn = ConnectionPool.getConnection(host, db,username)){
	    	
	    	FileWriter fw = new FileWriter(filename);
	    	List<Client> clients = getClient(initialLetter,host,db,clientName,username,"Purl Received");
	    	
        	String header="Customer_uniqueIdentity,Customer_email,Customer_name,Video,ThumbNail_link";
        	fw.append(header);
        	fw.append('\n');
          	for(Client clt : clients){
          		if(clt.getVideo_file()!=null){
				    fw.append(clt.getCustomer_id());
				    fw.append(',');
				    String customer_uniqueidentity=clt.getCustomer_id();
					int emailIndex= customer_uniqueidentity.lastIndexOf("_");
					String customer_email= customer_uniqueidentity.substring(0,emailIndex);
				    fw.append(customer_email);
				    fw.append(',');
				    fw.append(clt.getCustomer_name());
				    fw.append(',');
				    fw.append("https://npv.n4mative.com/nvidyoPlayer.html?v=/videos/centimark/annualroof/"+
				    			clt.getVideo_file());
				    fw.append(',');
				    //fw.append("https://pps.nvidyo.co.za/thumb/pps/prof_share/"+clt.getVideo_file().replaceAll(".mp4",".jpg"));
				    String thumb=clt.getThumbnail_link1().substring(clt.getThumbnail_link1().lastIndexOf("/")+1);
				    fw.append("https://npv.n4mative.com/thumb/centimark/annualroof/"+ thumb);
				    fw.append('\n');
				    update_status_FinalCsv_create(clt.getClient_id(), clt.getProject_id(), 
				    		clt.getGenerate_id(), clt.getCustomer_id(),host,db,clientName,username);
          		}
          	}
	            System.out.println(" Final Dispatched CSV is created successfully.");
          	
	            fw.flush();
			    fw.close();
			    conn.close();
          	
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
    }
    
    private static void update_status_FinalCsv_create(int clientId, int projectId, int generateID, String customer_id, String host, String db, String clientName, String username) {
    	CallableStatement stmt = null;
		String strSQL = "{call sp_update_status_FinalCsv_create_v1(?,?,?,?,?)}";
		
		try (Connection conn = ConnectionPool.getConnection(host, db,username)) {
			  	
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

