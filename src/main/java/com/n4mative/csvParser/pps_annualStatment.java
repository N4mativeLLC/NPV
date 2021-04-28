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
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

//import org.springframework.core.env.Environment;
import com.n4mative.database.ConnectionPool;
//import com.n4mative.database.ConnectionPool;

public class pps_annualStatment {
	
	//static String host = "192.168.1.71";
    //static String db = "NPV_QA";
    //static String clientName="absa";
	
	// 35.156.191.67
	// root
	// Gauch022$
	// NPV_QA 
	// NPV_Analytics
	static int client_id;
	static int project_id;
	static int generate_id;
	
    public static void main(String[] args) throws ParseException {
    	
    	//"/Users/rk/Downloads/PPS/PPS-Video/PSA_Statement_PROD_N4MATIVE_Final_Dummy_Run_20210311.csv"

    	String inputParam = args[0];
    	String inputFile= args[1];
    	String outputFilePath=args[2];
    	String host = args[3];
        String db = args[4];
        String clientName=args[5];
        String username=args[6];
        client_id = Integer.parseInt(args[7]);
    	project_id = Integer.parseInt(args[8]);
    	generate_id = Integer.parseInt(args[9]);
    	
        //String inputFile="/Users/saurabh/N4Mative/idomoo/PPS/annualstatement/datafiles/ProfitShare2019N4MATIVE_21Testcases.CSV";
    	//String inputFile= "/Users/surbhi/git/NPV/src/main/resources/PPS/INPUT/sampleInput.csv";
    	/*String inputFile= "/Users/surbhi/git/NPV/src/main/resources/PPS/INPUT/purl_two.csv";
    	//String outputFilePath="/Users/saurabh/N4Mative/idomoo/PPS/annualstatement/datafiles/";
    	String outputFilePath="/Users/surbhi/git/NPV/src/main/resources/PPS/OUTPUT/";
    	String host = "192.168.1.71";
        String db = "NPV_QA";
        String clientName="pps";
        String username= "liamapp";*/
    	String dt = new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance().getTime());
    	//String inputParam = "initialFile";
    	//if (args[0].equalsIgnoreCase("initialFile")){
    	if (inputParam.equalsIgnoreCase("initialFile")){ //step 1
    		readInitialFile(inputFile,host,db,clientName,username);
    	} else if (inputParam.equalsIgnoreCase("createInitial")){ //step 2 - use the output file for video generation in Idomoo platform
    		createInitialCsv_noDateDiff("*",dt,outputFilePath,host,db,clientName,username);
    	} else if (inputParam.equalsIgnoreCase("purlFile")){ // step 3 - this file comes from Idomoo platform after video generation is complete.
    		readPurlFile(inputFile,host,db,clientName,username);
    	} else if (inputParam.equalsIgnoreCase("createFinal")){ //step 4 - this will be sent out to client for video distribution
    		createFinalDispatchCsv("*",dt,outputFilePath,host,db,clientName,username);
    	}
    }
    
    static String toProperCase(String s) {
	    return s.substring(0, 1).toUpperCase() +
	               s.substring(1).toLowerCase();
	}
    
    private static void readInitialFile(String csvFile, String host, String db, String clientName, String username) throws ParseException{
    	BufferedReader br = null;
        String line = "";
        String csvSplitBy = "\\,";
        //String firstName="";
        try {
        	
        	int count= 0;
            br = new BufferedReader(new FileReader(csvFile));
            String headerline = br.readLine();
            //while ((line = br.readLine()) != null && count <=10) {
            while ((line = br.readLine()) != null) {
            	String[] data = line.split(csvSplitBy);
            	String customer_id=data.length > 0 ? data[0].trim() : "";
            	String customer_name=data.length > 1 ? data[1].trim() : "";
            	String data_string= line;
            	saveClient(count,customer_id,customer_name,data_string,host,db,clientName,username);
                count++;
                if(count > 9999 && (count % 10000 == 0)) {
			    	System.out.println("Updated 10K records");
			    }
            }
            System.out.println("Inserted records: " + count);
            
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
				stmt.setInt(2, client_id); //clientID
				stmt.setInt(3, project_id); //projectID
				stmt.setInt(4, generate_id); //generate_ID
				stmt.setString(5, customer_id);
				stmt.setString(6, customer_name);
				stmt.setString(7, data_string);
				stmt.setString(8, "Record Received");
				
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
		System.out.println("Fetching records for preprocess......");
		List<Client> clients = getClient(initialLetter,host,db,clientName,username,"Record Received");
		System.out.println("Starting preprocess......");
		String strDelim=",";
		String cvsSplitBy = "\\"+strDelim;
		Locale locale = new Locale("en", "ZA");
		
		int count = 0;
		for(Client clt : clients){
			String line=clt.getData_string();
            System.out.println(line);
            String dataRow="";
            StringBuilder dataRow1 = new StringBuilder();
            line=line.trim();
            if (!line.endsWith(strDelim))
		    	 line+=strDelim;
		    //line+=" "+strDelim;
		    //line+=" " +strDelim;
		    //line+=" " +strDelim;
	    
		    String[] data = line.split(cvsSplitBy);
	    
	     //String firstNameOld = data[53];
		    String firstName=data[1];
	    
		    String parsedName=firstName.trim().toUpperCase().replaceAll(" - ","_").replaceAll("-", "_").replaceAll("'", "_").replaceAll(" ", "_");
	     //System.out.println(parsedName.trim());
		    /*
		    for(int i=0;i<19;i++){
		    	dataRow1.append(data[i].trim()+strDelim);
		    	if(i==1){
		    		dataRow1.append(parsedName.trim()+strDelim);
		    	} else if(i>6 && i<13){
		    		dataRow1.append(data[i]+strDelim+currencyWithChosenLocalisation(Double.parseDouble( data[i]), locale)+strDelim);
		    	}
		    }
		    */
		    dataRow+=data[0]+strDelim+data[1].trim()+strDelim+parsedName.trim()
		     +strDelim+data[2]+strDelim+data[3]
            +strDelim+data[4]+strDelim+data[5]+strDelim+data[6]
            +strDelim+data[7]+strDelim+currencyWithChosenLocalisation(Double.parseDouble( data[7]), locale)
            +strDelim+data[8]+strDelim+currencyWithChosenLocalisation(Double.parseDouble( data[8]), locale)
            +strDelim+data[9]+strDelim+currencyWithChosenLocalisation(Double.parseDouble( data[9]), locale)
            +strDelim+data[10]+strDelim+currencyWithChosenLocalisation(Double.parseDouble( data[10]), locale)
            +strDelim+data[11]+strDelim+currencyWithChosenLocalisation(Double.parseDouble( data[11]), locale)
            +strDelim+data[12]+strDelim+currencyWithChosenLocalisation(Double.parseDouble( data[12]), locale)
            +strDelim+data[13]+strDelim+data[14]+strDelim+data[15]+strDelim+data[16]+strDelim
            +data[17]+strDelim+data[18]+strDelim;
		    
		    //System.out.println(dataRow);
		    //System.out.println(dataRow1.toString());
		    count++;
            if(count > 9999 && (count % 10000 == 0)) {
		    	System.out.println("Updated 10K records");
		    }
	     	update_status_PreProcess(clt.getClient_id(),clt.getProject_id(),
	            clt.getGenerate_id(),clt.getCustomer_id(),dataRow,host,db,clientName,username);
		}
            
		System.out.println(" Pre Process completed successfully.");
	}

	private static String currencyWithChosenLocalisation(double value, Locale locale) {
	    NumberFormat nf = NumberFormat.getCurrencyInstance(locale);
	    String valueFormated = nf.format(value);
	    valueFormated = valueFormated.replace(" ", "").replace(",", " ");
	    //valueFormated = valueFormated.replace(",", " ");
	    //System.out.println(valueFormated);
	    return valueFormated;
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

        	String strSQL = "{call sp_GetClientInfo_v2(?,?,?,?,?,?)}";
        	stmt = conn.prepareCall(strSQL);
        	stmt.setString(1, initialLetter);
        	stmt.setString(2, clientName);
        	stmt.setString(3, status);
        	stmt.setInt(4, client_id);
        	stmt.setInt(5, project_id);
        	stmt.setInt(6, generate_id);
        	
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
    	System.out.println("File for creating videos: " + filename);
        try (Connection conn = ConnectionPool.getConnection(host, db,username)){
            
            FileWriter fw = new FileWriter(filename);
            //FileWriter fw1 = new FileWriter(filename1);
            System.out.println("Fetching records......");
            List<Client> clients = getClient(initialLetter,host,db,clientName,username,"preprocess completed");
            System.out.println("Creating initial file for NPV......");
            //String header="MemberNumber,FirstName,firstnamefixed,YearsMember,StatementYear,YearOfJoining,ProfShareBal,ClosingBal,OperativeProfit,InvReturns,Country,Occupation,NumberOfProducts,AgeAsAtEndOfBonusYear,PPS_Sickness_and_Permanent_Incapacity_Benefit,Professional_Life_Provider,Critical_Illness_Cover,Professional_Disability_Provider,Accidental_Death_Benefit,Education_Cover,PPSi_Bonus_Total,MedAid_Bonus_Total,Advisor,Advisor_Email,curr1,curr2,curr3,curr4,curr5,curr6,curr7,Rec1,Rec2,Rec3,Rec4,Rec5,Rec6,Rec7,Rec8,RecCount";

            String header = "MemberNumber,FirstName,First_name_audio,Age_group,PPS_Life_Risk,"
            	+ "PPS_Investments,PPS_short_term_Insurance,PPS_Prof_Med,"
            	+ "ProfitShare_Amount,ProfitShare_Amount_Display,"
            	+ "Potential_ProfitShare_Amount,Potential_ProfitShare_Amount_Display,"
            	+ "2019_Closing_Bal,2019_Closing_Bal_Display,Operating_profit,Operating_profit_Display,"
            	+ "Investment_returns,Investment_returns_Display,"
            	+ "2020_prof_share_balance,2020_prof_share_balance_Display,Profitbooster_percent,"
            	+ "Profit_member,has_financial_advisor,financial_advisor,advisor_email,Medical_Health_Professional";
            
            /*String header="MemberNumber,FirstName,firstnamefixed,YearsMember,StatementYear,"
                    + "YearOfJoining,ProfShareAlloc,ProfShareBal,ClosingBal,OperativeProfit,InvReturns,"
                    + "Country,Occupation,NumberOfProducts,AgeAsAtEndOfBonusYear,"
                    + "PPS_Sickness_and_Permanent_Incapacity_Benefit,Professional_Life_Provider,Critical_Illness_Cover,Professional_Disability_Provider,"
                    + "Accidental_Death_Benefit,Education_Cover,PPSi_Bonus_Total,MedAid_Bonus_Total,Advisor,Advisor_Email,"
                    + "curr1,curr2,curr3,curr4,curr5,curr6,curr7,Rec1,Rec2,Rec3,Rec4,Rec5,Rec6,Rec7,Rec8,RecCount";
            */
            fw.append(header);
            fw.append('\n');
            int count = 0;
            for(Client clt : clients){
            	String allDataString = clt.getData_string();
            	  //System.out.println(allDataString);
            	fw.append(allDataString);
                fw.append('\n');
                update_statusCsvCreated_initial(clt.getClient_id(), clt.getProject_id(), 
                	clt.getGenerate_id(), clt.getCustomer_id(),host,db,clientName,username);
                count++;
                if(count > 9999 && (count % 10000 == 0)) {
    		    	System.out.println("Updated 10K records");
    		    }
            }
            System.out.println("Parsed CSV File with " + count + " records is created successfully. ");
          
            fw.flush();
            fw.close();
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
        String csvSplitBy = "\\,";
        //String firstName="";
        try {
        	System.out.println("Reading purl file......");
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
        		if (header[i].toUpperCase().equals("FIRSTNAME")){
        			customer_name=i;
        		}else if(header[i].toUpperCase().equals("MEMBERNUMBER")){
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
        	int count = 0;
        	String purlStringHeader = "MemberNumber,FirstName,Age_group,ProfitShare_Amount_Display,Potential_ProfitShare_Amount_Display,"
					  + "2019_Closing_Bal_Display,Operating_profit_Display,Investment_returns_Display,2020_prof_share_balance_Display,"
					  + "PPS_Life_Risk,PPS_Investments,PPS_short_term_Insurance,PPS_Prof_Med,"
					  + "Medical_Health_Professional,Profit_member,financial_advisor,advisor_email,video,thumbnail_1";
        	String[] purlHeaders = purlStringHeader.split(",");
        	while ((line = br.readLine()) != null) {

	        	String[] data = line.split(csvSplitBy);
	        	
	        	String customer_id_1=data[customer_id];
	        	//String customer_name_1=data[customer_name];
	        	String video_link_1=data[video_link];
	        	String thumbnail_s=data[thumbnail];
	        	String thumbnail_1_1=data[thumbnail_1];
	        	String video_file= video_link_1.substring(video_link_1.lastIndexOf("/")+1);
	        	
	        	//String purl_string= line;
	           
	        	String strDelim="|";
	        	String[] data1 = line.split(csvSplitBy);
	        	//String outLine="";
	        	StringBuilder outline = new StringBuilder();
	        	StringBuilder purl_string = new StringBuilder();
	        	//System.out.println(data1[data1.length-3]);
	        	//System.out.println(indx.get("MemberNumber"));
	        	//System.out.println(data1[indx.get("MemberNumber")]);
	        	/*
	        	First_name_audio,FirstName,MemberNumber,ProfitShare_Amount_Display,
	        	2019_Closing_Bal,2019_Closing_Bal_Display,Operating_profit_Display,
	        	Investment_returns_Display,2020_prof_share_balance_Display,PPS_Prof_Med,
	        	PPS_short_term_Insurance,PPS_Investments,PPS_Life_Risk,Age_group,
	        	Medical_Health_Professional,Profit_member,advisor_email,
	        	financial_advisor,recipient's_email_address_(optional),email_parameter(optional),
	        	video,thumbnail,thumbnail_1
				*/
	        	outline.append(data1[indx.get("MemberNumber")]+strDelim);
	        	outline.append(data1[indx.get("FirstName")]+strDelim);
	        	outline.append(data1[indx.get("First_name_audio")]+strDelim);
	        	outline.append(data1[indx.get("Age_group")]+strDelim);
	        	outline.append(data1[indx.get("2019_Closing_Bal")]+strDelim);
	        	outline.append(data1[indx.get("2019_Closing_Bal_Display")]+strDelim);
	        	outline.append(data1[indx.get("ProfitShare_Amount_Display")]+strDelim);
	        	outline.append(data1[indx.get("Potential_ProfitShare_Amount_Display")]+strDelim);
	        	outline.append(data1[indx.get("Operating_profit_Display")]+strDelim);
	        	outline.append(data1[indx.get("2020_prof_share_balance_Display")]+strDelim);
	        	outline.append(data1[indx.get("Investment_returns_Display")]+strDelim);
	        	outline.append(data1[indx.get("Potential_ProfitShare_Amount_Display")]+strDelim);
	        	outline.append(data1[indx.get("PPS_Life_Risk")]+strDelim);
	        	outline.append(data1[indx.get("PPS_Investments")]+strDelim);
	        	outline.append(data1[indx.get("PPS_short_term_Insurance")]+strDelim);
	        	outline.append(data1[indx.get("PPS_Prof_Med")]+strDelim);
	        	outline.append(data1[indx.get("Medical_Health_Professional")]+strDelim);
	        	outline.append(data1[indx.get("Profit_member")]+strDelim);
	        	outline.append(data1[indx.get("financial_advisor")]+strDelim);
	        	outline.append(data1[indx.get("advisor_email")]+strDelim);
	        	outline.append(data1[indx.get("video")]+strDelim);
	        	outline.append(data1[indx.get("thumbnail")]+strDelim);
	        	outline.append(data1[indx.get("thumbnail_1")]+strDelim);
	        	
	        	//purl_String
	        	
	        	for(String pHeader : purlHeaders) {
	        		//System.out.println(pHeader);
	        		purl_string.append(data1[indx.get(pHeader)]+strDelim);
	        	}
	        	/*
	        	purl_string.append(data1[indx.get("MemberNumber")]+strDelim);
	        	purl_string.append(data1[indx.get("FirstName")]+strDelim);
	        	purl_string.append(data1[indx.get("Age_group")]+strDelim);
	        	purl_string.append(data1[indx.get("ProfitShare_Amount_Display")]+strDelim);
	        	purl_string.append(data1[indx.get("Potential_ProfitShare_Amount_Display")]+strDelim);
	        	purl_string.append(data1[indx.get("2019_Closing_Bal_Display")]+strDelim);
	        	purl_string.append(data1[indx.get("Operating_profit_Display")]+strDelim);
	        	purl_string.append(data1[indx.get("Investment_returns_Display")]+strDelim);
	        	purl_string.append(data1[indx.get("2020_prof_share_balance_Display")]+strDelim);
	        	purl_string.append(data1[indx.get("Potential_ProfitShare_Amount_Display")]+strDelim);
	        	purl_string.append(data1[indx.get("PPS_Life_Risk")]+strDelim);
	        	purl_string.append(data1[indx.get("PPS_Investments")]+strDelim);
	        	purl_string.append(data1[indx.get("PPS_short_term_Insurance")]+strDelim);
	        	purl_string.append(data1[indx.get("PPS_Prof_Med")]+strDelim);
	        	purl_string.append(data1[indx.get("Medical_Health_Professional")]+strDelim);
	        	purl_string.append(data1[indx.get("Profit_member")]+strDelim);
	        	purl_string.append(data1[indx.get("financial_advisor")]+strDelim);
	        	purl_string.append(data1[indx.get("advisor_email")]+strDelim);
	        	purl_string.append(data1[indx.get("video")]+strDelim);
	        	purl_string.append(data1[indx.get("thumbnail_1")]+strDelim);
				*/
        		//System.out.println(outLine);
	        	video_file = video_file.replaceAll(".mp4","") + "_" + customer_id_1 +".mp4";
	        	updateClient_purlReceived(client_id,project_id,generate_id,customer_id_1,video_link_1,video_file,thumbnail_s,
			    	thumbnail_1_1, outline.toString(), purl_string.toString(), host,db,clientName,username);
        		count++;
        		if(count > 9999 && (count % 10000 == 0)) {
			    	System.out.println("Updated 10K records");
			    }
        	}
        	System.out.println(count + " - Purl records inserted successfully");
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
    
    private static void updateClient_purlReceived(int clientId, int projectId, int generateID, String customer_id, 
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
				//System.out.println("Record updated with Purl File Received status");
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
	    	int recordCnt = 0;
        	String header="Customer_id,Customer_name,Video,ThumbNail_link";
        	fw.append(header);
        	fw.append('\n');
          	for(Client clt : clients){
          		if(clt.getVideo_file()!=null){
				    fw.append(clt.getCustomer_id());
				    fw.append(',');
				    fw.append(clt.getCustomer_name());
				    fw.append(',');
				    fw.append("https://pps.nvidyo.co.za/n4mvp/v2/vplayer/videoJsAutoPlayNm.html?v=/videos/pps/prof_share_202104/"+
				    	clt.getVideo_file());
				    //fw.append("https://pps.nvidyo.co.za/videos/pps/ins_cc/"+clt.getVideo_file());
				    //fw.append("https://pps.nvidyo.co.za/videos/pps/prof_share/"+clt.getVideo_file()
				    //		+"_"+clt.getCustomer_id());
				    fw.append(',');
				    //fw.append("https://pps.nvidyo.co.za/thumb/pps/prof_share/"+clt.getVideo_file().replaceAll(".mp4",".jpg"));
				    String thumb=clt.getThumbnail_link1().substring(clt.getThumbnail_link1().lastIndexOf("/")+1);
				    fw.append("https://pps.nvidyo.co.za/thumb/pps/prof_share_202104/"+ thumb);
				    fw.append('\n');
				    update_status_FinalCsv_create(clt.getClient_id(), clt.getProject_id(), 
				    	clt.getGenerate_id(), clt.getCustomer_id(),host,db,clientName,username);
				    recordCnt++;
				    if(recordCnt > 9999 && (recordCnt % 10000 == 0)) {
				    	System.out.println("Updated 10K records");
				    }
          		}
          	}
            System.out.println(" Final Dispatched CSV with " + recordCnt + " records is created successfully.");
      	
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
				//System.out.println("Record updated with Final dispatched Csv status");
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

