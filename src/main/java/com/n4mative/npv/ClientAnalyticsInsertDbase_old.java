package com.n4mative.npv;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.analytics.Analytics;
import com.google.api.services.analytics.AnalyticsScopes;
import com.google.api.services.analytics.Analytics.Data.Ga.Get;
import com.google.api.services.analytics.model.Accounts;
import com.google.api.services.analytics.model.GaData;
import com.google.api.services.analytics.model.Profiles;
import com.google.api.services.analytics.model.Webproperties;
import com.n4mative.database.ConnectionPool;

public class ClientAnalyticsInsertDbase_old {
	static String host = "192.168.1.71";
    static String db = "NPV_QA";
    static String username="liamapp";
    static String campaign="test_prod";//"Professional Banking Package";//profit Share
    static String client="N4Mative";//"nedbank";//pps
    static String viewId="221978758";
	public static void main(String[] args) {
		
		final String APPLICATION_NAME = "GA Data Ingestion";
	    final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
	    String KEY_FILE_LOCATION = args[2];//"/Users/surbhi/git/NPV/src/main/java/com/n4mative/npv/videoJSTest.json";
	    String dimsMetric = "ga:pageviews,ga:avgTimeOnPage";
        String dimsDimension = "ga:dateHourMinute, ga:pagePath,ga:country,ga:region,ga:city,ga:browser,ga:operatingSystem";
        String dimsMetric_event = "ga:totalEvents";
        String dimsDimension_event ="ga:dateHourMinute,ga:eventAction,ga:eventLabel,ga:country,ga:city,ga:browser,ga:operatingSystem";
        //String dimsDimension_event ="ga:dateHourMinute,ga:eventAction,ga:pagePath,ga:country,ga:city,ga:browser,ga:operatingSystem";

        String startDate = args[0];
        String endDate = args[1];
        if (endDate.equalsIgnoreCase("today")){
        	DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        	Date dateobj = new Date();
        	endDate = df.format(dateobj);
        }
        if (args.length >3 && args.length <=7){
        	System.out.println("All required parameters missing");
        	System.exit(1);
        }
        if (args.length > 3){
        	host=args[3];
            db=args[4];	
            username=args[5];
            campaign= args[6];
            client= args[7];
            viewId= args[8];
        }
       
        //List<Campaign> campaignLst = new ArrayList<>();
        List<Campaign> eventLst = new ArrayList<>();
        try {
        	
        	Analytics analytics = initializeAnalytic(KEY_FILE_LOCATION,JSON_FACTORY, APPLICATION_NAME );
            //String profile = getFirstProfileId(analytics);
            //System.out.println("First Profile Id is: " + profile);
            
            //deleteEventAnalytics(startDate,endDate,client,campaign);
            
            //GaData result = getResults(analytics, profile, dimsMetric, dimsDimension, startDate, endDate,1);
            GaData result_event = getResults(analytics, viewId, dimsMetric_event, dimsDimension_event, startDate, endDate,1);
            
            System.out.println(result_event.getTotalResults());
           
            eventLst= geneateEvent(result_event);
            insertEventAnalytics(eventLst);
            int result_eventIndex=result_event.getTotalResults() / 10000;
            if (result_eventIndex > 0){
            	for (int i=1;i<=result_eventIndex;++i){
            		result_event.clear();
            		result_event = getResults(analytics, viewId, dimsMetric_event, dimsDimension_event, startDate, endDate,(i*10000)+1);
            		eventLst.clear();
            		eventLst= geneateEvent(result_event);
                    insertEventAnalytics(eventLst);
            	}
            }
            
            
        } catch (Exception e) {
            e.printStackTrace();
        }
       
	}
	
	private static void deleteEventAnalytics(String startDate, String endDate, String clientName, String campaignName) {
		CallableStatement stmt = null;
		String strSQL = "{call sp_deleteEventsAnalyticsData_client(?,?,?,?)}";
		
		try (Connection conn = ConnectionPool.getConnection(host, db,username)) {
				stmt = conn.prepareCall(strSQL);
		        stmt.setString(1, startDate);
		        stmt.setString(2, endDate);
		        stmt.setString(3, clientName);
		        stmt.setString(4, campaignName);
				// execute the preparedstatement insert
				stmt.executeQuery();
			    //st.close();
				
			    //return true;
				System.out.println(" Events Record deleted");
			}catch (Exception e)
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


	private static GaData getResults(Analytics analytics, String profileId, 
            String dimsMetric, String dimsDimension, 
            String startDate, String endDate, int startIndex) throws IOException {
        // Query the Core Reporting API for the number of sessions
        // String dimsDate = ",ga:date, ga:hour";       
        
        Get get = analytics.data().ga().get("ga:" + profileId, startDate, endDate, dimsMetric);
        get.setDimensions(dimsDimension);
        get.setMaxResults(50000);
        get.setStartIndex(startIndex);
        return get.execute();
      }
	
	
	private static Analytics initializeAnalytic(String KEY_FILE_LOCATION, JsonFactory JSON_FACTORY, String APPLICATION_NAME) throws GeneralSecurityException, IOException {
        
        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
       
         //Using JSON file for secret key
        GoogleCredential credential = GoogleCredential.fromStream(new FileInputStream(KEY_FILE_LOCATION))
                                                        .createScoped(AnalyticsScopes.all());
        
        
        return new Analytics.Builder(httpTransport, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME).build();
    }
	
	private static String getFirstProfileId(Analytics analytics) throws IOException {
        // Get the first view (profile) ID for the authorized user.
        String profileId = null;

        // Query for the list of all accounts associated with the service account.
        Accounts accounts = analytics.management().accounts().list().execute();

        if (accounts.getItems().isEmpty()) {
          System.err.println("No accounts found");
        } else {
          String firstAccountId = accounts.getItems().get(0).getId();

          // Query for the list of properties associated with the first account.
          Webproperties properties = analytics.management().webproperties()
              .list("~all").execute();

          if (properties.getItems().isEmpty()) {
            System.err.println("No Webproperties found");
          } else {
            String firstWebpropertyId = properties.getItems().get(3).getId();
            System.out.println(firstWebpropertyId);

            // Query for the list views (profiles) associated with the property.
            Profiles profiles = analytics.management().profiles()
                .list(firstAccountId, firstWebpropertyId).execute();

            if (profiles.getItems().isEmpty()) {
              System.err.println("No views (profiles) found");
            } else {
              // Return the first (view) profile associated with the property.
              profileId = profiles.getItems().get(0).getId();
            }
          }
        }
        return profileId;
      }

	
	
	private static String getDate(String date) {
		String formattedDate = "";
		formattedDate = date.substring(0,4) + "-" + date.substring(4,6) + "-" + date.substring(6,8) + " " + date.substring(8,10) + ":" + date.substring(10,12) + ":00";
		//YYYYMMDDHHMM.
		System.out.println(formattedDate);
		return formattedDate;
	}
	
	
	private static List<Campaign> geneateEvent(GaData result_event) {
		 // (3) Get data from GaData object
		List<List<String>> rows = result_event.getRows();
		List<Campaign> eventLst= new ArrayList<Campaign>();
		List<String> record=null;
		
		if(rows!=null){
			for(int i=0;i<rows.size();i++){
				
				Campaign event = new Campaign();
			    record=rows.get(i);
			    
				event.setEventDate(record.get(0).substring(0, 8));
				event.setEventDateTime(getDate(record.get(0)));
			    event.setEventCampaign(campaign);
			    event.setEventClient(client);
				event.setEventAction(record.get(1));  
				event.setEventPercentRank(getPercentrank(record.get(1)));
	    		//event.setEventlabel(getVideo(record.get(2)));
	    		event.setEventlabel(record.get(2));
	    		event.setEventCountry(record.get(3));
	    		event.setEventCity(record.get(4));
	    		event.setEventBrowser(record.get(5));
	    		event.setEventOperatingSystem(record.get(6));
	    		event.setTotalEvents(Integer.parseInt(record.get(7)));
	    		eventLst.add(event);
	
			}
		}else{
			System.out.println("No data found for these dates");
		}
		return eventLst;
	}
	
	private static int getPercentrank(String eventAction) {
		if (eventAction.equals("< 25 percent played")){
			return 1;
		}else if(eventAction.equals("25-50 percent played")) {
			return 2;
		}
		else if(eventAction.equals("50-75 percent played")) {
			return 3;
		}
		else if(eventAction.equals("75-99 percent played")) {
			return 4;
		}
		else if(eventAction.equals("100 percent played")) {
			return 5;
		}
		return 0;
		
	}
	private static String getVideo(String videoUrl) {
		String video="";
		
		video=videoUrl.substring(videoUrl.lastIndexOf("/")+1, videoUrl.lastIndexOf("."));
		
		/*if(videoUrl.contains(".mp4")){
	    	
		    if (videoUrl.contains("&v=/")){
				video =videoUrl.substring(videoUrl.indexOf("&v=/")+1, videoUrl.indexOf(".mp4", videoUrl.indexOf("&v=/"))+4);
				
			}else if (videoUrl.contains("?v=/")){
				System.out.println(videoUrl.lastIndexOf("/")+1);
				System.out.println(videoUrl.indexOf("."));
				video=videoUrl.substring(videoUrl.lastIndexOf("/")+1, videoUrl.lastIndexOf("."));
				
			}
	    }*/
		return video;
		
	}
	
	private static void insertEventAnalytics(List<Campaign> eventLst) {
		
		CallableStatement stmt = null;
		String strSQL = "{call sp_InsertEventAnalytics_client_v2(?,?,?,?,?,?,?,?,?,?,?,?)}";
		
		try (Connection conn = ConnectionPool.getConnection(host, db, username)) {
			for (Campaign evt : eventLst) {	
				stmt = conn.prepareCall(strSQL);
				// set all the preparedstatement parameters
				
				stmt.setString(1, evt.getEventDate());
				stmt.setTimestamp(2, Timestamp.valueOf(evt.getEventDateTime()));
				stmt.setString(3, evt.getEventClient());
				stmt.setString(4, evt.getEventCampaign());
				stmt.setString(5, evt.getEventAction());
				stmt.setInt(6, evt.getEventPercentRank());
				stmt.setString(7, evt.getEventlabel());
				stmt.setString(8, evt.getEventCountry());
				stmt.setString(9, evt.getEventCity());
				stmt.setString(10, evt.getEventBrowser());
				stmt.setString(11, evt.getEventOperatingSystem());
				stmt.setInt(12, evt.getTotalEvents());
				
				// execute the preparedstatement insert
				stmt.executeQuery();
			    //st.close();
				
			    //return true;
			}
			System.out.println("Events Record Inserted");
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
