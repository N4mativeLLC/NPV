package com.n4mative.npv;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.GeneralSecurityException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.analytics.Analytics;
import com.google.api.services.analytics.Analytics.Data.Ga.Get;
import com.google.api.services.analytics.AnalyticsScopes;
import com.google.api.services.analytics.model.Accounts;
import com.google.api.services.analytics.model.GaData;
import com.google.api.services.analytics.model.GaData.ColumnHeaders;
import com.google.api.services.analytics.model.Profiles;
import com.google.api.services.analytics.model.Webproperties;

public class AnalytisReportCsv {
    
    private static final String APPLICATION_NAME = "GA Data Ingestion";
    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private static String KEY_FILE_LOCATION = "/Users/surbhi/Documents/workspace/nVidYoWebapp/src/main/java/org/nvidyo/webapp/npv/videoJSTest.json";
    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    private static String apiEmail = "videoreport@videojstest.iam.gserviceaccount.com";
    private static String jsonFilePath = "/Users/surbhi/Documents/workspace/nVidYoWebapp/src/main/resources/csvOutput/";
    private static String csvFilePath_folder="/Users/surbhi/Documents/workspace/nVidYoWebapp/src/main/resources/csvOutput/";
    private static String csvFilePath_all="/Users/surbhi/Documents/workspace/nVidYoWebapp/src/main/resources/csvOutput/";

    public static void main(String[] args) {
        
//        KEY_FILE_LOCATION = args[0];
//        apiEmail = args[1];
//        jsonFilePath = args[2];
//        csvFilePath = args[3];
    	String dimsMetric="";
    	String dimsDimension="";
    	String dimsDimension_folder="";
    	String dt = new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance().getTime());
    	if(args[0].equalsIgnoreCase("pageview")){
    		dimsMetric = "ga:pageviews";
            dimsDimension = "ga:date, ga:pagePath,ga:pageTitle";//,ga:browser,ga:country,ga:region,ga:city";
            dimsDimension_folder = "ga:pagePath,ga:pageTitle";
    	
    	}else if(args[0].equalsIgnoreCase("events")){
    		 dimsMetric = "ga:totalEvents,ga:eventValue,ga:sessionsWithEvent";
             dimsDimension ="ga:eventCategory,ga:eventAction,ga:eventLabel";
    	}
    	
    	String startDate = "today";//args[6];
        String endDate = "today";//args[7];
        
        try {
        	Analytics analytics = initializeAnalytic();
            String profile = getFirstProfileId(analytics);
            System.out.println("First Profile Id is: " + profile);
            
            GaData result = getResults(analytics, profile, dimsMetric, dimsDimension, startDate, endDate);
            GaData result_folder = getResults(analytics, profile, dimsMetric, dimsDimension_folder, startDate, endDate);
            
            if(!args[0].equalsIgnoreCase("events")){
            	///generateJsonFile(result, jsonFilePath);
                //generateCsvFile_folder(result_folder, csvFilePath_folder);
            	generateCsvFile_folder(result, csvFilePath_folder,dt);
                ///generateCsvFile_all(result, csvFilePath_all);	
            }else{
            	generateJsonFile(result, jsonFilePath,dt);
            	generateCsvFile_all(result, csvFilePath_all,dt);
            }
            
            // System.out.println(json.getString("rows"));
            
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    
    // Get Data
    private static GaData getResults(Analytics analytics, String profileId, 
            String dimsMetric, String dimsDimension, 
            String startDate, String endDate) throws IOException {
        // Query the Core Reporting API for the number of sessions
        // String dimsDate = ",ga:date, ga:hour";       
        
        Get get = analytics.data().ga().get("ga:" + profileId, startDate, endDate, dimsMetric);
        get.setDimensions(dimsDimension);
        get.setMaxResults(10000);
        
        return get.execute();
      }
    
    private static String generateHeaderString(GaData results) {
        
        String columns = "";
        List<ColumnHeaders> header = results.getColumnHeaders();
        Iterator<ColumnHeaders> iter = header.iterator();
        while (iter.hasNext()) {
            columns += iter.next().getName().replace("ga:",  "") + ",";
        }
        return columns.substring(0, columns.length() - 1);
    }

    private static void generateCsvFile_folder(GaData results, String csvFilePath, String dt) {
        
        try {
            // (1) Prepare files
            FileWriter fWriter = new FileWriter(csvFilePath+"N4MAnalyticsReport_folder"+dt+".csv");
            BufferedWriter bWriter = new BufferedWriter(fWriter);
            PrintWriter out = new PrintWriter(bWriter);
            
            // (2) Write header
            out.println(generateHeaderString(results));
            
            // (3) Get data from GaData object
            List<List<String>> rows = results.getRows();
            int pageView=0;
            String row="";
            int uniquePage=0;
            Map<String,Integer> map=new HashMap<String,Integer>();
            List<String> record=null;
            
            for(int i=0;i<rows.size();i++){
            	//row ="";
                String url= "";
                record=rows.get(i);
            	url = record.get(1).toString();
            	String folder = url.split("\\?")[1].split("/")[3];
        		System.out.println(folder);
        		
        		pageView = Integer.parseInt(record.get(3));
        		if(map.containsKey(folder)){
        			int count = map.get(folder);
        			pageView = pageView + count;
        			
        		}
        		map.put(folder,pageView);
        	}
            for ( String key : map.keySet() ) {
            	row="";
                //System.out.println( key );
                row += key +","+ record.get(1)+ ","+ map.get(key);
                out.println(row);
            }out.close();
            System.out.println("CSV File has been created.");
            
        } catch (IOException e) {
            System.out.println("Problem with csv file IO operation: " + e.toString());
        }
    }

       private static void generateCsvFile_all(GaData results, String csvFilePath, String dt) {
        
        try {
            // (1) Prepare files
            FileWriter fWriter = new FileWriter(csvFilePath+"N4MAnalyticsReport_all"+dt+".csv");
            BufferedWriter bWriter = new BufferedWriter(fWriter);
            PrintWriter out = new PrintWriter(bWriter);
            
            // (2) Write header
            out.println(generateHeaderString(results));
            
            // (3) Get data from GaData object
            List<List<String>> rows = results.getRows();
            Iterator<List<String>> iter = rows.iterator();
            
            for(int i=0;i<rows.size();i++){
            	String row ="";
                List<String> record=rows.get(i);
            	for(int j=0;j< record.size();j++){
            		row += record.get(j)+",";
            	}
            	out.println(row.substring(0, row.length() - 1));
            	//System.out.println(row.substring(0, row.length() - 1));
            }
           
            // (4) Close the file
            out.close();
            System.out.println("CSV File has been created.");
            
        } catch (IOException e) {
            System.out.println("Problem with csv file IO operation: " + e.toString());
        }
    }

    private static void generateJsonFile(GaData results, String filePath, String dt) throws JSONException {
        try {
            // (1) Prepare files
            FileWriter fWriter = new FileWriter(filePath+"N4MAnalyticsReport"+dt+".json");
            BufferedWriter bWriter = new BufferedWriter(fWriter);
            PrintWriter out = new PrintWriter(bWriter);
            // (2) Create JSON Object and write into the file with indentation
            JSONObject json = new JSONObject(results);
            out.println(json.toString(4));
            // (3) Close the file
            out.close();
            System.out.println("JSON File has been created.");
        } catch (IOException e) {
            System.out.println("Problem with JSON file IO operation: " + e.toString());
        }
    }
    
    // AUTHENTICATION 
    
    /**
     * Initializes an Analytics service object.
     *
     * @return An authorized Analytics service object.
     * @throws IOException
     * @throws GeneralSecurityException
     */
    private static Analytics initializeAnalytic() throws GeneralSecurityException, IOException {
        
        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        /*GoogleCredential credential = new GoogleCredential.Builder()
                .setTransport(HTTP_TRANSPORT)
                .setJsonFactory(JSON_FACTORY)
                .setServiceAccountId(apiEmail)
                .setServiceAccountScopes(Arrays.asList(AnalyticsScopes.ANALYTICS_READONLY))
                .setServiceAccountPrivateKeyFromP12File(new File(KEY_FILE_LOCATION)).build();*/
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
              .list(firstAccountId).execute();

          if (properties.getItems().isEmpty()) {
            System.err.println("No Webproperties found");
          } else {
            String firstWebpropertyId = properties.getItems().get(0).getId();

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
}