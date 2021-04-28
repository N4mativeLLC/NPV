package com.n4mative.csvParser;

import java.text.NumberFormat;
import java.util.Locale;

public class TestPath {

	public static void main(String[] args) {
		
		int num1,num2,result;
		num1=30000;
		num2=10000;
		System.out.println("num1="+num1 + " num2="+num2);
		result=num1%num2;
		System.out.println("The result after modulus operation is : "+result);
		
		// TODO Auto-generated method stub
		/*String path="/membercommunication/en/2019/profitsharevideos.html?:redirect=/membercommunication/en/2019/commserror.html&:selfUrl=/content/forms/af/pps/2019/profitsharevideos/Profit-Share-Videos&first_name=fulu&last_name=mm&comments=&title=Mr&name=Billy&surname=Mmola&contactnumber=+27791379164&contactemail=fmmbadeni@pps.co.za&idorpassport=124405994&message=&v=/videos/pps/ins_cc/8c63fe1a6aea443ea3f5e27a740f10d4d4bc8ed9d214484da86f96d17a898b54.mp4&:cq_csrf_token=eyJleHAiOjE1NjUxNjcwMTEsImlhdCI6MTU2NTE2NjQxMX0.RvMuyhIYzw-m5XYUWOi8AGHjtGx8O0I66ajVBGAAcdE";
		//path="/membercommunication/en/2019/profitsharevideos.html?:redirect=/membercommunication/en/2019/commserror.html&:selfUrl=/content/forms/af/pps/2019/profitsharevideos/Profit-Share-Videos&first_name=fulu&last_name=mm&comments=&title=Mr&name=Billy&surname=Mmola&contactnumber=+27791379164&contactemail=fmmbadeni@pps.co.za&idorpassport=124405994&message=&:cq_csrf_token=eyJleHAiOjE1NjUxNjcwMTEsImlhdCI6MTU2NTE2NjQxMX0.RvMuyhIYzw-m5XYUWOi8AGHjtGx8O0I66ajVBGAAcdE";

		String videoPath=path;
		
		if (path.contains("&v=/")){
			videoPath=path.substring(path.indexOf("&v=/")+1, path.indexOf(".mp4", path.indexOf("&v=/"))+4);
		}else if (path.contains("?v=/")){
			videoPath=path.substring(path.indexOf("?v=/")+1, path.indexOf(".mp4", path.indexOf("?v=/"))+4);
		}
		System.out.println(videoPath);*/
		
		/*DateFormat df = new SimpleDateFormat("yyyyMMddHH");

	    Date dateobj = new Date();

	    String filterEndDate = df.format(dateobj);

	    String FilterHr =filterEndDate.substring(8,10);
	    	System.out.println(FilterHr);
	        String Filters = "ga:dateHour=~^%s([%s][%s])$";
	        System.out.println(filterEndDate.substring(0,8));
	        System.out.println(FilterHr.substring(0,1));
	        System.out.println(FilterHr.substring(1,2));
	        Filters = String.format(Filters, filterEndDate.substring(0,8), FilterHr.substring(0,1), FilterHr.substring(1,2));
	       	System.out.println(Filters);
	        
	      
	        */
		
		/*String customer_identity="aadamson@in-rel.com_1/28/20";
		int emailIndex= customer_identity.lastIndexOf("_");
		String email= customer_identity.substring(0,emailIndex);*/
		
		Locale locale = new Locale("en", "ZA");
		
		currencyWithChosenLocalisation(679.99, locale);
		currencyWithChosenLocalisation(8845.72, locale);
		currencyWithChosenLocalisation(20857.22, locale);
		currencyWithChosenLocalisation(317524.17, locale);
		//currencyWithChosenLocalisation(7791.08, locale);
		
		String s = "REV FR. PAUL"; //"0212362828758374099";

		 

		String modifiedFirstName =s.trim().replaceAll("\\.","_").replaceAll(":","_").replaceAll("-","_").replaceAll(" ","_");
		
		//modifiedFirstName =modifiedFirstNamereplace(". ","_").replace(": ","_").replace("-","_");

		System.out.println(modifiedFirstName);
		//System.out.println(email);
	        
	}
	
	public static String currencyWithChosenLocalisation(double value, Locale locale) {
	    NumberFormat nf = NumberFormat.getCurrencyInstance(locale);
	    String valueFormated = nf.format(value);
	    valueFormated = valueFormated.replace(" ", "").replace(",", " ");
	    //valueFormated = valueFormated.replace(",", " ");
	    System.out.println(valueFormated);
	    return valueFormated;
	}

}
