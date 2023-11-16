package de.pangaea.abcdharvester.util;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.log4j.Logger;

import de.pangaea.abcdharvester.es.ESConnector;

public class DateUtility {
	private static Logger logger; 
	
	public static void main(String[] args){
		//convertABCDWeirdoDates("2013-06-14");
		
		isDateTimeValid("2022-04-25T14:43:00");  //:
	}
	

	public static String isDateTimeValid(String in) {
		logger = Logger.getLogger(DateUtility.class.getName());
		String retString;
		DateTimeFormatter dateFormatter = DateTimeFormatter
			    .ofPattern("yyyy-MM-dd'T'HH:mm:ss[xxx][xx][X]", Locale.GERMAN).withResolverStyle(ResolverStyle.LENIENT);
	    try {
            //dateFormatter.parse("May / June 2019");
	    	TemporalAccessor ta = dateFormatter.parse(in);
	    	retString = in;
        } catch (DateTimeParseException e) {
        	logger .error("Error parsing date: " + in + ", not setting timestamp.");
           retString = "";
        }
		return retString;
	}
	
	//method not used anymore --> use isDateTimeValid() instead
	public static String convertABCDWeirdoDates(String in) throws NumberFormatException{
		String ret = "";
		int year = 0;
		int month = 0;
		int date = 0;
		boolean bFormat = true;
	//  possible incoming dates:
//		1986-08-15
//		08.10.2013
//		7.1952
//		1968
		String[] tarr = null;
		if(in.contains("T")) {
			tarr = in.split("T");
			in = tarr[0];
		}
		
		String[] arr = null;
		if(in.contains(".")) {
			arr = in.split("\\.");
		}else if(in.contains("-")) {
			arr = in.split("-");
		}else {
			//year only
			ret = in;
			bFormat = false;
		}
		if(bFormat) {
			if(arr != null && arr.length > 2) {
				//1986-08-15
				if(arr[0].length() == 4) {
					date = Integer.parseInt(arr[2]);
					month = Integer.parseInt(arr[1]);
					year = Integer.parseInt(arr[0]);	
				}
				
				//08-10-2013
				if(arr[2].length() == 4) {
					date = Integer.parseInt(arr[0]);
					month = Integer.parseInt(arr[1]);
					year = Integer.parseInt(arr[2]);			
				}
			}else if (arr.length == 2){
				//7.1952
				if(arr[1].length() == 4) {
					date = 1;
					month = Integer.parseInt(arr[0]);
					year = Integer.parseInt(arr[1]);	
				}
			}	
		
		
		
		Calendar calCurrentDate = Calendar.getInstance(TimeZone.getTimeZone("Europe/Berlin"));
		calCurrentDate.set(year, month-1, date, 0, 0, 0);
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		formatter.setTimeZone(TimeZone.getTimeZone("Europe/Berlin"));
		ret = formatter.format(calCurrentDate.getTime());
		
		}
			
		return ret;
	}
}
