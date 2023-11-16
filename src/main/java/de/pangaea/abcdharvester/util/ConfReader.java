package de.pangaea.abcdharvester.util;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;

public class ConfReader {

	
	public static Properties readProps(String filename){
		Properties properties = new Properties();
		 
		try {
			InputStream fstream = new FileInputStream(filename);
			properties.load(fstream);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//logger.debug("Error reading configuration file: "+e.getMessage());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		
		return properties;
		
	}
}
