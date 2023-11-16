package de.pangaea.abcdharvester.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

public class FileIO {
	
	public static StringBuffer readFileForBuffer(String fileName) {
		StringBuffer buffer = new StringBuffer();

		File file = new File(fileName);

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		char[] buf = new char[1024];
		int numRead = 0;
		try {
			while ((numRead = reader.read(buf)) != -1) {
				String readData = String.valueOf(buf, 0, numRead);
				buffer.append(readData);

				buf = new char[1024];
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			reader.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		return buffer;

	}
	
	public static synchronized void writeFile( String data) {

		
		File f=new File("parserrors.txt");
		//File f=new File(Starter.responseFileLoc+"antares_"+formatter.format( new Date() )+".xml");
		
		Writer fstream = null;
		BufferedWriter out = null;
			
		try {
			fstream = new OutputStreamWriter(new FileOutputStream(f), StandardCharsets.UTF_8);
			out = new BufferedWriter(fstream);
			out.write(data);
			out.flush();
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		try {
//			FileWriter fWriter = new FileWriter(f);
//			fWriter.write(data);
//			fWriter.flush();
//			fWriter.close();
//		} catch (IOException e) {
//			System.out.println("Error writing json file: "+e.getMessage());
//		}
	}
	
}
