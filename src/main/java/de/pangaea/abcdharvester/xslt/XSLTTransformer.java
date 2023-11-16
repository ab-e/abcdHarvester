package de.pangaea.abcdharvester.xslt;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

public class XSLTTransformer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Source xsl = new StreamSource(new File("abcd2pansimple.xslt"));
		String result = "";
		
		/**
		 * 
		 * 
		 */
		Source xmlInput = new StreamSource(new File("response.xml"));
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		Result stringOutput = new StreamResult(bos);

		try {
		    Transformer transformer = TransformerFactory.newInstance().newTransformer(xsl);
		    //transformer.setParameter("corePath", corePath);
		    
		    transformer.transform(xmlInput, stringOutput);
		} catch (TransformerException e) {
		}
		
		//String xsltResult = bos.toString();
		BufferedReader br = new BufferedReader(new StringReader(bos.toString()));
		String line;
		try {
			while((line = br.readLine())!= null){
				//System.out.println(line);
				if(line.length() > 0){
					result = result += line+System.lineSeparator();
				}
			}
		} catch (IOException e1) {
		}
		System.out.println(result);
	}

}
