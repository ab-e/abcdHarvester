package de.pangaea.abcdharvester.xml;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class ABCDDOMParser {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		parseABCD();
	}
	
	public static void parseABCD() {
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			 
			//build document
			Document document = builder.parse(new File("response.00001.xml"));
			 
			//normalize
			document.getDocumentElement().normalize();
			 
			//root node
			Element root = document.getDocumentElement();
			System.out.println(root.getNodeName());
			
			//get metadata to use in all units
			NodeList ntList =document.getElementsByTagName("abcd:Title");
			//System.out.println(ntList.getNodeValue());
			Node node = ntList.item(0);
			System.out.println(node.getTextContent());
			
			//description
			//NodeList descList =document.getElementsByTagName("abcd:DataSets/abcd:DataSet/abcd:Metadata/abcd:Description/abcd:Representation/abcd:Details");
			NodeList descList =document.getElementsByTagName("abcd:Details");
			Node descNode = descList.item(0);
			System.out.println(descNode.getTextContent());
			
			//owners
		//if(document.getLocalName() != null) {
			//	if(document.getLocalName().contains("abcd:Owners")) {
					NodeList ownerList =document.getElementsByTagName("abcd:Owners");
					Node ownerNode = ownerList.item(0);
					NodeList clist = ownerNode.getChildNodes();
					for(int i = 0; i < clist.getLength(); i++) {
						System.out.println(clist.item(i).getTextContent());
					}
					
				//}
			//}
			
			//get all units
			NodeList nList = document.getElementsByTagName("abcd:Unit");
			System.out.println(nList.getLength());
			
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
