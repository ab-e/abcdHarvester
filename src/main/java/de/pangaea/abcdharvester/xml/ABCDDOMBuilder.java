package de.pangaea.abcdharvester.xml;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.Logger;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import de.pangaea.abcdharvester.es.ESInputObject;
import de.pangaea.abcdharvester.es.ESMetadataInputObject;
import de.pangaea.abcdharvester.es.ESConnector;

public class ABCDDOMBuilder {

	private static Logger logger; 
	private static ESConnector esConnector= new ESConnector();
	private Writer sw;
	private boolean bPushed;
	private boolean bDatasetPushed;
	private Transformer transformer;
	private DOMSource domSource;
	private int l;
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//generateXMLTree("","","","","","","","");
	}
//	public static void generateXMLTree(String scientificName, String unitID, String recordURI, String titl, String citation, String owner, String recordBasis,
//										String higherTaxonRank){
	
	public ABCDDOMBuilder() {
		logger = Logger.getLogger(ABCDDOMBuilder.class.getName());
		
		try {
			transformer = TransformerFactory.newInstance().newTransformer();
		} catch (TransformerConfigurationException | TransformerFactoryConfigurationError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// formatting properties
		transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
		//transformer.setOutputProperty(OutputKeys.ENCODING, "utf8");
	}
	
	public boolean generateDatasetXMLTree(ESMetadataInputObject esMDObject){
		String contact = "";
		bPushed = false;
		try {
			Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
			//dataset metadata
			Element dataset = doc.createElement("dataset");
			dataset.setAttribute("xmlns", "urn:pangaea.de:dataportals");
			dataset.setAttribute("xmlns:dc", "http://purl.org/dc/elements/1.1/");
			dataset.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
			dataset.setAttribute("xsi:schemaLocation", "urn:pangaea.de:dataportals http://ws.pangaea.de/schemas/pansimple/pansimple.xsd");
			doc.appendChild(dataset);
			
			Element title = doc.createElement("dc:title");
			if(esMDObject.getDsContactList().size() > 0) {
				for(int i = 0; i < esMDObject.getDsContactList().size(); i++) {
					contact = contact.concat(esMDObject.getDsContactList().get(i).concat(System.lineSeparator()));
				}
			}
			
			title.setTextContent(esMDObject.getDsTitle().concat(System.lineSeparator().concat(contact)));
			dataset.appendChild(title);
			
			Element description = doc.createElement("dc:description");
			description.setTextContent(esMDObject.getDsDescription());
			dataset.appendChild(description);
			
			if(esMDObject.getDsContribList().size() > 0) {
				for(int i = 0; i < esMDObject.getDsContribList().size(); i++) {
					Element contributor = doc.createElement("dc:contributor");
					contributor.setTextContent(esMDObject.getDsContribList().get(i));
					dataset.appendChild(contributor);
				}
			}
			
			Element publisher = doc.createElement("dc:publisher");
			publisher.setTextContent(esMDObject.getPublisher());
			dataset.appendChild(publisher);
			
			Element dataCenter = doc.createElement("dataCenter");
			dataCenter.setTextContent(esMDObject.getDataCenter());
			dataset.appendChild(dataCenter);
			
			Element type = doc.createElement("dc:type");
			type.setTextContent(esMDObject.getDsType());
			dataset.appendChild(type);
			
			Element format = doc.createElement("dc:format");
			format.setTextContent(esMDObject.getDsFormat());
			dataset.appendChild(format);
			
			if(esMDObject.getDsLinkageList().size() > 0) {
				for(int i = 0; i < esMDObject.getDsLinkageList().size(); i++) {
					Element linkage = doc.createElement("linkage");
					
					l = esMDObject.getDsLinkageList().get(i).lastIndexOf("#");
					linkage.setAttribute("type", esMDObject.getDsLinkageList().get(i).substring(l+1));
					linkage.setTextContent( esMDObject.getDsLinkageList().get(i).substring(0, l));
					dataset.appendChild(linkage);
				}
			}
			
			Element identifier = doc.createElement("dc:identifier");
			identifier.setTextContent(esMDObject.getDsIdentifier());
			dataset.appendChild(identifier);
			
			Element source = doc.createElement("dc:source");
			source.setTextContent(esMDObject.getDsSource());
			dataset.appendChild(source);
			
			if(esMDObject.getDsRightsList().size() > 0) {
				for(int i = 0; i < esMDObject.getDsRightsList().size(); i++) {
					Element rights = doc.createElement("dc:rights");
					rights.setTextContent(esMDObject.getDsRightsList().get(i));
					dataset.appendChild(rights);
				}
			}
			
//			if(!esMDObject.getDsRelation().isBlank()) {
//				Element relation = doc.createElement("dc:relation");
//				relation.setTextContent(esMDObject.getDsRelation());
//				dataset.appendChild(relation);
//			}
			
			Element date = doc.createElement("dc:date");
			date.setTextContent(esMDObject.getDsDate());
			dataset.appendChild(date);
			
			
			domSource = new DOMSource(doc);
			sw = new StringWriter();
			StreamResult streamResult = new StreamResult(sw); 
			transformer.transform(domSource, streamResult);
			//System.out.println(sw.toString());
			
			if(!ABCDStaxParser.isbTestrun()) {
				bPushed = esConnector.putES(sw, null);
			}
			else {
				logger.debug(sw.toString());
				bPushed = true;
			}
			
			if(bPushed) {
				logger.info("Dataset metadata pushed, proceeding with units...");
			}
		} 
		catch (ParserConfigurationException | TransformerException | FileNotFoundException | URISyntaxException | InterruptedException | ExecutionException  e) {
			logger.error("Error building dataset xml: " + e.getMessage());
		}
//		catch (ParserConfigurationException | TransformerException  e) {
//			logger.error("Error building dataset xml: " + e.getMessage());
//		}
		return bPushed;
	}
	
	public boolean generateXMLTree(ESInputObject esObject){
								
			try {
				Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
//			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
//			  factory.setValidating(true);
//			  DocumentBuilder builder = factory.newDocumentBuilder();
//			  Document doc = builder.newDocument();
//			  
				
				//dataset metadata
				Element dataset = doc.createElement("dataset");
				dataset.setAttribute("xmlns", "urn:pangaea.de:dataportals");
				dataset.setAttribute("xmlns:dc", "http://purl.org/dc/elements/1.1/");
				dataset.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
				dataset.setAttribute("xsi:schemaLocation", "urn:pangaea.de:dataportals http://ws.pangaea.de/schemas/pansimple/pansimple.xsd");
				doc.appendChild(dataset);
 
				
				Element title = doc.createElement("dc:title");
				//title.setTextContent(scientificName + ", a "+ recordBasis +" of the \""+ titl +"\" dataset [ID: "+ unitID +"]");
				//title.setTextContent(esObject.getScientificName() +" [ID: " + esObject.getIdentifier() + "], " + esObject.getTitle());
				title.setTextContent(esObject.getTitle());
				dataset.appendChild(title);
 
				//description
				Element description = doc.createElement("dc:description");
				description.setTextContent(esObject.getDescription());
				dataset.appendChild(description);
				
				//contributor - dataset level
				for(int i = 0; i < esObject.getContrListDataset().size(); i++) {
					Element contributor = doc.createElement("dc:contributor");
					contributor.setTextContent(esObject.getContrListDataset().get(i));
					dataset.appendChild(contributor);
				}
				//contributor - unit level
				for(int i = 0; i < esObject.getContrListUnit().size(); i++) {
					Element contributor = doc.createElement("dc:contributor");
					contributor.setTextContent(esObject.getContrListUnit().get(i));
					dataset.appendChild(contributor);
				}
				
				//publisher
				Element publisher = doc.createElement("dc:publisher");
				publisher.setTextContent(esObject.getPublisher());
				dataset.appendChild(publisher);
				
				//dataCenter
				Element datacenter = doc.createElement("dataCenter");
				datacenter.setTextContent(esObject.getDataCenter());
				dataset.appendChild(datacenter);
				
				//type
				for(int i = 0; i < esObject.getType().size(); i++) {
					Element type = doc.createElement("dc:type");
					type.setTextContent(esObject.getType().get(i));
					dataset.appendChild(type);
				}
				
				//format
				Element format = doc.createElement("dc:format");
				format.setTextContent(esObject.getFormat());
				dataset.appendChild(format);
				
				//identifier
				Element identifier = doc.createElement("dc:identifier");
				identifier.setTextContent(esObject.getIdentifier());
				dataset.appendChild(identifier);
				
				//source			
				Element source = doc.createElement("dc:source");
				source.setTextContent(esObject.getSource());
				dataset.appendChild(source);
				
				//linkage
				
				for(int i = 0; i < esObject.getLinkageList().size(); i++) {
					Element linkage = doc.createElement("linkage");
					l = esObject.getLinkageList().get(i).lastIndexOf("#");
					//linkage.setAttribute("type", esObject.getLinkageList().get(i).split("#")[1]);
					//linkage.setTextContent(esObject.getLinkageList().get(i).split("#")[0]);
					//System.out.println(esObject.getLinkageList().get(i).substring(l+1));
					//System.out.println(esObject.getLinkageList().get(i).substring(0, l));
					linkage.setAttribute("type", esObject.getLinkageList().get(i).substring(l+1));
					linkage.setTextContent( esObject.getLinkageList().get(i).substring(0, l));
					dataset.appendChild(linkage);
				}
				
				
				//coverage
				Element coverage = doc.createElement("dc:coverage");
				coverage.setAttribute("xsi:type", "CoverageType");
				Element nbl = doc.createElement("northBoundLatitude");
				nbl.setTextContent(esObject.getLatitude());
				coverage.appendChild(nbl);
				Element wbl = doc.createElement("westBoundLongitude");
				wbl.setTextContent(esObject.getLongitude());
				coverage.appendChild(wbl);
				Element sbl = doc.createElement("southBoundLatitude");
				sbl.setTextContent(esObject.getLatitude());
				coverage.appendChild(sbl);
				Element ebl = doc.createElement("eastBoundLongitude");
				ebl.setTextContent(esObject.getLongitude());
				coverage.appendChild(ebl);
				
				Element startDate = doc.createElement("startDate");
				startDate.setTextContent(esObject.getStatDate());
				Element endDate = doc.createElement("endDate");
				endDate.setTextContent(esObject.getEndDate());
				coverage.appendChild(startDate);
				coverage.appendChild(endDate);
				
				
				dataset.appendChild(coverage);
				
				//subject
				for(int i = 0; i < esObject.getSubjectList().size(); i++) {
					Element subject = doc.createElement("dc:subject");
					subject.setAttribute("type", esObject.getSubjectList().get(i).split("#")[1]);
					subject.setAttribute("xsi:type", "SubjectType");
					subject.setTextContent(esObject.getSubjectList().get(i).split("#")[0]);
					dataset.appendChild(subject);
				}
				
				
				
				//rights (license) on dataset level
				if(esObject.getLicense().length() > 0) {
					Element rights = doc.createElement("dc:rights");
					rights.setTextContent(esObject.getLicense());
					dataset.appendChild(rights);
				}
				//rights (license) on unit level
				for(int i = 0; i < esObject.getLicenseList().size(); i++) {
					Element rightsU = doc.createElement("dc:rights");
					rightsU.setTextContent(esObject.getLicenseList().get(i));
					dataset.appendChild(rightsU);
				}
				
				/**
				//relation from description/URI
				if(esObject.getRelationURI().length() > 0) {
					Element relation = doc.createElement("dc:relation");
					relation.setTextContent(esObject.getRelationURI());
					dataset.appendChild(relation);
				}
				//relation from Unit/AssociatedUnitID
				if(esObject.getRelationUnitID().length() > 0) {
					Element relation = doc.createElement("dc:relation");
					relation.setTextContent(esObject.getRelationUnitID());
					dataset.appendChild(relation);
				}
				**/
				
				//parentIdentifier
				Element parentIdentifier = doc.createElement("parentIdentifier");
				parentIdentifier.setTextContent(esObject.getParentIdentifier());
				dataset.appendChild(parentIdentifier);
				
				//additionalContent
				Element additionalContent = doc.createElement("additionalContent");
				additionalContent.setTextContent(esObject.getAdditlContent());
				dataset.appendChild(additionalContent);
				
				
				
//				Transformer transformer = TransformerFactory.newInstance().newTransformer();
//				// formatting properties
//				transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
//				transformer.setOutputProperty(OutputKeys.INDENT, "yes");
//				transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
//				//transformer.setOutputProperty(OutputKeys.ENCODING, "utf8");
				
				
				domSource = new DOMSource(doc);
				//System.out.println(doc.getXmlEncoding());
				
				sw = new StringWriter();
				
				//StreamResult streamResult = new StreamResult(new File("dom-parsre-test.xml"));
				
				StreamResult streamResult = new StreamResult(sw); 
				//StreamResult streamResult = new StreamResult(out); 
				transformer.transform(domSource, streamResult);
				
				//pass string to ESConnector
				if(!ABCDStaxParser.isbTestrun()) {
					bPushed = esConnector.putES(sw, esObject.getIdentifier());
				}else {
					logger.debug(sw.toString());
					bPushed = true;
				}
				
				
			} 
			catch (DOMException | IllegalArgumentException | ParserConfigurationException
					| TransformerFactoryConfigurationError | TransformerException | FileNotFoundException | URISyntaxException | InterruptedException | ExecutionException   e) {
				logger.error("Error building DOM for: " + esObject.getIdentifier() + e.getMessage());
				bPushed = false;
			}
//			catch (DOMException | IllegalArgumentException | ParserConfigurationException
//					| TransformerFactoryConfigurationError | TransformerException  e) {
//				logger.error("Error building DOM for: " + esObject.getIdentifier() + e.getMessage());
//				bPushed = false;
//			}
			return bPushed;
 	
	}
}
