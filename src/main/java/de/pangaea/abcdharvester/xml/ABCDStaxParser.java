package de.pangaea.abcdharvester.xml;

import java.io.File;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLSocket;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import de.pangaea.abcdharvester.BMSDatasets;
import de.pangaea.abcdharvester.XmlArchive;
import de.pangaea.abcdharvester.es.ESInputObject;
import de.pangaea.abcdharvester.es.ESMetadataInputObject;
import de.pangaea.abcdharvester.es.ESConnector;
import de.pangaea.abcdharvester.util.ConfReader;
import de.pangaea.abcdharvester.util.DateUtility;
import de.pangaea.abcdharvester.json.FileIO;
import de.pangaea.abcdharvester.json.JSONDeserialiser;
import de.pangaea.abcdharvester.json.JSONtest;


public class ABCDStaxParser {
	private static XMLInputFactory inputFactory;
	private static InputStream input;
	private static XMLStreamReader xmlStreamReader;
	private static XMLEventReader eventReader; 
	private static XMLEventReader metadataEventReader; 
	
	private static boolean bCit = false;
	private static boolean bLicDataset = false;
	private static boolean bLicUnit = false;
	private static boolean bToU = false;
	private static boolean bOwn = false;
	private static boolean bOwnUnit = false;
	private static boolean bIdentUnit = false;
	private static boolean bDesc = false;
	private static boolean bGathering = false;
	private static boolean bContentContact = false;
	private static boolean bContentContactUnit = false;
	private static boolean bMultiMediaObj = false;
	private static boolean bNoAgent = true;
	private static boolean blinkageIsSet = false;
	private static boolean bFailed = false;
	private static boolean bCountry = false;
	private static boolean bUnit = false;
	private static boolean bChronoStrat = false;
	private static boolean bIsKingdom = false;
	
	private static boolean bDeleteFile = true;
	private static boolean bDatasetMetadataPushed = false;
	private static boolean bProcess = false;
	private static boolean bDeleteOldArchive = false;
	private static boolean bISOTimestampsSet = false;
	
	private static String scientificName = null;
	private static String title = null;
	private static String details = null;
	private static String citation = null;
	private static String archiveDesc = null;
	private static ArrayList<String> contributorListDataset = new ArrayList<String>();
	private static ArrayList<String> contributorListUnit = new ArrayList<String>();
	private static ArrayList<String> typeList;
	private static ArrayList<String> locationList;
	private static ArrayList<String> linkageList;
	private static ArrayList<String> licenseList = new ArrayList<String>();
	private static ArrayList<String> subjectList;
	
	private static String recordBasis = null;
	private static String kindOfunit = null;
	private static String unitID = null;
	private static String recordURI = "";
	private static String fileURI = null;
	private static String unitGUID = "";
	private static String higherTaxonRank = null;
	private static String higherTaxonName = null;
	private static String fullName = "";
	private static String inheritedName = "";
	private static String givenName = "";
	private static String titleTemp = "";
	private static String isoDateStringBegin = "";
	private static String isoDateStringEnd = "";
	private static String dateString = "";
	private static String beginDateForAdditionalContent = "";
	private static String endDateForAdditionalContent = "";
	private static String dateForAdditionalContent = "";
	
	private static String provider_dataset = "";
	private static String archiveID = "";
	
	private static String sourceInstID = "";
	private static String sourceID = "";
	private static String dateLastEdited = "";
	private static String localityText = "";
	private static String countryName = "";
	private static String countryISOCode = "";
	private static String spatialDatum = "";
	private static String longitude = "";
	private static String latitude = "";





	private static final String PARENT_IDNTIFIER_PREFIX = "urn:gfbio.org:abcd:set:";
	private static final String LINKAGE_TYPE_METADATA = "metadata";
	private static final String LINKAGE_TYPE_MULTIMEDIA = "multimedia";
	private static final String LINKAGE_TYPE_DATA = "data";
	
	private static final String SUBJECT_TYPE_PARAMETER = "parameter";
	private static final String SUBJECT_TYPE_STRATIGRAPHY = "stratigraphy";
	private static final String SUBJECT_TYPE_TAXONOMY = "taxonomy";
	private static final String SUBJECT_TYPE_KINGDOM = "kingdom";
	
	private static int iCounter = 0;
	private static Properties props;
	
	private static Logger logger; 
	
	private static boolean bPushed = true;
	private static boolean bDatasetPushed = true;
	private static StringWriter sw = new StringWriter();
	
	private static HashMap<String,String> providerMap = new HashMap<String, String>();
	//archiveMap contains current archives listed in BMS
	private static HashMap<String,String> archiveMap = new HashMap<String, String>();
	//pushedMap contains archives pushed to index during previous harvest
	private static HashMap<String,String> pushedMap = new HashMap<String, String>();
	private static String providerID = "";
	private static HashMap<String, BMSDatasets> bmsMap = new HashMap<String, BMSDatasets>();
	
	private static final String DS_TYPE = "ABCD_Dataset";
	private static final String DS_FORMAT = "text/html"; 
	
	private static String es_host = "";
	private static String es_user = "";
	private static String es_pw = "";
	private static String es_http_version = "";
	private static String es_https = "";
	
	private static boolean bOverride = false;
	private static String archive = "";
	private static String version = "";
	
	private static boolean bTestrun = false;
	
	public static void main(String[] args) {
		PropertyConfigurator.configure("./conf/log4j.properties");
		logger = Logger.getLogger(ABCDStaxParser.class.getName());
		props = ConfReader.readProps("./conf/conf.properties");
		setEs_host(props.getProperty("es_host"));
		setEs_user(props.getProperty("es_user"));
		setEs_pw(props.getProperty("es_pw"));
		setEs_http_version(props.getProperty("es_http_version"));
		setEs_https(props.getProperty("es_https"));
		setbTestrun(Boolean.parseBoolean(props.getProperty("testrun"))); 
		bOverride = false;
		/**
		 * check for arguments
		 * HARVEST:
		 * "harvest 9_389" will force harvest (even if archive version was already harvested). Comparing
		 * 	datasets.txt and datasetsPushed.txt will be overridden.
		 * DELETE:
		 * "delete 9_389_490" will force deletion from index and exit.
		 */
		String sCommand = "";
		String sArchive = "";
		
		if(args.length != 2 && args.length != 0) {
			logger.error("Program requires zero or two arguments - quitting.");
			System.exit(0);
		}
		
		if(args.length == 2) {
			sCommand = args[0];
			sArchive = args[1];
			int d = sArchive.lastIndexOf("_");
			archive = sArchive.substring(0, d);
			version = sArchive.substring(d+1, sArchive.length());
		}
		
		if(sCommand.equals("delete")) {
			try {
				ESConnector.deleteDataset(archive, version);
			} catch (URISyntaxException | InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				logger.error("Error deleting from index: " + e.getMessage());
			}
			System.exit(0);
		}	
		
		if(sCommand.equals("harvest")) {
			bOverride = true;
		}
		
		logger.info("Checking for new ABCD archives to parse...");
		if(isbTestrun()) {
			logger.info("Running in testmode, not pushing to index...");
		}
		parseABCD();
		//de.pangaea.abcdharvester.util.FileIO.writeFile(sw.toString());
		//ESConnector.commit();
		logger.info("...finished.");
		System.exit(0);
	}
	
	private static ABCDDOMBuilder abcdDomBuilder = new ABCDDOMBuilder();
	
	private static void parseDatasetMetadata( String datasetID, File inFile) {
		/**
		 * variables for parent dataset xml
		 * 
		 */
		String dsTitle = "";
		ArrayList<String> dsContactList = new ArrayList<String>();
		String dsDescription = "";
		ArrayList<String> dsContribList = new ArrayList<String>();
		String dsPublisher = "";
		String dsDataCenter = "";
		
		ArrayList<String> dsLinkageList = new ArrayList<String>();
		String dsIdentifier = "";
		String dsSource = "";
		String dsLic = "";
		String dsToU = "";
		String dsRelation = "";
		 String dsDate = "";
		ArrayList<String> dsRightsList = new ArrayList<String>();
		boolean bDSContentContact = false;
		boolean bDSUnit = false;
		boolean bDSDesc = false;
		boolean bDSOwn = false;
		boolean bDSIdentifierSet = false;
		boolean bDSCit = false;
		boolean bDSToU = false;
		boolean bDSLic = false;
		dsContactList.clear();
		dsContribList.clear();
		dsLinkageList.clear();
		dsRightsList.clear();
		
		
		
		ESMetadataInputObject esMDObject = new ESMetadataInputObject();
		StartElement startElement = null;
		EndElement endElement = null;
		bDSContentContact = false;
		bDSUnit = false;
		bDSDesc = false;
		bDSOwn = false;
		bDSIdentifierSet = false;
		bDSCit = false;
		bDSToU = false;
		bDSLic = false;
		
		try (InputStreamReader inReader = new InputStreamReader(new FileInputStream(inFile))){
			
				metadataEventReader = inputFactory.createXMLEventReader(inReader);
				while(metadataEventReader.hasNext()) {
					
					XMLEvent event = metadataEventReader.nextEvent();
					Characters chars;
					if(event.isStartElement()) {
						startElement = event.asStartElement();
					}
					if(event.isEndElement()) {
						endElement = event.asEndElement();
						if(endElement.getName().getLocalPart().equalsIgnoreCase("Owner")) {
							bDSOwn = false;
						}
						if(endElement.getName().getLocalPart().equalsIgnoreCase("ContentContact")) {
		            		bDSContentContact = false;
						}
						if(endElement.getName().getLocalPart().equalsIgnoreCase("Description")) {
		            		bDSDesc = false;
						}
						if(endElement.getName().getLocalPart().equalsIgnoreCase("TermsOfUse")) {
		            		bDSToU = false;
						}
						if(endElement.getName().getLocalPart().equalsIgnoreCase("License")) {
		            		bDSLic = false;
						}
						if(endElement.getName().getLocalPart().equalsIgnoreCase("Citation")) {
		            		bDSCit = false;
						}
					}
					
					if(event.isStartElement() && startElement.getName().getLocalPart().equalsIgnoreCase("Unit")){					
						bDSUnit = true;
						//System.out.println("UNIT OPEN");
					}
					if(event.isStartElement() && !bDSUnit) {
						
							
							if(startElement.getName().getLocalPart().equalsIgnoreCase("ContentContact")) {
			            		bDSContentContact = true;
							}
							if(startElement.getName().getLocalPart().equalsIgnoreCase("Description")) {
			            		bDSDesc = true;
							}
							if(startElement.getName().getLocalPart().equalsIgnoreCase("Owner")) {
			            		bDSOwn = true;
							}
							if(startElement.getName().getLocalPart().equalsIgnoreCase("TermsOfUse")) {
			            		bDSToU = true;
							}
							if(startElement.getName().getLocalPart().equalsIgnoreCase("License")) {
			            		bDSLic = true;
							}
							if(startElement.getName().getLocalPart().equalsIgnoreCase("Citation")) {
			            		bDSCit = true;
							}
							
							if(metadataEventReader.peek().isCharacters()) {
								chars = (Characters) metadataEventReader.nextEvent();
								if(bDSContentContact && startElement.getName().getLocalPart().equalsIgnoreCase("Name")) {
									if(!dsContactList.contains("Contact: ".concat( chars.getData().replaceAll("\\s+"," ") ))) {
										dsContactList.add("Contact: ".concat( chars.getData().replaceAll("\\s+"," ") ));
									}
									if(!dsContribList.contains(chars.getData().replaceAll("\\s+"," "))) {
										dsContribList.add(chars.getData().replaceAll("\\s+"," "));
									}
									
								}
								if(startElement.getName().getLocalPart().equalsIgnoreCase("Title")) {
									dsTitle = "Data set: ".concat( chars.getData().replaceAll("\\s+"," ") );
								}
								if(bDSDesc && startElement.getName().getLocalPart().equalsIgnoreCase("Details")) {
									dsDescription = chars.getData().replaceAll("\\s+"," ").concat(", zipped ABCD Archive");
									//set archiveWideDesc to use as descriptipn for units if those donT contain description
									setArchiveDesc(chars.getData().replaceAll("\\s+"," "));
								}
								if(bDSOwn && startElement.getName().getLocalPart().equalsIgnoreCase("Text")) {
									if(!dsContribList.contains(chars.getData().replaceAll("\\s+"," "))) {
										dsContribList.add(chars.getData().replaceAll("\\s+"," "));
									}
									bDSOwn = false;
								}
								//if(startElement.getName().getLocalPart().equalsIgnoreCase("RecordURI")) {
								if(startElement.getName().getLocalPart().equalsIgnoreCase("URI") && !bDSLic) {
									if(!dsLinkageList.contains(chars.getData().replaceAll("\\s+"," ").concat("#").concat(LINKAGE_TYPE_METADATA))) {
										dsLinkageList.add( chars.getData().replaceAll("\\s+"," ").concat("#").concat(LINKAGE_TYPE_METADATA) );
									}
									
								}
								if(bDSDesc && startElement.getName().getLocalPart().equalsIgnoreCase("URI")) {
									dsIdentifier = chars.getData().replaceAll("\\s+"," ");
									//bDSIdentifierSet = true;
									bDSDesc = false;
								}
								if(!bDSDesc && startElement.getName().getLocalPart().equalsIgnoreCase("URI")) {
									dsRelation = chars.getData().replaceAll("\\s+"," ");
								}
								if(bDSCit && startElement.getName().getLocalPart().equalsIgnoreCase("Text")) {
									dsSource = chars.getData().replaceAll("\\s+"," ");
									bDSCit = false;
								}
								if(bDSToU && startElement.getName().getLocalPart().equalsIgnoreCase("Text")) {
									dsToU = chars.getData().replaceAll("\\s+"," ");
									bDSToU = false;
								}
								if(bDSLic && startElement.getName().getLocalPart().equalsIgnoreCase("Text")) {
									dsLic = chars.getData().replaceAll("\\s+"," ");
									//bDSLic = false;
								}
								if(startElement.getName().getLocalPart().equalsIgnoreCase("DateModified")) {
									dsDate = chars.getData().replaceAll("\\s+"," ");
								}
								
								
							
						}
						
					}
					
					
				}
				
				if(!dsToU.isBlank() && !dsToU.equals(dsLic)) {
					dsRightsList.add(dsToU);
				}
				if(!dsLic.isBlank()) {
					dsRightsList.add("License: ".concat(dsLic));
				}
				//add BMS URL to linkageList
				dsLinkageList.add(getBMSArchiveURL(datasetID).concat("#").concat(LINKAGE_TYPE_DATA));
				// add publisher and datacenter
				esMDObject.setPublisher("Data Center " + providerMap.get(providerID).split("#")[0] );
				esMDObject.setDataCenter("Data Center " + providerMap.get(providerID).split("#")[0] );
				esMDObject.setDsContactList(dsContactList);
				esMDObject.setDsContribList(dsContribList);
				esMDObject.setDsTitle(dsTitle);
				esMDObject.setDsDescription(dsDescription);
				esMDObject.setDsLinkageList(dsLinkageList);
				esMDObject.setDsIdentifier(dsIdentifier);
				//esMDObject.setDsRelation(dsRelation);
				esMDObject.setDsSource(dsSource);
				esMDObject.setDsRightsList(dsRightsList);
				esMDObject.setDsType(DS_TYPE);
				esMDObject.setDsFormat(DS_FORMAT);
				esMDObject.setDsDate(dsDate);
				
				bDatasetPushed = abcdDomBuilder.generateDatasetXMLTree(esMDObject);
				//bDatasetPushed = true;
				inReader.close();
				metadataEventReader.close();
			
		} catch (FileNotFoundException e) {
			logger.error("Error parsing dataset metadata: " + e.getMessage());
			setbDatasetMetadataPushed(false);
		} catch (IOException e) {
			logger.error("Error parsing dataset metadata: " + e.getMessage());
			setbDatasetMetadataPushed(false);
		} catch (XMLStreamException e){
			logger.error("Error parsing dataset metadata: " + e.getMessage());
			setbDatasetMetadataPushed(false);
		}
		
		
		
		
		//setbDatasetMetadataPushed(true);
		setbDatasetMetadataPushed(bDatasetPushed);
	}
	
	private static void parseABCD() {
		
		
		
		
		providerMap = FileIO.readFile("providerNames.txt");
		
		
		//datasets.txt contains current files from BMS
		if(bOverride) {
			archiveMap.put(archive, version);
		}else {
			archiveMap = FileIO.readFile("datasets.txt");
		}
		
		//datasetsPushed.txt contains archives pushed to index in during last harvest
		pushedMap = FileIO.readFile("datasetsPushed.txt"); 
		
		Set newArchiveSet = archiveMap.keySet();
		String archiveKey;
		Iterator iter = newArchiveSet.iterator();
//		while( iter.hasNext() ){
//			archiveKey = (String)iter.next();
//			if(!pushedMap.containsKey(archiveKey)) {
//				//process archive
//				System.out.println("new archive: " + archiveKey);
//			}
//			else if( (pushedMap.containsKey(archiveKey)) && (!pushedMap.get(archiveKey).equals(archiveMap.get(archiveKey))) ){
//				//process archive
//				System.out.println("new version: " + archiveKey+"_"+archiveMap.get(archiveKey));
//			}else {
//				System.out.println("fuck off");
//			}
//		}
		//System.exit(0);
		ESInputObject esObject =new ESInputObject();
		
		bmsMap = JSONDeserialiser.deserialiseBMS();
		/**
		 * TODO: iterate over all dataset directories
		 * 		 get provider from dirname (first digit(s))
		 */
		//production
		File[] abcdFiles = new File(props.getProperty("abcd_path")).listFiles();
		//Property("abcd_path")).listFiles();
		//debug
		//sFile[] abcdFiles = new File("/home/abe/Daten/marum/projects/gfbio/bms_archives/debug/").listFiles();
		if(abcdFiles.length == 0) {
			logger.info("No archives to process, quitting.");
			System.exit(0);
		}
		
		inputFactory=XMLInputFactory.newInstance();
		
		//set to COALESCING to avoid incomplete tag contents
		inputFactory.setProperty(XMLInputFactory.IS_COALESCING, true);
		
		//for(File file : abcdFiles){
		  while(iter.hasNext()) {
			bProcess = false;
			bDeleteOldArchive = false;
			archiveKey = (String)iter.next();
			//process only if archive is new or if existing archive has a new version
			if( !pushedMap.containsKey(archiveKey)){
				bProcess = true; 
			}
			if( (pushedMap.containsKey(archiveKey) ) && (!pushedMap.get(archiveKey).equals(archiveMap.get(archiveKey)))) {
			    bProcess = true;
			    // set flag to delete old archive version
			    bDeleteOldArchive = true;
			}
			
			//force processing if manual harvest of a single archive is initiated
			if(bOverride) {
				bProcess = true;
			}
			if(bProcess) {	
			String path = (String) props.get("abcd_path");
			File file  = new File( path.concat("/").concat(archiveKey) ); 
			//bDeleteOldArchive = false;
			if(bDeleteOldArchive) {
				try {
					ESConnector.deleteDataset(archiveKey, pushedMap.get(file.getName() ));
				} catch (URISyntaxException | InterruptedException | ExecutionException e) {
					logger.error("Error deleting archive: " + e.getMessage());
				}
			}
			
			
			if(file != null && file.isDirectory()) {

				setProvider_dataset(file.getName());
				setArchiveID(archiveMap.get(file.getName()));
				//use providerID later to get provider name/shortname from providerMap
				providerID = file.getName().split("_")[0];
				
				setbDatasetMetadataPushed(false);
				
				logger.info("Processing " + file.getName().concat("_").concat(archiveMap.get(file.getName() )));
				//System.out.println("Processing " + file.getName().concat("_").concat(archiveMap.get(file.getName() )));
				Characters chars;
				StringBuilder stringBuilder = new StringBuilder();
				
//				inputFactory=XMLInputFactory.newInstance();
//				//set to COALESCING to avoid incomplete tag contents
//				inputFactory.setProperty(XMLInputFactory.IS_COALESCING, true);
				
				File[] responseFiles = new File( file.getPath()).listFiles();
				
				for(File respFile : responseFiles) {
					logger.info(respFile.getName());
					
					
					
					contributorListDataset.clear();
					
					bFailed = false;
					
//					inputFactory=XMLInputFactory.newInstance();
//					//set to COALESCING to avoid incomplete tag contents
//					inputFactory.setProperty(XMLInputFactory.IS_COALESCING, true);
			
					try (InputStreamReader isr = new InputStreamReader(new FileInputStream(respFile))){
						
						
						
						/**
						 *call method to parse and push dataset level metadata ONCE 
						 */
						if(!isbDatasetMetadataPushed()) {
							parseDatasetMetadata( file.getName(), respFile);
							//break to push dataset metadata only (no units, test mode)
							//break;
						}
						
						//if metadata parsing and pushing to index was not successful, stop processing units, as these would be pushed to index without parent
						if(!isbDatasetMetadataPushed()) {
							logger.info("Not processing " + archiveKey + " - parent not pushed to index.");
							break;
						}
						
						
						eventReader = inputFactory.createXMLEventReader(isr);
												
						while(eventReader.hasNext()){
							//int event=xmlStreamReader.next();
							StartElement startElement = null;
							EndElement endElement = null;
							XMLEvent event = eventReader.nextEvent();
							
							//create new object for passing data to elasticsearch
			            	//esObject = new ESInputObject();
			            	
							if(event.isStartElement()) {
								startElement = event.asStartElement();
							}
							if(event.isEndElement()) {
								endElement = event.asEndElement();
							}
							//bCit = false;
							
							if(event.isStartElement() && startElement.getName().getLocalPart().equalsIgnoreCase("Unit")){
								/**
								 * reset esObject fields here
								 */
								esObject.setRelationUnitID("");
								esObject.setIdentifier("");
								esObject.setScientificName("");
								esObject.setLatitude("");
								esObject.setLongitude("");
								//esObject.setParentIdentifier("");
								esObject.setDate("");
								esObject.setDescription("");
								esObject.setRelationURI("");
								esObject.setLicense("");
								esObject.setStatDate("");
								if(locationList != null) {
									locationList.clear();
								}
								
								contributorListDataset.clear();
								esObject.setTitle("");
								contributorListUnit.clear();
								esObject.setAdditlContent("");
								
								blinkageIsSet = false;
								linkageList = new ArrayList<String>();
								//licenseList = new ArrayList<String>();
								subjectList = new ArrayList<String>();
								licenseList.clear();
								bUnit = true;
								typeList = new ArrayList<String>();
								typeList.add("ABCD_Unit");
								//System.out.println("UNIT OPEN");
							}
							
							
							
				            if(bUnit && event.isStartElement()) {
				            	
				            	//StartElement element = xmlEvent.asStartElement();
				            	
	//			            	if(eventReader.peek().isCharacters()) {
	//				            	Characters cars = (Characters) eventReader.nextEvent();
	//				            	if(!cars.getData().isBlank()) {
	//				            		stringBuilder.append(cars.getData().replaceAll("\\s+"," "));
	//				            	}
	//			            	}
				            	if(startElement.getName().getLocalPart().equalsIgnoreCase("MultiMediaObject")) {
									bMultiMediaObj = true;
								}
				            	
				            	if(startElement.getName().getLocalPart().equalsIgnoreCase("License")) {
									bLicUnit = true;
								}
				            	
				            	if(startElement.getName().getLocalPart().equalsIgnoreCase("Owner")) {
				            		bOwnUnit = true;
								}
//				            	if(startElement.getName().getLocalPart().equalsIgnoreCase("Identifications")) {
//				            		bIdentUnit = true;
//								}
				            	if(startElement.getName().getLocalPart().equalsIgnoreCase("ContentContact")) {
				            		bContentContactUnit = true;
								}
				            	if(startElement.getName().getLocalPart().equalsIgnoreCase("Gathering")) {
				            		bGathering = true;
				            		locationList = new ArrayList<String>();
								}
				            	
				            	if(eventReader.peek().isCharacters()) {
				            		chars = (Characters) eventReader.nextEvent();
					            	//add all unit tags to stringBuilder to fill additionalContent
				            		//if(!chars.getData().isBlank()) {
				            		if(!chars.getData().trim().isEmpty()) {
					            		stringBuilder.append(chars.getData().replaceAll("\\s+"," "));
					            		stringBuilder.append(", ");
					            	}
				            		//in addition, write to specific variables
				            		//contributor unit level
									if(startElement.getName().getLocalPart().equalsIgnoreCase("creator") && bMultiMediaObj) {
										//chars = (Characters) eventReader.nextEvent();
										bMultiMediaObj = false;
										//System.out.println(chars.getData().replaceAll("\\s+"," "));
										contributorListUnit.add(chars.getData().replaceAll("\\s+"," "));
									}
									if(startElement.getName().getLocalPart().equalsIgnoreCase("Text") && bOwnUnit) {
										contributorListUnit.add(chars.getData().replaceAll("\\s+"," "));
										bOwnUnit = false;
									}
//									if(startElement.getName().getLocalPart().equalsIgnoreCase("FullName") && bIdentUnit) {
//										contributorListUnit.add(chars.getData().replaceAll("\\s+"," "));
//										bIdentUnit = false;
//									}
									if(startElement.getName().getLocalPart().equalsIgnoreCase("Name") && bContentContactUnit) {
										bContentContactUnit = false;
										contributorListUnit.add(chars.getData().replaceAll("\\s+"," "));
									}
									
									//relation from AssociatedUnitID
									if(startElement.getName().getLocalPart().equalsIgnoreCase("AssociatedUnitID")){
										//chars = (Characters) eventReader.nextEvent();
										esObject.setRelationUnitID(chars.getData().replaceAll("\\s+"," "));
									}
									
									
									if(bGathering) {
										if(startElement.getName().getLocalPart().equalsIgnoreCase("FullName") ) {
											//chars = (Characters) eventReader.nextEvent();
											fullName = chars.getData().replaceAll("\\s+"," ");
											contributorListUnit.add(fullName);
											//bNoAgent = false;
										}
										//if(bNoAgent){
											if(startElement.getName().getLocalPart().equalsIgnoreCase("InheritedName") ) {
												//chars = (Characters) eventReader.nextEvent();
												inheritedName = chars.getData().replaceAll("\\s+"," ");
											}
											if(startElement.getName().getLocalPart().equalsIgnoreCase("GivenNames") ) {
												//chars = (Characters) eventReader.nextEvent();
												givenName = chars.getData().replaceAll("\\s+"," ");
											}
											if(inheritedName.length() > 0 && givenName.length() > 0) {
												contributorListUnit.add(inheritedName +", "+ givenName);
											}
											//bNoAgent = false;
										//}
										//if(bNoAgent) {
											if(startElement.getName().getLocalPart().equalsIgnoreCase("AgentText") ) {
												//chars = (Characters) eventReader.nextEvent();
												contributorListUnit.add(chars.getData().replaceAll("\\s+"," "));
												//bNoAgent = false;
											}
										//}
										//if(bNoAgent) {
											if(startElement.getName().getLocalPart().equalsIgnoreCase("GatheringAgentsText") ) {
												//chars = (Characters) eventReader.nextEvent();
												contributorListUnit.add(chars.getData().replaceAll("\\s+"," "));
												//bNoAgent = false;
											}
										//}
									}
									
									//relation 
									//type
									if(startElement.getName().getLocalPart().equalsIgnoreCase("RecordBasis")) {
										//chars = (Characters) eventReader.nextEvent();
										
									
										recordBasis = chars.getData().replaceAll("\\s+"," ").
												replaceAll("(\\p{Ll})(\\p{Lu})","$1 $2").toLowerCase();
										//esObject.setRecordBasis(recordBasis);
										//typeList = new ArrayList<String>();
										//typeList.add("ABCD_Unit");
										typeList.add(recordBasis);
										//esObject.setType(typeList);
										if(recordBasis.contains("Specimen")) {
											typeList.add("PhysicalObject");
										}
										if(recordBasis.equals("MaterialSample")) {
											typeList.add("PhysicalObject");
										}
										if(recordBasis.contains("Photograph")) {
											typeList.add("Image");
										}
										if(recordBasis.equals("MultimediaObject")) {
											typeList.add("Image");
										}
										if(recordBasis.contains("Observation")) {
											typeList.add("Dataset");
										}
										if(recordBasis.equals("Literature")) {
											typeList.add("Dataset");
										}
									}
									if(startElement.getName().getLocalPart().equalsIgnoreCase("KindOfUnit")) {
										//chars = (Characters) eventReader.nextEvent();
										kindOfunit = chars.getData().replaceAll("\\s+"," ");
										if(kindOfunit.contains("Specimen")) {
											typeList.add("PhysicalObject");
										}
										if(kindOfunit.equals("MaterialSample")) {
											typeList.add("PhysicalObject");
										}
										if(kindOfunit.contains("Photograph")) {
											typeList.add("Image");
										}
										if(kindOfunit.equals("MultimediaObject")) {
											typeList.add("Image");
										}
										if(kindOfunit.contains("Observation")) {
											typeList.add("Dataset");
										}
										if(kindOfunit.equals("Literature")) {
											typeList.add("Dataset");
										}
									}
									if(startElement.getName().getLocalPart().equalsIgnoreCase("fileURI")) {
										//chars = (Characters) eventReader.nextEvent();
										fileURI = chars.getData().replaceAll("\\s+"," ");
										if(!typeList.contains("MultimediaObject")) {
											typeList.add("MultimediaObject");
										}
										
										//add linkage (type multimedia)
										linkageList.add(fileURI.concat("#").concat(LINKAGE_TYPE_MULTIMEDIA));
										
										//add subject
										if(!subjectList.contains(("Multimedia Object").concat("#").concat(SUBJECT_TYPE_PARAMETER))){
											subjectList.add( ("Multimedia Object").concat("#").concat(SUBJECT_TYPE_PARAMETER) );
										}
										
									}
									
									//linkage
									if(startElement.getName().getLocalPart().equalsIgnoreCase("RecordURI")) {
										//chars = (Characters) eventReader.nextEvent();
										recordURI = chars.getData().replaceAll("\\s+"," ");
									}
									
									if(startElement.getName().getLocalPart().equalsIgnoreCase("UnitGUID") && !blinkageIsSet) {
										//chars = (Characters) eventReader.nextEvent();
										unitGUID = chars.getData().replaceAll("\\s+"," ");
									}
									
									
									//identifier
									/**
									 * TODO: prefix providerID_datasetID_archive_ID
									 */
									if(startElement.getName().getLocalPart().equalsIgnoreCase("UnitID")) {
										//chars = (Characters) eventReader.nextEvent();
										unitID = chars.getData().replaceAll("\\s+"," ");
										esObject.setIdentifier(unitID);
									}
									
									//SourceID for linkage if no RecordURI exists
									if(startElement.getName().getLocalPart().equalsIgnoreCase("SourceID")) {
										sourceID = chars.getData().replaceAll("\\s+"," ");
									}
									
									if(startElement.getName().getLocalPart().equalsIgnoreCase("FullScientificNameString")) {
										//chars = (Characters) eventReader.nextEvent();
										//System.out.println(xmlStreamReader.getAttributeValue(0));
										//System.out.println(chars.getData().replaceAll("\\s+"," "));
										scientificName = chars.getData().replaceAll("\\s+"," ");
										esObject.setScientificName(scientificName);
										
										//subject
										subjectList.add(scientificName.concat("#").concat(SUBJECT_TYPE_TAXONOMY));
									}
									
									
									if(startElement.getName().getLocalPart().equalsIgnoreCase("Text") && bLicUnit) {
										//chars = (Characters) eventReader.nextEvent();
										bLicUnit = false;
										//String license = chars.getData().replaceAll("\\s+"," ");
										//esObject.setLicense(chars.getData().replaceAll("\\s+"," "));
										if(!licenseList.contains("License for associated multimedia objects: ".concat(chars.getData().replaceAll("\\s+"," ")))) {
											licenseList.add("License for associated multimedia objects: ".concat(chars.getData().replaceAll("\\s+"," ")));
										}
											
										
									}
							
									
									
									//coverage (+ start/enddates)
									if(startElement.getName().getLocalPart().equalsIgnoreCase("LatitudeDecimal")) {
										//chars = (Characters) eventReader.nextEvent();
										latitude = chars.getData().replaceAll("\\s+"," ");
										//esObject.setLatitude(chars.getData().replaceAll("\\s+"," "));
										esObject.setLatitude(latitude);
										
										//subject
										subjectList.add( ("Latitude").concat("#").concat(SUBJECT_TYPE_PARAMETER));
									}
									if(startElement.getName().getLocalPart().equalsIgnoreCase("LongitudeDecimal")) {
										//chars = (Characters) eventReader.nextEvent();
										longitude = chars.getData().replaceAll("\\s+"," ");
										//esObject.setLongitude(chars.getData().replaceAll("\\s+"," "));
										esObject.setLongitude(longitude);
										//subject
										subjectList.add( ("Longitude").concat("#").concat(SUBJECT_TYPE_PARAMETER));
									}
									
									//subject: stratigraphy
									if(startElement.getName().getLocalPart().equalsIgnoreCase("ChronostratigraphicTerm")) {
										bChronoStrat = true;
									}
									if(startElement.getName().getLocalPart().equalsIgnoreCase("Term") && bChronoStrat) {
										subjectList.add(chars.getData().replaceAll("\\s+"," ").concat("#").concat(SUBJECT_TYPE_STRATIGRAPHY));
										bChronoStrat = false;
									}
									
									
									
									//location && Agent (as contributor)
//									if(startElement.getName().getLocalPart().equalsIgnoreCase("Gathering")) {
//										bGathering = true;
//										locationList = new ArrayList<String>();
//									}
									if(event.isStartElement() && startElement.getName().getLocalPart().equalsIgnoreCase("Name") && bGathering) {
										//chars = (Characters) eventReader.nextEvent();
										locationList.add(chars.getData().replaceAll("\\s+"," "));
									}
									if(startElement.getName().getLocalPart().equalsIgnoreCase("AreaName") && bGathering) {
										//chars = (Characters) eventReader.nextEvent();
										locationList.add(chars.getData().replaceAll("\\s+"," "));
									}
									//subject
									if(startElement.getName().getLocalPart().equalsIgnoreCase("Altitude") && bGathering) {
										subjectList.add( ("Altitude").concat("#").concat(SUBJECT_TYPE_PARAMETER));
									}
									if(startElement.getName().getLocalPart().equalsIgnoreCase("LocalityText") && bGathering) {
										subjectList.add( ("Locality").concat("#").concat(SUBJECT_TYPE_PARAMETER));
									}
									
									//datestamp and subject
									if(startElement.getName().getLocalPart().equalsIgnoreCase("ISODateTimeBegin") && bGathering) {
										//chars = (Characters) eventReader.nextEvent();
										isoDateStringBegin = chars.getData().replaceAll("\\s+"," ");
										subjectList.add( ("Date").concat("#").concat(SUBJECT_TYPE_PARAMETER));
									}
									if(startElement.getName().getLocalPart().equalsIgnoreCase("ISODateTimeEnd") && bGathering) {
										//chars = (Characters) eventReader.nextEvent();
										isoDateStringEnd = chars.getData().replaceAll("\\s+"," ");
										//subjectList.add( ("Date").concat("#").concat(SUBJECT_TYPE_PARAMETER));
									}
									if(startElement.getName().getLocalPart().equalsIgnoreCase("DateText") && bGathering) {
										//chars = (Characters) eventReader.nextEvent();
										dateString = chars.getData().replaceAll("\\s+"," ");
									}
									
									
									//unit
									if(startElement.getName().getLocalPart().equalsIgnoreCase("HigherTaxonName")) {
										//chars = (Characters) eventReader.nextEvent();
										higherTaxonName = chars.getData().replaceAll("\\s+"," ");
										
									}
									if(startElement.getName().getLocalPart().equalsIgnoreCase("HigherTaxonRank")) {
										//chars = (Characters) eventReader.nextEvent();
										higherTaxonRank = chars.getData().replaceAll("\\s+"," ");
										if(higherTaxonRank.equalsIgnoreCase("regnum") || higherTaxonRank.equalsIgnoreCase("kingdom")) {
											//System.out.println(higherTaxonRank);
											bIsKingdom = true;
										}
										//subject
										//subjectList.add(chars.getData().replaceAll("\\s+"," ").concat("#").concat(SUBJECT_TYPE_KINGDOM));
										//subject
										if(bIsKingdom) {
											subjectList.add( (higherTaxonName).concat("#").concat(SUBJECT_TYPE_KINGDOM) );
											bIsKingdom = false;
										}else {
											subjectList.add( (higherTaxonName).concat("#").concat(SUBJECT_TYPE_TAXONOMY));
										}
									}
									
									
									
				            	}
				            }
							
				            
				            
				            
							
							
							if(event.isStartElement() && !bUnit){
								//System.out.println(xmlStreamReader.getName());
								//if(xmlStreamReader.getName().toString().equals("{http://www.tdwg.org/schemas/abcd/2.06}Title")) {
								
								if(startElement.getName().getLocalPart().equalsIgnoreCase("Title")) {
									chars = (Characters) eventReader.nextEvent();
									//System.out.println(xmlStreamReader.getName());
									//System.out.println(xmlStreamReader.getAttributeValue(0));
									//System.out.println(chars.getData().replaceAll("\\s+"," "));
									title = chars.getData().replaceAll("\\s+"," ");
									
									//use title to construct parentIdentifier
									//esObject.setParentIdentifier(PARENT_IDNTIFIER_PREFIX.concat(title.replaceAll("\\s", "_")));
									
									//changed: set parentidentifier to actual parentID
									esObject.setParentIdentifier("urn:gfbio.org:abcd:".concat(getProvider_dataset()).concat("_").concat(getArchiveID()));
									//esObject.setTitle(title);
								}
								if(startElement.getName().getLocalPart().equalsIgnoreCase("DateModified")) {
									chars = (Characters) eventReader.nextEvent();
									esObject.setDate(chars.getData().replaceAll("\\s+"," "));
								}
								if(startElement.getName().getLocalPart().equalsIgnoreCase("Description")) {
									bDesc = true;
								}
								if(startElement.getName().getLocalPart().equalsIgnoreCase("TermsOfUse")) {
									bToU = true;
								}
								if(startElement.getName().getLocalPart().equalsIgnoreCase("License")) {
									bLicDataset = true;
								}
								
								//moved
//								if(startElement.getName().getLocalPart().equalsIgnoreCase("MultiMediaObject")) {
//									bMultiMediaObj = true;
//								}
								
								//dc:description (dataset metadata)
								if(startElement.getName().getLocalPart().equalsIgnoreCase("Details") && bDesc) {
									chars = (Characters) eventReader.nextEvent();
									//bDesc = false;
									//System.out.println(xmlStreamReader.getAttributeValue(0));
									//System.out.println(chars.getData().replaceAll("\\s+"," "));
									details = chars.getData().replaceAll("\\s+"," ");
									esObject.setDescription(details);
								}
								//dc:relation (URI from description tag)
								if(startElement.getName().getLocalPart().equalsIgnoreCase("URI") && bDesc) {
									chars = (Characters) eventReader.nextEvent();
									esObject.setRelationURI(chars.getData().replaceAll("\\s+"," "));
								}
								if(startElement.getName().getLocalPart().equalsIgnoreCase("Text") && bLicDataset) {
									chars = (Characters) eventReader.nextEvent();
									bLicDataset = false;
									//String license = chars.getData().replaceAll("\\s+"," ");
									esObject.setLicense(chars.getData().replaceAll("\\s+"," "));
									//licenseList.add("License: ".concat(chars.getData().replaceAll("\\s+"," ")));
									
									
								}
								if(startElement.getName().getLocalPart().equalsIgnoreCase("Text") && bToU) {
									chars = (Characters) eventReader.nextEvent();
									bToU = false;
									//String license = chars.getData().replaceAll("\\s+"," ");
									//esObject.setTermsOfUse(chars.getData().replaceAll("\\s+"," "));
									licenseList.add(chars.getData().replaceAll("\\s+"," "));
								}
								
								if(startElement.getName().getLocalPart().equalsIgnoreCase("Owner")) {
									bOwn = true;
								}
								if(startElement.getName().getLocalPart().equalsIgnoreCase("ContentContact")) {
									bContentContact = true;
								}
								
								//contributor dataset level
								if(startElement.getName().getLocalPart().equalsIgnoreCase("Name") && bContentContact) {
									chars = (Characters) eventReader.nextEvent();
									bContentContact = false;
									//System.out.println(chars.getData().replaceAll("\\s+"," "));
									String temp = chars.getData().replaceAll("\\s+"," ");
									if(temp.contains(",")) {
										String[] tempArr = chars.getData().replaceAll("\\s+"," ").split(",");
										for(int i = 0; i < tempArr.length; i++) {
											contributorListDataset.add(tempArr[i]);
										}
									}else {
										contributorListDataset.add(temp);
									}
									
								}


								
								


								//publisher
								// example: Data Center SNSB
								esObject.setPublisher("Data Center " + providerMap.get(providerID).split("#")[0] );
								//dataCenter
								//example: Data Center SNSB
								esObject.setDataCenter("Data Center " + providerMap.get(providerID).split("#")[0] );
								
								

								if(startElement.getName().getLocalPart().equalsIgnoreCase("Citation")) {
									bCit = true;
								}
//								
//						
								if(startElement.getName().getLocalPart().equalsIgnoreCase("Text") && bCit) {
									chars = (Characters) eventReader.nextEvent();
									bCit = false;
									//System.out.println(chars.getData().replaceAll("\\s+"," "));
									citation = chars.getData().replaceAll("\\s+"," ");
									esObject.setSource(citation);
								}

								
								
							}//dataset level end
							
							
							
							if(event.isEndElement() && endElement.getName().getLocalPart().equalsIgnoreCase("Description")){
								bDesc = false;
							}
							
							if(event.isEndElement() && endElement.getName().getLocalPart().equalsIgnoreCase("Gathering")){
								//System.out.println("owner closed");
								bGathering = false;
								//bNoAgent = true;
								
								
								
								//check which date to use
								if(isoDateStringBegin.length() > 0 && isoDateStringEnd.length() > 0){
									setISOTimestamps("both", esObject);
								}
								if(isoDateStringBegin.length() > 0 && isoDateStringEnd.length() == 0){
									setISOTimestamps("begin", esObject);
								}
								if(isoDateStringBegin.length() == 0 && isoDateStringEnd.length() > 0){
									setISOTimestamps("end", esObject);
								}
								
								
								
								if(dateString.length() > 0) {
									//dateForAdditionalContent = dateString;
									//only use dateString if no ISO timestamps available
									if(!bISOTimestampsSet) {
										try {
	//										esObject.setStatDate(DateUtility.convertABCDWeirdoDates(dateString));
	//										esObject.setEndDate(DateUtility.convertABCDWeirdoDates(dateString));
											esObject.setStatDate(DateUtility.isDateTimeValid(dateString));
											esObject.setEndDate(DateUtility.isDateTimeValid(dateString));
										} catch (NumberFormatException | NullPointerException e) {
											//logger.info("Date unusable: " + dateString + ", trying to use yyyy as fallback.");
											//logger.info("Incoming date string: " + dateString);
											//abcd dates can contain all kinds of junk. trying to use year only
											Pattern pattern = Pattern.compile("(\\d{4})");
											Matcher m = pattern.matcher(dateString);
											if(m.find()) {
												esObject.setStatDate(m.group(0));
												esObject.setEndDate(m.group(0));
											}else {
												esObject.setStatDate("");
												esObject.setEndDate("");
											}
										}
									}
								}
								isoDateStringBegin = "";
								isoDateStringEnd = "";
								dateString = "";
								bISOTimestampsSet = false;
								
								esObject.setLocationList(locationList);
							}
							if(event.isEndElement() && endElement.getName().getLocalPart().equalsIgnoreCase("Owner") && !bUnit){
								//System.out.println("owner closed");
								bOwn = false;
								//get name and shortname from map
								//shortname
								contributorListDataset.add(providerMap.get(providerID).split("#")[0]);
								//longname
								contributorListDataset.add(providerMap.get(providerID).split("#")[1]);
								esObject.setContrListDataset(contributorListDataset);
							}
							
							
							//construct dc:title
							if(recordBasis == null) {
								recordBasis = "";
							}
							if(scientificName != null) {
								titleTemp = scientificName + ", a " + recordBasis + " record of the " + title + " dataset ";
							}else if(scientificName == null && higherTaxonName != null){
								titleTemp = higherTaxonName + "( " + higherTaxonRank + " ), unspecified, a " + recordBasis + " record of the " + title + " dataset";
							}else {
								titleTemp = "Undetermined " + recordBasis;
							}
							esObject.setTitle(titleTemp + "[ID: "+ unitID + " ]");
							
							if(event.isEndElement() && endElement.getName().getLocalPart().equalsIgnoreCase("Unit")){
								
								esObject.setType(typeList);
									
								//check which linkage to set:1=RecordURI or 2=FileURI or 3=Biocase query tool
								if(recordURI.length() > 0 && recordURI.contains("http")) {
									linkageList.add(recordURI.concat("#").concat(LINKAGE_TYPE_METADATA));
								}
								else if(unitGUID.length() > 0 && unitGUID.contains("http")) {
									linkageList.add(unitGUID.concat("#").concat(LINKAGE_TYPE_METADATA));
								}
								else {
									/**
									 *  linkage: use Biocase query tool + datasource if above 2 options did not work
									 */
//									linkageList.add( bmsMap.get(file.getName()).getBiocaseUrl().concat("querytool/details.cgi?dsa=").
//											concat(bmsMap.get(file.getName()).getDatasource().
//													concat("&detail=unit&schema=http://www.tdwg.org/schemas/abcd/2.06&cat=").
//													concat(unitID)).
//													concat("#").concat(LINKAGE_TYPE_METADATA) ) ;
									linkageList.add( bmsMap.get(file.getName()).getBiocaseUrl().concat("querytool/details.cgi?dsa=").
											concat(bmsMap.get(file.getName()).getDatasource().
													concat("&detail=unit&schema=http://www.tdwg.org/schemas/abcd/2.06&cat=").
													concat(unitID)).
													concat("&inst=").concat(bmsMap.get(file.getName()).getProviderShortname()).
													concat("&col=").concat(sourceID).
													concat("#").concat(LINKAGE_TYPE_METADATA) ) ;
										}
//									System.out.println(
//									linkageList.add(
//									bmsMap.get(file.getName()).getBiocaseUrl().concat("?dsa").
//									concat(bmsMap.get(file.getName()).getDatasource().
//											concat("&detail=unit&schema=http://www.tdwg.org/schemas/abcd/2.06&cat=").
//											concat(unitID)).
//											concat("#").concat(LINKAGE_TYPE_METADATA) ) );
//								}
								
								recordURI = "";
								unitGUID = "";
								
	
								
								esObject.setLinkageList(linkageList);
					 			esObject.setContrListUnit(contributorListUnit);
					 			esObject.setContrListDataset(contributorListDataset);
					 			bOwnUnit = false;
								esObject.setLicenseList(licenseList);
								
								esObject.setSubjectList(subjectList);
					 			stringBuilder.deleteCharAt(stringBuilder.lastIndexOf(","));
					 			esObject.setAdditlContent(stringBuilder.toString());
								stringBuilder.delete(0, stringBuilder.length());
								//check if unit contained description - if not, fall back to description from dataset metadata
								if(esObject.getDescription().length() == 0) {
									esObject.setDescription(getArchiveDesc());
								}
					 			//if(!bFailed) {
									//ABCDDOMBuilder.generateXMLTree(esObject);
					 			
								bPushed = abcdDomBuilder.generateXMLTree(esObject);
								if(!bPushed) {
									setbDeleteFile(bPushed);
								}
								//}{
								contributorListUnit.clear();
								/**
								 * TODO: check if typeList can be cleared here
								 */
								bUnit = false;
								bCountry = false;
								//System.out.println("UNIT CLOSED");
							}
							
						if( (esObject.getDescription() != null) && esObject.getDescription().length() ==0) {
							//System.out.println("sdsf");
						}
						//System.out.println(esObject.getDescription());
						}
						isr.close();
						eventReader.close();
					}catch (FileNotFoundException e) {
						logger.error(e.getMessage());
					} catch (XMLStreamException e) {
						logger.error(file.getName() +" "+ respFile.getName() +" "+ e.getMessage());
						//this is a parsing exception due to invalid xml, so DO NOT retry this file
						/**
						 * TODO: move corrupt files to dedicated folder? 
						 * OR: ipmplement method to remove invalid chars
						 */
						setbDeleteFile(false);
					} catch (IOException e) {
						logger.error(e.getMessage());
					} 

					//only delete file if indexing succeeded
					if(isbDeleteFile()) {
						respFile.delete();
					}
					
					//reset bDeleteFile to true for next iteration
					setbDeleteFile(true);
				}
			}
			//delete archive directory if empty 
			if(file.isDirectory() && file.list().length == 0) {
				file.delete();
			}
		  }
		}
		//call final commit
		//logger.info("Calling final commit...");
		ESConnector.commit();
		logger.info("...done.");
		//System.exit(0);
		
		
		//copy files
		Path source = Paths.get("datasets.txt");
		try {
			File file = new File("datasetsPushed.txt");
			if(file.exists()) {
				file.delete();
			}
			Files.copy(source, source.resolveSibling("datasetsPushed.txt"));
			//Files.move(source, source.resolveSibling("datasetsPushed.txt"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void setISOTimestamps(String sWhichDate, ESInputObject esObject) {
		if(sWhichDate.equals("begin")) {
			//catch exception if date formatting fails
			try {
				esObject.setStatDate(DateUtility.isDateTimeValid(isoDateStringBegin));
				//set boolean to true to avoid overwriting
				bISOTimestampsSet = true;
			} catch (NumberFormatException | NullPointerException e) {
				//logger.info("Date unusable: " + isoDateString + ", trying to use yyyy as fallback.");
				//logger.info("Incoming date string: " + isoDateString);
				
				//abcd dates can contain all kinds of junk. trying to use year only
				Pattern pattern = Pattern.compile("(\\d{4})");
				Matcher mStart = pattern.matcher(isoDateStringBegin);
				Matcher mEnd = pattern.matcher(isoDateStringEnd);
				if(mStart.find()) {
					esObject.setStatDate(mStart.group(0));
					//esObject.setEndDate(m.group(0));
				}else {
					esObject.setStatDate("");
					//esObject.setEndDate("");
				}
				if(mEnd.find()) {
					esObject.setEndDate(mEnd.group(0));
				}else {
					esObject.setEndDate("");
					//esObject.setEndDate("");
				}
			}
		}
		
		if(sWhichDate.equals("end")) {
			try {
				esObject.setEndDate(DateUtility.isDateTimeValid(isoDateStringEnd));
				bISOTimestampsSet = true;
			} catch (Exception e) {
				//abcd dates can contain all kinds of junk. trying to use year only
				Pattern pattern = Pattern.compile("(\\d{4})");
				Matcher mStart = pattern.matcher(isoDateStringBegin);
				Matcher mEnd = pattern.matcher(isoDateStringEnd);
				if(mStart.find()) {
					esObject.setStatDate(mStart.group(0));
					//esObject.setEndDate(m.group(0));
				}else {
					esObject.setStatDate("");
					//esObject.setEndDate("");
				}
				if(mEnd.find()) {
					esObject.setEndDate(mEnd.group(0));
				}else {
					esObject.setEndDate("");
					//esObject.setEndDate("");
				}
			}
		}
		
		if(sWhichDate.equals("both")) {
			//catch exception if date formatting fails
			try {
//				esObject.setStatDate(DateUtility.convertABCDWeirdoDates(isoDateString));
//				esObject.setEndDate(DateUtility.convertABCDWeirdoDates(isoDateString));
				esObject.setStatDate(DateUtility.isDateTimeValid(isoDateStringBegin));
				esObject.setEndDate(DateUtility.isDateTimeValid(isoDateStringEnd));
				//set boolean to true to avoid overwriting
				bISOTimestampsSet = true;
			} catch (NumberFormatException | NullPointerException e) {
				//logger.info("Date unusable: " + isoDateString + ", trying to use yyyy as fallback.");
				//logger.info("Incoming date string: " + isoDateString);
				
				//abcd dates can contain all kinds of junk. trying to use year only
				Pattern pattern = Pattern.compile("(\\d{4})");
				Matcher mStart = pattern.matcher(isoDateStringBegin);
				Matcher mEnd = pattern.matcher(isoDateStringEnd);
				if(mStart.find()) {
					esObject.setStatDate(mStart.group(0));
					//esObject.setEndDate(m.group(0));
				}else {
					esObject.setStatDate("");
					//esObject.setEndDate("");
				}
				if(mEnd.find()) {
					esObject.setEndDate(mEnd.group(0));
				}else {
					esObject.setEndDate("");
					//esObject.setEndDate("");
				}
				
				//bFailed = true;
			}
		}
	}
	
	private static String getBMSArchiveURL(String bmsArchiveID) {
		String retString = "";
//		Set<String> bmsMapKeys = bmsMap.keySet();
//		Iterator<String> iter = bmsMapKeys.iterator();
//		while(iter.hasNext()) {
			BMSDatasets bmsDataset = bmsMap.get(bmsArchiveID);
			List<XmlArchive>archivesList = bmsDataset.getXmlArchives();
			
			ListIterator<XmlArchive> listIter = archivesList.listIterator();
			while(listIter.hasNext()) {
				XmlArchive archive = listIter.next();
				if(archive.getLatest()) {
					retString = archive.getXmlArchive();
					break;
				}
			}
			//}
		return retString;
	}
	
	public static String getProvider_dataset() {
		return provider_dataset;
	}


	public static void setProvider_dataset(String provider_dataset) {
		ABCDStaxParser.provider_dataset = provider_dataset;
	}


	public static String getEs_host() {
		return es_host;
	}

	public static String getEs_user() {
		return es_user;
	}

	public static String getEs_pw() {
		return es_pw;
	}

	public static String getEs_http_version() {
		return es_http_version;
	}

	public static String getEs_https() {
		return es_https;
	}

	public static void setEs_host(String es_host) {
		ABCDStaxParser.es_host = es_host;
	}

	public static void setEs_user(String es_user) {
		ABCDStaxParser.es_user = es_user;
	}

	public static void setEs_pw(String es_pw) {
		ABCDStaxParser.es_pw = es_pw;
	}

	public static void setEs_http_version(String es_http_version) {
		ABCDStaxParser.es_http_version = es_http_version;
	}

	public static void setEs_https(String es_https) {
		ABCDStaxParser.es_https = es_https;
	}

	public static String getArchiveID() {
		return archiveID;
	}


	public static void setArchiveID(String archiveID) {
		ABCDStaxParser.archiveID = archiveID;
	}


	public static boolean isbDeleteFile() {
		return bDeleteFile;
	}


	public static void setbDeleteFile(boolean bDeleteFile) {
		ABCDStaxParser.bDeleteFile = bDeleteFile;
	}

	public static boolean isbDatasetMetadataPushed() {
		return bDatasetMetadataPushed;
	}

	public static void setbDatasetMetadataPushed(boolean bDatasetMetadataPushed) {
		ABCDStaxParser.bDatasetMetadataPushed = bDatasetMetadataPushed;
	}

	static String getArchiveDesc() {
		return archiveDesc;
	}

	static void setArchiveDesc(String archiveDesc) {
		ABCDStaxParser.archiveDesc = archiveDesc;
	}

	public static boolean isbTestrun() {
		return bTestrun;
	}

	public static void setbTestrun(boolean bTestrun) {
		ABCDStaxParser.bTestrun = bTestrun;
	}
}
