package de.pangaea.abcdharvester.json;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import de.pangaea.abcdharvester.BMSDatasets;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

public class JSONtest {

	private static String providerID = "";
	private static String providerDatacenter = "";
	private static String datasetID = "";
	private static String biocaseURL = "";
	private static String datasource = "";
	private static String providerName = "";
	private static String providerShortName = "";
	private static String archiveID = "";
	private static String path = "/home/abe/Daten/marum/projects/gfbio/bms_archives/abcd/";
	private static HashMap<String, String> datasetIDmap = new HashMap<String, String>();
	private static HashMap<String, String> providerIDNAMEmap = new HashMap<String, String>();
	private static HashMap<String, String> providerNameShortnameMap = new HashMap<String, String>();
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		File parentDir;// = new File("/home/abe/Daten/marum/projects/gfbio/bms_archives");
		
		//String json = "{\"size\": \"10\", \"query\": {\"match\":{\"name.fullmatch\": {\"query\": \"globigerina  \\r\\n buloiedes \", \"fuzziness\" : \"auto\" }}}}";
        //JsonObject jsonObj = new JsonParser().parse(json).getAsJsonObject();
		
		//read from local file: last harvested latest archive id
		HashMap<String,String> inMap = FileIO.readFile("datasets.txt");
	    JsonObject jsonObj = null;
		Object obj = null;
		try {
			URL url = new URL("http://bms.test.code.naturkundemuseum.berlin/services/datasets");
		    
			BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
            StringBuilder strBuilder = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
            	strBuilder.append(line).append(System.lineSeparator());
            }
            in.close();
            //write json string to file
            FileIO.writeFile("bmsArchives", strBuilder.toString());
		    //BMSDatasets bmsd = new Gson().fromJson(reader, BMSDatasets.class);
            obj = new JsonParser().parse(strBuilder.toString());
            //obj = new JsonParser().parse(readUrl(url.toString()));
//		    obj = new JsonParser().parse(new FileReader("xml_archives.json"));
		} catch (JsonIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonSyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        //JsonArray jarray = jsonObj.getAsJsonArray();
       JsonArray jArray = (JsonArray) obj;
       
       Set<Entry<String, JsonElement>> jset = null;
       for(int i = 0; i < jArray.size(); i++){
    	   jsonObj = (JsonObject) jArray.get(i);
    	   jset = jsonObj.entrySet();
           
           //System.out.println(jset.size());
            for (Map.Entry<String, JsonElement> entry: jset) {
               // System.out.println(entry.getKey());
                
            	 if(entry.getKey().equals("provider_id")){
            		
            		 providerID = entry.getValue().getAsString();
            	 }
            	 if(entry.getKey().equals("provider_datacenter")){
             		
            		 providerDatacenter = entry.getValue().getAsString();
            	 }
            	 if(entry.getKey().equals("biocase_url")){
              		
            		 biocaseURL = entry.getValue().getAsString();
            	 }
            	 if(entry.getKey().equals("datasource")){
              		
            		 datasource = entry.getValue().getAsString();
            	 }
            	 if(entry.getKey().equals("dataset_id")){
            		 datasetID = entry.getValue().getAsString();
            	 }
            	 if(entry.getKey().equals("provider_shortname")){
              		
            		 providerShortName = entry.getValue().getAsString();
            	 }
            	 if(entry.getKey().equals("provider_name")){
               		
            		 providerName = entry.getValue().getAsString();
            	 }
            	
                if(entry.getKey().equals("xml_archives")){
                	JsonElement je = entry.getValue();
                	JsonArray jArr = (JsonArray) je;
                	//JsonObject idObject = jArr.getAsJsonObject();
                	
                	for(int a = 0; a < jArr.size(); a++){
                		JsonObject idObject = (JsonObject) jArr.get(a);
                    	archiveID = idObject.get("archive_id").getAsString();
                    	String xml_archive = idObject.get("xml_archive").getAsString();
                    	String latest = idObject.get("latest").getAsString();
                    	String latest_id = idObject.get("archive_id").getAsString();
                    	//System.out.println(id);
                    	//System.out.println(xml_archive);
                    	//System.out.println(latest);
                    
                    	if(Boolean.parseBoolean(latest)){
                    		/**
                    		 * TODO: create file containing dataset_id and latest_archive_id 
                    		 * compare latest id with last harvested latest id
                    		 * delete old dataset in case new one is available
                    		 */
                    		//datasetIDmap.put(providerID+"_"+datasetID, latest_id.concat("#").concat(biocaseURL).concat("#").concat(datasource));
                    		datasetIDmap.put(providerID+"_"+datasetID, latest_id);
                    		
                    		parentDir = new File(path);
                    		File newDirectory = new File(parentDir, providerID+"_"+datasetID );
                        	if(!newDirectory.exists()){
                        		newDirectory.mkdir();
                        	}
                        	//if(!xml_archive.contains("biocase.naturkundemuseum-berlin.de") && !xml_archive.contains("biocase.zfmk.de") ) {
                        		//downloadZip(xml_archive, newDirectory.getAbsolutePath(), archiveID );
	                        	//new version:
	                        	
                    		downloadZip(xml_archive, parentDir.getAbsolutePath(), providerID, datasetID, archiveID );
//                        	try {
//								testdownload(xml_archive, i);
//							} catch (IOException e) {
//								// TODO Auto-generated catch block
//								e.printStackTrace();
//							}
                    		
                        	
                        	//!!!COMMENTED FOR TESTING
//                    		try {
//								unzip(new URL(xml_archive), Paths.get(newDirectory.getAbsolutePath()));
//							} catch (MalformedURLException e) {
//								// TODO Auto-generated catch block
//								e.printStackTrace();
//							}
                        	//!!!COMMENTED FOR TESTING
                        	
                        	
                    		//}else {
                        	//	System.out.println("Skipping " + xml_archive);
                        	//}
                        	
                    	}
                	}
                	
                	
                }
//                if(entry.getKey().equals("query")){
//                	
//                	System.out.println(entry.getValue());
//                	JsonObject jsonObj2 = new JsonParser().parse(entry.getValue().toString()).getAsJsonObject();
//                	Set<Entry<String, JsonElement>> jset2 = jsonObj2.entrySet();
//                	for (Map.Entry<String, JsonElement> entry2: jset2) {
//                		System.out.println(entry2.getKey());
//                	}
//                }
                providerIDNAMEmap.put(providerID, providerShortName +"#"+ providerName);
                providerNameShortnameMap.put(providerShortName, providerName);
            }
       }
       
       setProviderNameShortnameMap(providerNameShortnameMap);
       // write id's to file 
       FileIO.writeFile("datasets.txt",datasetIDmap);
       FileIO.writeFile("providerNames.txt",providerIDNAMEmap);
       
        
        Stream<Entry<String, JsonElement>> jStream = jset.stream().sorted();
       
        
//        for(int i = 0; i < jset.size(); i++){
//        	jset.st
//        }
        
//       Gson gson = new Gson();
//       JsonElement el = gson.toJsonTree(jsonObj);
//       
//       System.out.println(el.getAsString());
       
//        System.out.printf(2, jsonObj.get("id").getAsInt());
//        System.out.println("Effective Java", jsonObj.get("name").getAsString());
//        System.out.println("Joshua Bloch", jsonObj.get("author").getAsString());
 
	}

	private static void writeProviderFile() {
		Object obj = null;
		JsonObject jsonObj = null;
		try {
			
			URL url = new URL("http://bms.test.code.naturkundemuseum.berlin/services/providers");
			obj = new JsonParser().parse(readUrl(url.toString()));
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonSyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		JsonArray jArray = (JsonArray) obj;
		Set<Entry<String, JsonElement>> jset = null;
		for(int i = 0; i < jArray.size(); i++){
	    	   jsonObj = (JsonObject) jArray.get(i);
	    	   jset = jsonObj.entrySet();
	           
	           //System.out.println(jset.size());
	            for (Map.Entry<String, JsonElement> entry: jset) {
	               // System.out.println(entry.getKey());
	                
	            	 if(entry.getKey().equals("provider_id")){
	            		
	            		 providerID = entry.getValue().getAsString();
	            	 }
	            	 if(entry.getKey().equals("dataset_id")){
	            		 datasetID = entry.getValue().getAsString();
	            	 }
	            }
		}
	}
	
	private static String readUrl(String urlString) throws Exception {
	    BufferedReader reader = null;
	    try {
	        URL url = new URL(urlString);
	        reader = new BufferedReader(new InputStreamReader(url.openStream()));
	        StringBuffer buffer = new StringBuffer();
	        int read;
	        char[] chars = new char[1024];
	        while ((read = reader.read(chars)) != -1)
	            buffer.append(chars, 0, read); 

	        return buffer.toString();
	    } finally {
	        if (reader != null)
	            reader.close();
	    }
	}
	
	public static void unzip(final URL url, final Path decryptTo) {
        try {
        	//System.out.println("Writing "+ decryptTo.toString());
        	
        	if(decryptTo.endsWith("7_232")){
        		System.out.println(decryptTo.toString());
        	}
			ZipInputStream zipInputStream = new ZipInputStream(Channels.newInputStream(Channels.newChannel(url.openStream())));
			    for (ZipEntry entry = zipInputStream.getNextEntry(); entry != null; entry = zipInputStream.getNextEntry()) {
			    	//System.out.println(entry.getName() + "    "+entry.getSize());
			    	Path toPath = decryptTo.resolve(entry.getName());
			    	
			        if (entry.isDirectory()) {
			            Files.createDirectory(toPath);
			        } else try (FileChannel fileChannel = FileChannel.open(toPath, WRITE, CREATE/*, DELETE_ON_CLOSE*/)) {
			            fileChannel.transferFrom(Channels.newChannel(zipInputStream), 0, Long.MAX_VALUE);
			        }
			    }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			System.out.println("DELETING: "+ decryptTo.toString());
//			File dir = new File(decryptTo.toString());
//			if(dir.exists() && dir.isDirectory() && dir.listFiles().length == 0) {
//				dir.delete();
//			}
		}
       
    }
	
	protected static void testdownload(String archive, int a) throws IOException {
		URL url = new URL(archive);

	    InputStream in = url.openStream();
	    FileOutputStream out = new FileOutputStream(path + "/"+a+".zip");
	    byte[] buffer = new byte[1024];
	    int read = 0;
	    while ((read = in.read(buffer)) >= 0) {
	        out.write(buffer, 0, read);
	    }
	}
	
	protected static void downloadZip(String archive, String path, String providerID, String datasetID, String archiveID){
		
		try {
			URL url = new URL(archive);
			boolean bReconnect = true;
			ReadableByteChannel rbc = null;
			while(bReconnect) {
				
					try {
						rbc = Channels.newChannel(url.openStream());
						bReconnect = false;
					} catch (Exception e) {
						// TODO Auto-generated catch block
						//e.printStackTrace();
						System.out.println("(Reconnecting to: "+ archive);
						try {
							Thread.sleep(3000);
						} catch (InterruptedException et) {
							// TODO Auto-generated catch block
							et.printStackTrace();
						}
					}
				
			}
			
			//check dir
			File dir = new File(path +"/"+  providerID+"_"+datasetID);
			if(!dir.exists()) {
				dir.mkdir();
			}
			
			FileOutputStream fos;
			//fos = new FileOutputStream(path + "/" + id + ".zip");
			System.out.println("Writing file "+path +"/"+  providerID+"_"+datasetID+"_"+archiveID + ".zip");
			//fos = new FileOutputStream(path +"/"+  providerID+"_"+datasetID+"_"+archiveID + ".zip");
			//fos = new FileOutputStream(path +"/"+  datasetID+"_"+archiveID + ".zip");
			fos = new FileOutputStream(dir.getAbsolutePath()+"/"+archiveID+".zip");
			try {
				fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
			fos.close();
			
			//unpack
			//Unzipper.unzip(path + "/" + id + ".zip", path );
			
			try {
				Unzipper.unzip(dir.getAbsolutePath(), archiveID);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//delete zip
			File zf = new File((dir + "/" + archiveID + ".zip"));
			//System.out.println(zf.canRead());
			if(zf.exists()){
				zf.delete();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public static HashMap<String, String> getProviderNameShortnameMap() {
		return providerNameShortnameMap;
	}

	public static void setProviderNameShortnameMap(HashMap<String, String> providerNameShortnameMap) {
		JSONtest.providerNameShortnameMap = providerNameShortnameMap;
	}
}
