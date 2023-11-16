package de.pangaea.abcdharvester.es;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow.Subscriber;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.net.http.*;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;

import de.pangaea.abcdharvester.delete.ESDeleter;
import de.pangaea.abcdharvester.delete.Query;
import de.pangaea.abcdharvester.delete.Term;
import de.pangaea.abcdharvester.util.ConfReader;
import de.pangaea.abcdharvester.util.FileIO;
import de.pangaea.abcdharvester.xml.ABCDDOMBuilder;
import de.pangaea.abcdharvester.xml.ABCDStaxParser;

public class ESConnector {

	private static HttpClient httpClient = null;
	private static String host;// = ABCDStaxParser.getEs_host();
	private static int port;// = Integer.parseInt(ABCDStaxParser.getEs_port());
	
	private static Logger logger; 
	private int iCounter = 0;
	
	private static ExecutorService executor;
	

	private static final String DATASET_ID_PREFIX = "urn:gfbio.org:abcd:";
	
	private static ESDeleter esDeleter = new ESDeleter();
	private static Term term = new Term();
	private static Query query = new Query();
	
	private static HttpClient.Version httpClientVersion;
	
	private static Properties props;
	
    //main only used for tests
	public static void main(String[] args) {
		// TODO Auto-generated method stub
    	props = ConfReader.readProps("./conf/conf.properties");
    	ESConnector esc = new ESConnector();
    	PropertyConfigurator.configure("./conf/log4j.properties");
    	logger = Logger.getLogger(ESConnector.class.getName());
    	
    	host = props.getProperty("es_host");
		try {
			get();
			//putES();
			//deleteDataset("9_389", "490");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    public ESConnector() {
    	PropertyConfigurator.configure("./conf/log4j.properties");
    	props = ConfReader.readProps("./conf/conf.properties");
    	host = props.getProperty("es_host");
    	logger = Logger.getLogger(ESConnector.class.getName());
    	if(props.getProperty("es_http_version").equals("HTTP_1_1")) {
    		httpClientVersion = Version.HTTP_1_1;
    	}
    	if(props.getProperty("es_http_version").equals("HTTP_2")) {
    		httpClientVersion = Version.HTTP_2;
    	}
    	
    	
    }
    
    //tests only
	private static void get() throws URISyntaxException {
		URI uri = new URI(host +  "/" + "gfbio-abcd-push/_search?q="+URLEncoder.encode("in complete checklists during chaffinch breeding"));
		//URI uri = new URI(props.getProperty("es_https") + "://" + host + "/gfbio-abcd-push/_search?q="+URLEncoder.encode("ZFMK Hymenoptera collection"));
		HttpClient httpClient = HttpClient.newHttpClient();
		HttpRequest request = HttpRequest.newBuilder().uri(uri).version(httpClientVersion).build();
		//HttpRequest request = HttpRequest.newBuilder().uri(uri).build();	
		httpClient.sendAsync(request, BodyHandlers.ofString())
			      //.thenApply(HttpResponse::body)
				  .thenApply(HttpResponse::body)
			      .thenAccept(System.out::println)
			      .join();
		
	}
	
	public static void deleteDataset(String archiveKey, String archiveVersion) throws URISyntaxException, InterruptedException, ExecutionException {
		logger.info("Deleting "+ archiveKey +"_"+archiveVersion);
		 URI uri = new URI(host + "/" + "gfbio-abcd-push/pansimple/_delete_by_query");
        
		term.setAbcdDatasetIdentifier(DATASET_ID_PREFIX.concat(archiveKey).concat("_").concat(archiveVersion));
        query.setTerm(term);
        esDeleter.setQuery(query);
        
        Gson gson = new Gson();
        
		final HttpClient httpClient = HttpClient.newBuilder()
	            .authenticator(new Authenticator() {
	                @Override
	                protected PasswordAuthentication getPasswordAuthentication() {
	                    return new PasswordAuthentication(
	                            ABCDStaxParser.getEs_user(),
	                            ABCDStaxParser.getEs_pw().toCharArray());
	                    		
	                }

	            })
	            .build();
        
		//logger.info(BodyPublishers.ofString(gson.toJson(esDeleter).toString()));
		HttpRequest request = HttpRequest.newBuilder()
			      .uri(uri)
			      .version(httpClientVersion)
			      .header("Content-Type", "application/xml")
			      .POST(BodyPublishers.ofString(gson.toJson(esDeleter), Charset.forName("UTF-8")))
			      .build();
		  
		  logger.info("delete query: " +gson.toJson(esDeleter));
		  CompletableFuture<HttpResponse<String>> response =  httpClient.sendAsync(request, BodyHandlers.ofString());
		  logger.info("delete response: " + response.get().body().toString());
		 
		
		  //commit after delete not necessary...
//	       URI commituri = new URI(host + "/" + "gfbio-abcd-push/xml/_commit");
//	       HttpRequest commitrequest = HttpRequest.newBuilder().uri(commituri).POST(BodyPublishers.noBody()).build();
//	       CompletableFuture<HttpResponse<String>> commitresponse = httpClient.sendAsync(commitrequest, BodyHandlers.ofString());
//	       logger.info("Commit delete: " + commitresponse.get().statusCode());
	}
	
	private void killHttpClient() {
		executor.shutdownNow();
		httpClient = null;
		System.gc();
	}
	private static void checkHttpClient() {
		executor = Executors.newSingleThreadExecutor();
		 httpClient = HttpClient.newBuilder()
	            .authenticator(new Authenticator() {
	                @Override
	                protected PasswordAuthentication getPasswordAuthentication() {
	                    return new PasswordAuthentication(
	                            ABCDStaxParser.getEs_user(),
	                            ABCDStaxParser.getEs_pw().toCharArray());
	                }

	            }).connectTimeout(Duration.ofSeconds(5)).executor(executor).build();
		 
	}
	public boolean putES(Writer sw, String unitID) throws URISyntaxException, FileNotFoundException, InterruptedException, ExecutionException {
		URI uri;
		boolean bPushOK = true;
		iCounter++;
		
		logger.debug("pushing to index: " );
		logger.debug(sw.toString());
		
		if (iCounter == 995) {
			//httpClient = null;
			killHttpClient();
			checkHttpClient();
			iCounter = 0;
		}
		try {
			if(httpClient == null) {
				checkHttpClient();
			}
		} catch (Exception e) {
			logger.error("Error checking http client: " + e.getMessage());
		}
		
		 if(unitID == null) {
        	
        	uri = new URI(host  + "/" + 
        			"gfbio-abcd-push/xml/urn:gfbio.org:abcd:"+
        				URLEncoder.encode(ABCDStaxParser.getProvider_dataset()+"_"+ABCDStaxParser.getArchiveID(), StandardCharsets.UTF_8));
        }else {
        	uri = new URI(host  + "/" + 
        			"gfbio-abcd-push/xml/urn:gfbio.org:abcd:"+
        				URLEncoder.encode(ABCDStaxParser.getProvider_dataset()+"_"+ABCDStaxParser.getArchiveID()+":"+unitID, StandardCharsets.UTF_8));
        }
		
		HttpRequest request = HttpRequest.newBuilder()
			      .uri(uri)
			      .version(httpClientVersion)
			      .header("Content-Type", "application/xml; charset=UTF-8")
			      .PUT(BodyPublishers.ofString(sw.toString(), Charset.forName("UTF-8")))
			      .build();
		

       try {
    	//ORIGINAL
		CompletableFuture<HttpResponse<String>> response =  httpClient.sendAsync(request, BodyHandlers.ofString());
		//logger.info("put response: " + response.get().body().toString());
    	   //ORIGINAL
		   if(response.get().statusCode() != 202) {
			   logger.error("Error pushing to index: " + ABCDStaxParser.getProvider_dataset()+"_"+ABCDStaxParser.getArchiveID()+":"+unitID);
			   logger.error("Response status: " + response.get().statusCode());
			   logger.error("Response string: " + response.get().toString());
			   //logger.error("Response body: " + response.get().body());
			   bPushOK = false;
		   }
		   
	} catch (InterruptedException e) {
		logger.error("Error sending1: " + e.getMessage());
		bPushOK = false;
	} catch (ExecutionException e) {
		logger.error("Error sending2 : " + ABCDStaxParser.getProvider_dataset()+"_"+ABCDStaxParser.getArchiveID()+":"+unitID+"  "+e.getMessage());
		bPushOK = false;
	}
       
       return bPushOK;
}
	
	public static void commit() {
		if(httpClient == null) {
			checkHttpClient();
		}
		 try {
			URI commituri = new URI(host + "/" + "gfbio-abcd-push/xml/_commit");
			   HttpRequest commitrequest = HttpRequest.newBuilder().uri(commituri).POST(BodyPublishers.noBody()).build();
			   CompletableFuture<HttpResponse<String>> commitresponse = httpClient.sendAsync(commitrequest, BodyHandlers.ofString());
			   //logger.info("commit put: " +commitresponse.get().statusCode());
		} catch (URISyntaxException e) {
			logger.error("Error sending commit: " + e.getMessage());
		}
	}
	


}
