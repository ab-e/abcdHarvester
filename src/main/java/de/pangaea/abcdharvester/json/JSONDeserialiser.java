package de.pangaea.abcdharvester.json;

import java.util.HashMap;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import de.pangaea.abcdharvester.BMSDatasets;

public class JSONDeserialiser {

	private static HashMap<String,BMSDatasets> bmsMap;
	
	public static HashMap<String,BMSDatasets> deserialiseBMS(){
		// TODO Auto-generated method stub
		Gson gson = new Gson();
		String json = FileIO.readFileForString("bmsArchives.json");
		Object obj = new JsonParser().parse(json);
		JsonArray jArray = (JsonArray) obj;
		//BMSDatasets bmsd = new Gson().fromJson(json, BMSDatasets.class);
		//System.out.println(jArray.size());
		bmsMap = new HashMap<String,BMSDatasets>();
		for(int i = 0; i < jArray.size(); i++) {
			//JsonObject jsonObj = (JsonObject) jArray.get(i);  
			BMSDatasets bd = gson.fromJson((JsonObject) jArray.get(i), BMSDatasets.class);
			if(!bd.getBiocaseUrl().endsWith("/")) {
				bd.setBiocaseUrl(bd.getBiocaseUrl().concat("/"));
			}
			//System.out.println(bd.getBiocaseUrl());
			//System.out.println(bd.getDatasource());
			bmsMap.put(bd.getProviderId()+"_"+bd.getDatasetId(), bd);
			
			//BMSDatasets bmsd = (BMSDatasets)jsonObj.getAsJsonObject();
			//bmsMap.put(bd.get("dataset_id").getAsString().concat("_"+bd.get("provider_id").getAsString()), jsonObj);
		}
		return bmsMap;
	}

}
