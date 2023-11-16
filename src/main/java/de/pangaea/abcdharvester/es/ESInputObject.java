package de.pangaea.abcdharvester.es;

import java.util.ArrayList;

public class ESInputObject {
	
	private String title;
	private String scientificName;
	private String description;
	private String citation;
	private String license;
	private String termsOfUse;
	private ArrayList<String> contrListDataset;
	private ArrayList<String> contrListUnit;
	private String publisher;
	private String dataCenter;
	private ArrayList<String> type;
	private final String FORMAT = "text/html";
	private ArrayList<String> linkageList;
	private ArrayList<String> licenseList;
	private ArrayList<String> subjectList;
	private String identifier;
	private String source;
	private String recordBasis;
	private String nblat ;
	private String wblon;
	private String sblat;
	private String eblon;
	private String latitude = "";
	private String longitude = "";
	private ArrayList<String> locationList;
	private String date = "";
	private String statDate = "";
	private String endDate = "";
	private ArrayList<String> subjList;
	private String rights;
	private String relationURI = "";
	private String relationUnitID = "";
	private String parentIdentifier;
	private String additlContent;
	
	
	
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public ArrayList<String> getContrListDataset() {
		return contrListDataset;
	}
	public void setContrListDataset(ArrayList<String> contrList) {
		this.contrListDataset = contrList;
	}
	public String getPublisher() {
		return publisher;
	}
	public void setPublisher(String publisher) {
		this.publisher = publisher;
	}
	public String getDataCenter() {
		return dataCenter;
	}
	public void setDataCenter(String dataCenter) {
		this.dataCenter = dataCenter;
	}
	public String getFormat() {
		return FORMAT;
	}public String getIdentifier() {
		return identifier;
	}
	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public String getNblat() {
		return nblat;
	}
	public void setNblat(String nblat) {
		this.nblat = nblat;
	}
	public String getWblon() {
		return wblon;
	}
	public void setWblon(String wblon) {
		this.wblon = wblon;
	}
	public String getSblat() {
		return sblat;
	}
	public void setSblat(String sblat) {
		this.sblat = sblat;
	}
	public String getEblon() {
		return eblon;
	}
	public void setEblon(String eblon) {
		this.eblon = eblon;
	}
	public String getStatDate() {
		return statDate;
	}
	public void setStatDate(String statDate) {
		this.statDate = statDate;
	}
	public String getEndDate() {
		return endDate;
	}
	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}
	public ArrayList<String> getSubjList() {
		return subjList;
	}
	public void setSubjList(ArrayList<String> subjList) {
		this.subjList = subjList;
	}
	public String getRights() {
		return rights;
	}
	public void setRights(String rights) {
		this.rights = rights;
	}
	public String getParentIdentifier() {
		return parentIdentifier;
	}
	public void setParentIdentifier(String parentIdentifier) {
		this.parentIdentifier = parentIdentifier;
	}
	public String getAdditlContent() {
		return additlContent;
	}
	public void setAdditlContent(String additlContent) {
		this.additlContent = additlContent;
	}
	public String getScientificName() {
		return scientificName;
	}
	public void setScientificName(String scientificName) {
		this.scientificName = scientificName;
	}
	public String getRecordBasis() {
		return recordBasis;
	}
	public void setRecordBasis(String recordBasis) {
		this.recordBasis = recordBasis;
	}
	public String getCitation() {
		return citation;
	}
	public void setCitation(String citation) {
		this.citation = citation;
	}
	public ArrayList<String> getType() {
		return type;
	}
	public void setType(ArrayList<String> type) {
		this.type = type;
	}
	public String getLatitude() {
		return latitude;
	}
	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}
	public String getLongitude() {
		return longitude;
	}
	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}
	public ArrayList<String> getLocationList() {
		return locationList;
	}
	public void setLocationList(ArrayList<String> locationList) {
		this.locationList = locationList;
	}
	public String getLicense() {
		return license;
	}
	public void setLicense(String license) {
		this.license = license;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public ArrayList<String> getLinkageList() {
		return linkageList;
	}
	public void setLinkageList(ArrayList<String> linkageList) {
		this.linkageList = linkageList;
	}
	public ArrayList<String> getContrListUnit() {
		return contrListUnit;
	}
	public void setContrListUnit(ArrayList<String> contrListUnit) {
		this.contrListUnit = contrListUnit;
	}
	public String getTermsOfUse() {
		return termsOfUse;
	}
	public void setTermsOfUse(String termsOfUse) {
		this.termsOfUse = termsOfUse;
	}
	public String getRelationURI() {
		return relationURI;
	}
	public void setRelationURI(String relationURI) {
		this.relationURI = relationURI;
	}
	public String getRelationUnitID() {
		return relationUnitID;
	}
	public void setRelationUnitID(String relationUnitID) {
		this.relationUnitID = relationUnitID;
	}
	public ArrayList<String> getLicenseList() {
		return licenseList;
	}
	public void setLicenseList(ArrayList<String> licenseList) {
		this.licenseList = licenseList;
	}
	public ArrayList<String> getSubjectList() {
		return subjectList;
	}
	public void setSubjectList(ArrayList<String> subjectList) {
		this.subjectList = subjectList;
	}
}
