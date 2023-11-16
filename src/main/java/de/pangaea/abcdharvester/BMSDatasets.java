
package de.pangaea.abcdharvester;

import java.util.List;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class BMSDatasets {

    @SerializedName("provider_id")
    @Expose
    private String providerId;
    @SerializedName("provider_datacenter")
    @Expose
    private String providerDatacenter;
    @SerializedName("provider_shortname")
    @Expose
    private String providerShortname;
    @SerializedName("provider_name")
    @Expose
    private String providerName;
    @SerializedName("provider_url")
    @Expose
    private String providerUrl;
    @SerializedName("biocase_url")
    @Expose
    private String biocaseUrl;
    @SerializedName("datasource")
    @Expose
    private String datasource;
    @SerializedName("dataset_id")
    @Expose
    private String datasetId;
    @SerializedName("dataset")
    @Expose
    private String dataset;
    @SerializedName("custom_landingpage")
    @Expose
    private String customLandingpage;
    @SerializedName("xml_archives")
    @Expose
    private List<XmlArchive> xmlArchives = null;
    @SerializedName("useful_links")
    @Expose
    private List<Object> usefulLinks = null;

    public String getProviderId() {
        return providerId;
    }

    public void setProviderId(String providerId) {
        this.providerId = providerId;
    }

    public String getProviderDatacenter() {
        return providerDatacenter;
    }

    public void setProviderDatacenter(String providerDatacenter) {
        this.providerDatacenter = providerDatacenter;
    }

    public String getProviderShortname() {
        return providerShortname;
    }

    public void setProviderShortname(String providerShortname) {
        this.providerShortname = providerShortname;
    }

    public String getProviderName() {
        return providerName;
    }

    public void setProviderName(String providerName) {
        this.providerName = providerName;
    }

    public String getProviderUrl() {
        return providerUrl;
    }

    public void setProviderUrl(String providerUrl) {
        this.providerUrl = providerUrl;
    }

    public String getBiocaseUrl() {
        return biocaseUrl;
    }

    public void setBiocaseUrl(String biocaseUrl) {
        this.biocaseUrl = biocaseUrl;
    }

    public String getDatasource() {
        return datasource;
    }

    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public String getCustomLandingpage() {
        return customLandingpage;
    }

    public void setCustomLandingpage(String customLandingpage) {
        this.customLandingpage = customLandingpage;
    }

    public List<XmlArchive> getXmlArchives() {
        return xmlArchives;
    }

    public void setXmlArchives(List<XmlArchive> xmlArchives) {
        this.xmlArchives = xmlArchives;
    }

    public List<Object> getUsefulLinks() {
        return usefulLinks;
    }

    public void setUsefulLinks(List<Object> usefulLinks) {
        this.usefulLinks = usefulLinks;
    }

}
