
package de.pangaea.abcdharvester;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class BMSProvider {

    @SerializedName("provider_id")
    @Expose
    private String providerId;
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

    public String getProviderId() {
        return providerId;
    }

    public void setProviderId(String providerId) {
        this.providerId = providerId;
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

}
