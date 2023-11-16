
package de.pangaea.abcdharvester;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class XmlArchive {

    @SerializedName("archive_id")
    @Expose
    private String archiveId;
    @SerializedName("xml_archive")
    @Expose
    private String xmlArchive;
    @SerializedName("latest")
    @Expose
    private Boolean latest;

    public String getArchiveId() {
        return archiveId;
    }

    public void setArchiveId(String archiveId) {
        this.archiveId = archiveId;
    }

    public String getXmlArchive() {
        return xmlArchive;
    }

    public void setXmlArchive(String xmlArchive) {
        this.xmlArchive = xmlArchive;
    }

    public Boolean getLatest() {
        return latest;
    }

    public void setLatest(Boolean latest) {
        this.latest = latest;
    }

}
