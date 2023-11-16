
package de.pangaea.abcdharvester.delete;


import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;


public class Term {

    @SerializedName("abcdDatasetIdentifier")
    @Expose
    private String abcdDatasetIdentifier;

    public String getAbcdDatasetIdentifier() {
        return abcdDatasetIdentifier;
    }

    public void setAbcdDatasetIdentifier(String abcdDatasetIdentifier) {
        this.abcdDatasetIdentifier = abcdDatasetIdentifier;
    }

}
