
package de.pangaea.abcdharvester.delete;


import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;


public class ESDeleter {

    @SerializedName("query")
    @Expose
    private Query query;

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

}
