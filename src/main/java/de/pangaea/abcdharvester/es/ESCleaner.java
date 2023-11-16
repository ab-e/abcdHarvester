/**
 * 
 */
package de.pangaea.abcdharvester.es;

import java.util.ArrayList;
import java.util.Scanner;

import de.pangaea.abcdharvester.util.FileIO;

/**
 * @author abe
 *
 * class is used to create a list of all current archive id's and check index
 *  for any id's not contained in this list (=outdated or invalid). These will then be removed from the index.
 */
public class ESCleaner {

	/**
	 * @param args
	 */
	static ArrayList<String> archiveIDList = new ArrayList<String>();
	
	public static void main(String[] args) {
		String strIDs = FileIO.readFileForBuffer("datasetsPushed.txt").toString();
		Scanner scanner = new Scanner(strIDs);
		while(scanner.hasNext()) {
			archiveIDList.add("urn:gfbio.org:abcd:".concat(scanner.next().replace("=", "_")));
		}
		System.out.println(archiveIDList.size());
		for(int i = 0; i < archiveIDList.size(); i++) {
			System.out.println("\""+archiveIDList.get(i)+"\""+",");
		}
		
	}

}
