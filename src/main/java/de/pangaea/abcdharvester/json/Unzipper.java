package de.pangaea.abcdharvester.json;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Unzipper {
	private static final int BUFFER_SIZE = 4096;
	
    public static void unzip(String destDirectory, String archiveID) throws IOException {
        File destDir = new File(destDirectory);
      
        System.out.println("Unzipping: "+destDirectory+"/"+archiveID+".zip");
        ZipInputStream zipIn = new ZipInputStream(new FileInputStream(destDirectory+"/"+archiveID+".zip"));
        ZipEntry entry = zipIn.getNextEntry();
        // iterates over entries in the zip file
        while (entry != null) {
            //String filePath = destDirectory + File.separator + providerID.concat("_").concat(datasetID).concat("_").concat(archiveID).concat("_"+entry.getName());
        	String filePath = destDirectory + File.separator + archiveID.concat("_"+entry.getName());
            if (!entry.isDirectory()) {
                // if the entry is a file, extract it
                extractFile(zipIn, filePath);
            } else {
                // if the entry is a directory, make the directory
                File dir = new File(filePath);
                dir.mkdir();
            }
            zipIn.closeEntry();
            entry = zipIn.getNextEntry();
        }
        zipIn.close();
    }
	
	private static void extractFile(ZipInputStream zipIn, String filePath) {
		BufferedOutputStream bos;
		try {
			bos = new BufferedOutputStream(new FileOutputStream(filePath));
			byte[] bytesIn = new byte[BUFFER_SIZE];
			int read = 0;

			while ((read = zipIn.read(bytesIn)) != -1) {
				bos.write(bytesIn, 0, read);
			}
			bos.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
}
