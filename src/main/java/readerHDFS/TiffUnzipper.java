package readerHDFS;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * This class is a modified version of UnzipUtility
 * http://www.codejava.net/java-se/file-io/programmatically-extract-a-zip-file-using-java
 * Make a TiffUnzipper object and call the unzip method with a local-FS-filepath
 * as argument. It unzips in the zip's directory and returns the local-FS-filepath which:
 * 1) is in the unzipped folder
 * 2) is the local-FS-filepath of the unzipped .tiff file!!!
 * 
 * G.A.
 * gioargyr@di.uoa.gr
 * gioargyr@gmail.com
 */

public class TiffUnzipper {
    /**
     * Size of the buffer to read/write data
     */
    private static final int BUFFER_SIZE = 65536;
    /**
     * Extracts a zip file specified by the zipFilePath to a directory specified by
     * destDirectory (will be created if does not exists)
     * @param zipFilePath
     * @param destDirectory
     * @throws IOException
     */
    public String unzip(String zipFilePath) throws IOException {
    	String tiffFilePath = "";
    	//System.out.println("");
    	System.out.println("~~~ Initiating unzipping ~~~");
    	File zipFile = new File(zipFilePath);
    	String destDirectory = zipFile.getParent();
        ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath));
        ZipEntry entry = zipIn.getNextEntry();
        // iterates over entries in the zip file
        while (entry != null) {
            String filePath = destDirectory + File.separator + entry.getName();
            if (!entry.isDirectory()) {
                extractFile(zipIn, filePath);
                //After exrtaction search and find the .tiff filepath
                String[] suffix = filePath.split("\\.");
//                for (int i = 0; i < suffix.length; i++) {
//                	System.out.println("I AM INSIDE THE FOR-LOOP. i = " + i);
//                }
                if(suffix[2].equals("tiff")) {
                	//System.out.println("THE TIFF'S LOCAL-FS-FILEPATH IS:");
                	//System.out.println(filePath);
                	tiffFilePath = filePath;
                }
            }
            else {
                File dir = new File(filePath);
                dir.mkdir();
            }
            zipIn.closeEntry();
            entry = zipIn.getNextEntry();
        }
        zipIn.close();
        //System.out.println("");
        //System.out.println("~~~ Completing unzipping ~~~");
        //System.out.println("");
        //System.out.println("");
        return tiffFilePath;
    }
    /**
     * Extracts a zip entry (file entry)
     * @param zipIn
     * @param filePath
     * @throws IOException
     */
    private void extractFile(ZipInputStream zipIn, String filePath) throws IOException {
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));
        byte[] bytesIn = new byte[BUFFER_SIZE];
        int read = 0;
        while ((read = zipIn.read(bytesIn)) != -1) {
            bos.write(bytesIn, 0, read);
        }
        bos.close();
    }

}
