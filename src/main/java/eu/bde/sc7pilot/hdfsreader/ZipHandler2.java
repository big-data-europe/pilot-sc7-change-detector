package eu.bde.sc7pilot.hdfsreader;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.esa.snap.core.datamodel.Product;
import eu.bde.sc7pilot.taskbased.MyRead;

public class ZipHandler2 {

	public String tiffToHDFS(String zipLocalFilePath, String dirHDFS) throws IOException {
        ArrayList<String> tiffsLocalFilePaths = new ArrayList<String>();
        String tiffInHDFS = "";
        TiffUnzipper unzipper = new TiffUnzipper();
        try {
            System.out.println("~~~ Unzipping ~~~"); 
            double startUnzip = System.currentTimeMillis();
            
           	tiffsLocalFilePaths = unzipper.unzip(zipLocalFilePath);
           
            double endUnzip = System.currentTimeMillis();
            double unzipTime = endUnzip - startUnzip;
            System.out.println("Unzip made in: " + unzipTime + " ms");
 
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        
        if (tiffsLocalFilePaths.isEmpty()) {
        	System.out.println("*No tiff found!*");
        }
        else if (tiffsLocalFilePaths.size() == 1) {
        	System.out.println("*Only one tiff found, procceding as usual*");
        	tiffInHDFS = tiffLocalToHDFS(tiffsLocalFilePaths.get(0), dirHDFS);
        }
        else {
        	System.out.println("*More than one tiffs found, wait for file-manipulation*");
        	for (int i = 0; i < tiffsLocalFilePaths.size(); i++) {
        		String[] stringParts = tiffsLocalFilePaths.get(i).split("-");
        		int parts = stringParts.length;
            	if (stringParts[3].equals("vv") || stringParts[parts - 1].equals("001")){
                    System.out.println("~~~ Storing to HDFS ~~~"); 
                    double startStore = System.currentTimeMillis();
                    
            		tiffInHDFS = tiffLocalToHDFS(tiffsLocalFilePaths.get(i), dirHDFS);
                   
                    double endStore = System.currentTimeMillis();
                    double storeTime = endStore - startStore;
                    System.out.println("File stored in HDFS in: " + storeTime + " ms");
            	}
            	else {
            		File unwantedTiff = new File(tiffsLocalFilePaths.get(i));
            		if (unwantedTiff.exists()) {
            			unwantedTiff.delete();
            		}
            	}
        	}
    		File origZip = new File(zipLocalFilePath);
    		String origName = origZip.getName();
    		String parentName = origZip.getParent();
    		String origPath = origZip.getAbsolutePath();
    		String alterNamePath = origPath + "ORIG";
    		File alteredZip = new File(alterNamePath);
    		if(origZip.renameTo(alteredZip)) {
    	        System.out.println("");
    	    } 
    		else {
    	         System.out.println("Error");
    		}           
            
            Zip zip = new Zip();                    
            String[] nameParts = origName.split("\\.");
            
            String dirNameToZip = parentName + File.separator + nameParts[0];
            File dirToZip = new File(dirNameToZip);
            
            dirToZip.mkdir();                   
            String safeName = nameParts[0] + ".SAFE";
            String dirNameToCopy = dirNameToZip + ".SAFE";
            File dirToCopy = new File(dirNameToCopy);
            
            String dirNameHelp = dirNameToZip + File.separator + safeName;
            File dirToHelp = new File(dirNameHelp);
            dirToHelp.mkdir();
           
            FileUtils.copyDirectory(dirToCopy, dirToHelp);
                              
            String finalName = parentName + File.separator + nameParts[0] + ".zip";
            //System.out.println("Directory to be zipped:  " + dirToZip);
            //System.out.println("Final Zip's name:  " + finalName);            
            System.out.println("~~~ Compression Started ~~~"); 
            double startComp = System.currentTimeMillis();
            
            zip.compressDirectory(dirNameToZip, finalName);
           
            double endComp = System.currentTimeMillis();
            double compTime = endComp - startComp;
            System.out.println("Completed compression in: " + compTime + " ms");
        }
        return tiffInHDFS;

    }

    private String tiffLocalToHDFS(String tiffLocalFilePath, String HDFSdir) throws IOException {
        //System.out.println("");
        //System.out.println("~~~ Initiating Local To HDFS ~~~");
        //System.out.println("IMPORTING TO HDFS THE FILE:");
        //System.out.println(tiffLocalFilePath);
        //*** Copying the extracted tiff to HDFS
        //Tiff from local FS -> IpnutStream -> (target) HDFS
        InputStream instream = new BufferedInputStream(new FileInputStream(tiffLocalFilePath));
        //Get configuration of Hadoop system
        Configuration conf = new Configuration();
        //conf.set("fs.default.name", "hdfs://localhost:54310");
        //System.out.println("Connecting to -- " + conf.get("fs.defaultFS"));	//check code
        //Finding tiff's name
        //System.out.println("");
        //System.out.println("TIFF'S NAME:");
        File tiffFile = new File(tiffLocalFilePath);
        String tiffName = tiffFile.getName();
        //System.out.println(tiffName);
        //System.out.println("");
        //Defining tiff's destination in HDFS
        String tiffInHDFS = HDFSdir + File.separator + tiffName;
        //Creating tiff's place in HDFS
        FileSystem fs = FileSystem.get(URI.create(tiffInHDFS), conf);
        OutputStream out = fs.create(new Path(tiffInHDFS));
        //Copy file from local to HDFS
        IOUtils.copyBytes(instream, out, 65536, true);
        //check code
        //System.out.println("TIFF IS COPIED TO HDFS AND HAS THE HDFS-FILEPATH: ");
        //System.out.println(tiffInHDFS);
        //System.out.println("");
        //System.out.println("~~~ Completing Local To HDFS ~~~");
        //System.out.println("");
        //System.out.println("");
        return tiffInHDFS;

    }

    public Product findTargetProduct(String zipLocalFilePath) {
        //System.out.println("");
        //System.out.println("~~~ Initiating Extract TP ~~~");
        File zipFile = new File(zipLocalFilePath);
        //Product targetProduct = processImages2(zipFile);
        MyRead myRead = null;
        Product targetProduct = null;
        try {
            myRead = new MyRead(zipFile, "read");
            targetProduct = myRead.getTargetProduct();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return targetProduct;
    }

}
