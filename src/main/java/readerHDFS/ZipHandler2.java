package readerHDFS;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.esa.snap.core.datamodel.Product;
import serialProcessingNew.MyRead;
public class ZipHandler2 {
	
    public String tiffToHDFS(String zipLocalFilePath, String dirHDFS) throws IOException {
    	String tiffLocalFilePath = "";
    	String tiffInHDFS = "";
    	TiffUnzipper unzipper = new TiffUnzipper();
        try {
        	tiffLocalFilePath = unzipper.unzip(zipLocalFilePath);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        //System.out.println("THE LOCAL PATH OF TIFF IS:");
        //System.out.println(tiffLocalFilePath);
        tiffInHDFS = tiffLocalToHDFS(tiffLocalFilePath, dirHDFS);
        //System.out.println("THE HDFS-PATH OF THE TIFF IS:");
        //System.out.println(tiffInHDFS);
        return tiffInHDFS;
    	
    }
    
    private String tiffLocalToHDFS(String tiffLocalFilePath, String HDFSdir) throws IOException {
    	//System.out.println("");
    	System.out.println("~~~ Initiating Local To HDFS ~~~");
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
    	System.out.println("~~~ Initiating Extract TP ~~~");
		File zipFile = new File(zipLocalFilePath);		
		//Product targetProduct = processImages2(zipFile);
		MyRead myRead = null;
		Product targetProduct = null;
		try {
			System.out.println("in zip handler problem 1");;
			myRead = new MyRead(zipFile, "read");
			System.out.println("in zip handler problem 2");;
			targetProduct = myRead.getTargetProduct();
			System.out.println("in zip handler problem 3");;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return targetProduct;
    }

}
