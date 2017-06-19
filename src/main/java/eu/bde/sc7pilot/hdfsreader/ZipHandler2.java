package eu.bde.sc7pilot.hdfsreader;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.esa.snap.core.datamodel.Product;
import eu.bde.sc7pilot.taskbased.MyRead;

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
        tiffInHDFS = tiffLocalToHDFS(tiffLocalFilePath, dirHDFS);
        return tiffInHDFS;

    }

    public String tiffLocalToHDFS(String tiffLocalFilePath, String HDFSdir) {
        System.out.println("Storing local file To HDFS...");
        long startTime = System.currentTimeMillis();
        
        InputStream instream = null;
        FileSystem fs = null;
        OutputStream out = null;
		try {
			instream = new BufferedInputStream(new FileInputStream(tiffLocalFilePath));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        //Get configuration of Hadoop system
        Configuration conf = new Configuration();
        //Creating tiff's place in HDFS
        File tiffFile = new File(tiffLocalFilePath);
        String tiffInHDFS = HDFSdir + File.separator + tiffFile.getName();
		try {
			fs = FileSystem.get(URI.create(tiffInHDFS), conf);
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			out = fs.create(new Path(tiffInHDFS));
		} 
		catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        //Copy file from local to HDFS
        try {
			IOUtils.copyBytes(instream, out, 65536, true);
		} 
        catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        System.out.println("\n[Dev's MSG]\t" + duration + " ms, for storing " + tiffLocalFilePath + " to HDFS\n");
        return tiffInHDFS;

    }

    public Product findTargetProduct(String zipLocalFilePath) {
        System.out.println("Extracting TargetProduct...");
        File zipFile = new File(zipLocalFilePath);
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
