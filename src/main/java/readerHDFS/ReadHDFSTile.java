package readerHDFS;

import java.awt.Rectangle;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.esa.snap.core.datamodel.ProductData;

import tileBased.model.MyTile;

public class ReadHDFSTile {
	private HDFSReader reader;
	public ReadHDFSTile(String path) {
		Path fileHDFSPath = new Path(path);
		Configuration conf = new Configuration();

		FSDataInputStream instream;
		ImageInputStream imgInStream = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(path), conf);
			instream = fs.open(fileHDFSPath);
			imgInStream = ImageIO.createImageInputStream(instream);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Iterator<ImageReader> imgIt = ImageIO.getImageReaders(imgInStream);
		ImageReader imgReader = imgIt.next();
		imgReader.setInput(imgInStream);

		reader = new HDFSReader(imgReader);
	}

	public void readTile(MyTile tile, BandInfo bandInfo) {

		ProductData dataBuffer = tile.getRawSamples();
		Rectangle rectangle = tile.getRectangle();
		try {
			reader.readBandRasterData(bandInfo, rectangle.x, rectangle.y, rectangle.width, rectangle.height, dataBuffer,
					null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		tile.setRawSamples(dataBuffer);
		//ProductData dataBuffer2 = tile.getRawSamples();
		//System.out.println(dataBuffer2.getClass());
		// tile.setRawSamples(dataBuffer);
		// System.out.println("whatever");
	}
//	public static void main(String[] args) throws IOException {
//	// TODO Auto-generated method stub
//	String dirHDFS = args[0];
//	String masterZipFilePath = args[1];
//	// String slaveZipFilePath = args[2];
//	// String outFile = args[3];
//
//	ZipHandler2 masterZipHandler = new ZipHandler2();
//	String masterTiffInHDFS = "";
//	try {
//		masterTiffInHDFS = masterZipHandler.tiffToHDFS(masterZipFilePath, dirHDFS);
//	} catch (IOException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
//
//	
//	Band masterBand = masterTargetProduct.getBandAt(0);
//
//	System.out.println("EXTRACTING BI FROM HDFS");
//	Path fileHDFSPath = new Path(masterTiffInHDFS);
//	Configuration conf = new Configuration();
//	FileSystem fs = FileSystem.get(URI.create(masterTiffInHDFS), conf);
//	FSDataInputStream instream = fs.open(fileHDFSPath);
//	ImageInputStream imgInStream = ImageIO.createImageInputStream(instream);
//	Iterator<ImageReader> imgIt = ImageIO.getImageReaders(imgInStream);
//	ImageReader imgReader = imgIt.next();
//	imgReader.setInput(imgInStream);
//
//	HDFSReader reader = new HDFSReader(imgReader);
//	LoopLimits limits = new LoopLimits(masterTargetProduct);
//	for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
//		for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
//			GetTile getTile = new GetTile(masterBand, null);
//			Tile tile = getTile.createTile(tileX, tileY);
//			ProductData dataBuffer1 = tile.getRawSamples();
//			Rectangle rectangle = tile.getRectangle();
//			//reader.readBandRasterData(rectangle.x, rectangle.y, rectangle.width, rectangle.height, dataBuffer1,
//				//	null);
//			tile.setRawSamples(dataBuffer1);
//			ProductData dataBuffer2 = tile.getRawSamples();
//			System.out.println(dataBuffer2.getClass());
//			// tile.setRawSamples(dataBuffer);
//			// System.out.println("whatever");
//		}
//	}
//	// BufferedImage bi = imgReader.readTile(0, 0, 0);
//	// System.out.println(bi.getClass());
//	System.out.println("BI IS EXTRACTED");
//
//}
}
