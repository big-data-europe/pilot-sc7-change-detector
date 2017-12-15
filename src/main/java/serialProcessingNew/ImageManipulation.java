package serialProcessingNew;

import java.awt.Rectangle;
import java.awt.image.Raster;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeSet;

import javax.media.jai.TiledImage;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPoint;

public class ImageManipulation {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String filesPath = "C:\\Users\\Giorgos\\Desktop\\BDE";
		String inputFileName = "cd2F5Avs82A5_noThres.tif";
		File inputFile = new File(filesPath, inputFileName);
		
		ImageManipulation im = new ImageManipulation();
		im.driver(inputFile);

	}
	
	public void driver(File imgAsFile) throws IOException {
		MyRead myRead = new MyRead(imgAsFile, "read");
		SerialProcessor sp = new SerialProcessor();
		String[] selPol = null;		// or new String[]{"VH", "VV"};
		sp.getBufferedImage(myRead, selPol);
		Product targetProduct = myRead.getTargetProduct();
		Band targetBand = targetProduct.getBandAt(0);
		TiledImage inputImg = (TiledImage)targetBand.getSourceImage().getImage(0);
		int imageWidth = inputImg.getWidth();
		int imageHeight = inputImg.getHeight();
		System.out.println("\n\nWidth=" + imageWidth + ", Height=" + imageHeight);
		Raster raster = inputImg.getData(new Rectangle(0, 0, imageWidth, imageHeight));
		visualizePoints(targetProduct, imageWidth, imageHeight);
//		readCertainPixels(raster);
		
	}
	
	public void readCertainPixels(Raster raster) {
		int width = raster.getWidth();
		int height = raster.getHeight();
		System.out.println("\n\nWidth=" + width + ", Height=" + height);
		for (int i = 150; i < 160; i++) {
			for (int j = 200; j < 210; j++) {
				double pixelValue = raster.getSampleDouble(i, j, 0);
				System.out.println("Pixel: " + i + "-" +  j + "\t" + pixelValue);
			}
		}
	}
	
	public void visualizePoints(Product product, int width, int height) {
		GeometryFactory geomFact = new GeometryFactory();
		ArrayList<Coordinate> allCoordinates = new ArrayList<>();
		TreeSet<Coordinate> clearAllCoordinates = new TreeSet<>();
		TreeSet<Double> xs = new TreeSet<>();
		TreeSet<Double> ys = new TreeSet<>();
		final StringBuilder sb = new StringBuilder();
		sb.append("MULTIPOINT (");
		for (int i = 0; i < width; i++) {
			for (int j = 0; j < height; j++) {
				String pixelInfoString = product.createPixelInfoString(i, j);
//				System.out.println(pixelInfoString);
				//Original CD-result:
				String[] cordinates = new String[2];
				cordinates[0] = pixelInfoString.split("Latitude:")[1].split("\\t")[1];
				cordinates[1] = pixelInfoString.split("Longitude:")[1].split("\\t")[1];
//				System.out.println(i + "-" + j + "\t" + cordinates[1] + " " + cordinates[0]);
				double x = MyUtils.compassToDecimal(cordinates[1]);
				double y = MyUtils.compassToDecimal(cordinates[0]);
//				xs.add(Double.parseDouble(cordinates[1]));
//				ys.add(Double.parseDouble(cordinates[0]));
//				allCoordinates.add(new Coordinate(Double.parseDouble(cordinates[1]), Double.parseDouble(cordinates[0])));
				xs.add(x);
				ys.add(y);
				allCoordinates.add(new Coordinate(x, y));
				clearAllCoordinates.add(new Coordinate(x, y));
//				points.add(new Point());
				//if (i % 100 == 0 && j % 100 == 0) {
				//	sb.append(cordinates[1] + " " + cordinates[0] + ",");
				//}
//				if (i == 0 && j == 0) sb.append(cordinates[1] + " " + cordinates[0] + ",");
//				else if (i == 0 && j == 378) sb.append(cordinates[1] + " " + cordinates[0] + ",");
//				else if (i == 458 && j == 378) sb.append(cordinates[1] + " " + cordinates[0] + ",");
//				else if (i == 458 && j == 0) sb.append(cordinates[1] + " " + cordinates[0] + ",");
				if (i == 0 && j == 0) sb.append(MyUtils.compassToDecimal(cordinates[1]) + " " + MyUtils.compassToDecimal(cordinates[0]) + ",");
				else if (i == 0 && j == 315) sb.append(MyUtils.compassToDecimal(cordinates[1]) + " " + MyUtils.compassToDecimal(cordinates[0]) + ",");
				else if (i == 480 && j == 315) sb.append(MyUtils.compassToDecimal(cordinates[1]) + " " + MyUtils.compassToDecimal(cordinates[0]) + ",");
				else if (i == 480 && j == 0) sb.append(MyUtils.compassToDecimal(cordinates[1]) + " " + MyUtils.compassToDecimal(cordinates[0]) + ",");
				//Argyros' CD-result:
//				String[] cordinates = new String[2];
//				cordinates[0] = pixelInfoString.split("Latitude:")[1].split("\\t")[1];
//				cordinates[1] = pixelInfoString.split("Longitude:")[1].split("\\t")[1];
//				double longtitude = MyUtils.compassToDecimal(cordinates[1]);
//				double latitude = MyUtils.compassToDecimal(cordinates[0]);
//				System.out.println(i + "-" + j + "\t" + longtitude + " " + latitude);
			}
		}
		System.out.println("all pixels are = " + allCoordinates.size());
		System.out.println("all geocoordinates are = " + clearAllCoordinates.size());
		Coordinate[] allCoords = allCoordinates.toArray(new Coordinate[allCoordinates.size()]);
		MultiPoint mPoint = geomFact.createMultiPoint(allCoords);
		Geometry multiPointEnv = mPoint.getEnvelope();
		double minX = multiPointEnv.getEnvelopeInternal().getMinX();
		double maxX = multiPointEnv.getEnvelopeInternal().getMaxX();
		double minY = multiPointEnv.getEnvelopeInternal().getMinY();
		double maxY = multiPointEnv.getEnvelopeInternal().getMaxY();
		Coordinate left = new Coordinate();
		Coordinate right = new Coordinate();
		Coordinate up = new Coordinate();
		Coordinate down = new Coordinate();
		for (Coordinate coord : allCoordinates) {
			if (coord.x == minX) {
				left = coord;
				System.out.println("Found left");
			}
			if (coord.x == maxX) {
				right = coord;
				System.out.println("Found right");
			}
			if (coord.y == minY) {
				down = coord;
				System.out.println("Found down");
			}
			if (coord.y == maxY) {
				up = coord;
				System.out.println("Found up");
			}
		}
		System.out.println("left: " + left.toString());
		System.out.println("right: " + right.toString());
		System.out.println("down: " + down.toString());
		System.out.println("up: " + up.toString());
		System.out.println("\nEnvelope of points: " + multiPointEnv.toString());
		System.out.println(sb);
		System.out.println("maxX = " + xs.last().toString());
		System.out.println("minX = " + xs.first().toString());
		System.out.println("maxY = " + ys.last().toString());
		System.out.println("minY = " + ys.first().toString());
	}

}
