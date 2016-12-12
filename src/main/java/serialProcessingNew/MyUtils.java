package serialProcessingNew;

import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import org.esa.snap.core.datamodel.GeoCoding;


public class MyUtils {

	public static void main(String[] args) throws IOException {
		
		SerialProcessor processor = new SerialProcessor();
		String[] selectedPolarisations=null;
		
		//String sourceFilePath = "/home/gvastakis/Desktop/sentinel-images-subsets/subset2_of_S1A_IW_GRDH_1SSV_20141225T142407_20141225T142436_003877_004A54_040F.dim";
		String sourceFilePath = "/home/gvastakis/Desktop/new-images/5/S1A_S6_GRDH_1SDV_20160815T214331_20160815T214400_012616_013C9D_2495.zip";
		File sourceFile = new File(sourceFilePath);
		
		MyRead myRead = new MyRead(sourceFile, "read");
		processor.getBufferedImage(myRead,selectedPolarisations);
				
		Product readProduct = myRead.getTargetProduct();
		
//		int x1 = 0;
//		int y1 = 0;
//		int x2 = 7000;
//		int y2 = 7000;
		
		int x1 = 2500;
		int y1 = 3000;
		int x2 = 5200;
		int y2 = 6400;
		
		String polygon = MyUtils.pixelsToPolygon(readProduct,x1,y1,x2,y2);
		System.out.println(polygon);
		
		Envelope env = MyUtils.geometryToBounds(polygon);
		System.out.println("NorthLat : " + env.getMinY());
		System.out.println("WestLon  : " + env.getMinX());
		System.out.println("SouthLat : " + env.getMaxY());
		System.out.println("EastLon  : " + env.getMaxX());
	
		
		System.out.println(MyUtils.pixelToGeoLocation(readProduct,x1,y1));
		System.out.println(MyUtils.pixelToGeoLocation(readProduct,x2,y2));
		
		GeoPos gp = MyUtils.pixelToGeoPos(readProduct,x1,y1);
		System.out.println(gp.getLon() + "," + gp.getLat());
		
		String[] coords = new String[2];
		coords = MyUtils.parsePixelInfoString(readProduct,x1,y1);
		System.out.println(coords[0] + "," + coords[1]);
		
	}
	
	public static Envelope geometryToBounds(String polygon) {
		
		WKTReader wktReader = new WKTReader();
		Geometry geometry = null;
		try {
			geometry = wktReader.read(polygon);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return geometry.getEnvelopeInternal();
		
	}
	
	public static String pixelsToPolygon(Product prod,int minX,int minY,int maxX,int maxY) throws IOException {
		
		if(minX>maxX || minY>maxY) {
			throw new IOException("Error:Give pixels in correct order");
		}
		
		if(prod.containsPixel(minX,minY)==false || prod.containsPixel(maxX,maxY)==false) {
			throw new IOException("Error:Pixels not contained in product");
		}
		
		GeoCoding gc = prod.getSceneGeoCoding();
		
		GeoPos topLeftCorner = gc.getGeoPos(new PixelPos(minX,minY),null);
		GeoPos downRightCorner = gc.getGeoPos(new PixelPos(maxX,maxY),null);
	
		GeoPos topRightCorner = gc.getGeoPos(new PixelPos(maxX,minY),null);
		GeoPos downLeftCorner = gc.getGeoPos(new PixelPos(minX,maxY),null);
		
		return new String("POLYGON (("
				+ topLeftCorner.getLon() + " " + topLeftCorner.getLat() + ", "
				+ topRightCorner.getLon() + " " + topRightCorner.getLat() + ", "
				+ downRightCorner.getLon() + " " + downRightCorner.getLat() + ", "
				+ downLeftCorner.getLon() + " " + downLeftCorner.getLat() + ", "
				+ topLeftCorner.getLon() + " " + topLeftCorner.getLat()
				+"))");
		
	}
	
	public static String pixelToGeoLocation(Product prod,int x,int y) throws IOException {
		
		if(prod.containsPixel(x,y)==false) {
			throw new IOException("Error:Pixel not contained in product");
		}
		
		GeoCoding gc = prod.getSceneGeoCoding();
		GeoPos geoPosition = gc.getGeoPos(new PixelPos(x,y), null);
				
		return new String("POINT ("
				+ geoPosition.getLon() + " " + geoPosition.getLat()
				+")");
		
	}
	
	// instead of returning a String return a GeoPos object
	public static GeoPos pixelToGeoPos(Product prod,int x,int y) throws IOException {
		
		if(prod.containsPixel(x,y)==false) {
			throw new IOException("Error:Pixel not contained in product");
		}
		
		GeoCoding gc = prod.getSceneGeoCoding();
		return gc.getGeoPos(new PixelPos(x,y), null);
				
	}
	
	// for backwards compatibility reason only : should be removed after contacting Mano
	public static String[] parsePixelInfoString(Product prod,int x,int y) throws IOException {
		
		GeoCoding gc = prod.getSceneGeoCoding();
		GeoPos geoPosition = gc.getGeoPos(new PixelPos(x,y), null);
		String[] cordinates = new String[2];
		// returning coordinates in reverse order from pixelToGeoPos
		cordinates[0] = String.valueOf(geoPosition.getLat());
		cordinates[1] = String.valueOf(geoPosition.getLon());
		return cordinates;
		
	}
	
	
	/*
	public static String pixelsToPolygon(Product prod,int minX,int minY,int maxX,int maxY) throws IOException {
		
		if(minX>maxX || minY>maxY) {
			throw new IOException("Error:Give pixels in correct order");
		}
		
		if(prod.containsPixel(minX,minY)==false || prod.containsPixel(maxX,maxY)==false) {
			throw new IOException("Error:Pixels not contained in product");
		}
		
		String[] topLeftCorner  = parsePixelInfoString(prod,minX,minY);
		String[] downRightCorner = parsePixelInfoString(prod,maxX,maxY);
		
		String[] topRightCorner  = parsePixelInfoString(prod,maxX,minY);
		String[] downLefttCorner = parsePixelInfoString(prod,minX,maxY);
				
		return new String("POLYGON (("
				+ topLeftCorner[1] + " " + topLeftCorner[0] + ", "
				+ topRightCorner[1] + " " + topRightCorner[0] + ", "
				+ downRightCorner[1] + " " + downRightCorner[0] + ", "
				+ downLefttCorner[1] + " " + downLefttCorner[0] + ", "
				+ topLeftCorner[1] + " " + topLeftCorner[0]
				+"))");
		
	}
	
	public static String pixelToGeoLocation(Product prod,int x,int y) throws IOException {
		
		if(prod.containsPixel(x,y)==false) {
			throw new IOException("Error:Pixel not contained in product");
		}
		
		String[] geoCoordinates  = parsePixelInfoString(prod,x,y);
				
		return new String("POINT ("
				+ geoCoordinates[1] + " " + geoCoordinates[0]
				+")");
		
	}
	
	public static String[] parsePixelInfoString(Product prod,int x,int y) throws IOException {
		String pixelInfoString = prod.createPixelInfoString(x,y);
		//System.out.println(pixelInfoString);
		String[] cordinates = new String[2];
		cordinates[0] = pixelInfoString.split("latitude:")[1].split("\\t")[1];
		cordinates[1] = pixelInfoString.split("longitude:")[1].split("\\t")[1];
		return cordinates;
		
	}
	*/

}
