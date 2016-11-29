package serialProcessingNew;

import java.io.File;
import java.io.IOException;

import org.esa.snap.core.datamodel.Product;

public class MyUtils {

	public static void main(String[] args) throws IOException {
		
		SerialProcessor processor = new SerialProcessor();
		String[] selectedPolarisations=null;
		
		String sourceFilePath = "/home/gvastakis/Desktop/sentinel-images-subsets/subset2_of_S1A_IW_GRDH_1SSV_20141225T142407_20141225T142436_003877_004A54_040F.dim";
		File sourceFile = new File(sourceFilePath);
		
		MyRead myRead = new MyRead(sourceFile, "read");
		processor.getBufferedImage(myRead,selectedPolarisations);
				
		Product readProduct = myRead.getTargetProduct();
		
		System.out.println(MyUtils.pixelsToPolygon(readProduct,2000,3000,8000,9000));
		
		System.out.println(MyUtils.pixelToGeoLocation(readProduct,2000,3000));

	}
	
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
				
		return new String(geoCoordinates[1] + " " + geoCoordinates[0]);
		
	}
	
	public static String[] parsePixelInfoString(Product prod,int x,int y) throws IOException {
		String pixelInfoString = prod.createPixelInfoString(x,y);
		//System.out.println(pixelInfoString);
		String[] cordinates = new String[2];
		cordinates[0] = pixelInfoString.split("latitude:")[1].split("\\t")[1];
		cordinates[1] = pixelInfoString.split("longitude:")[1].split("\\t")[1];
		return cordinates;
		
	}

}