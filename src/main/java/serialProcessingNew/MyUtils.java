package serialProcessingNew;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

import org.esa.snap.core.datamodel.Product;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

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
	
	// Utils for checking DBScan result
	public static ArrayList<Geometry> clearingGeoms(ArrayList<Geometry> inputGeometries){
		ArrayList<Geometry> trimedGeoms = new ArrayList<>();
		TreeSet<Integer> prosecuted = new TreeSet<>();
		HashMap<Integer, ArrayList<Integer>> fullComplaintsList = new HashMap<>();
		for(int i = 0; i < inputGeometries.size() - 1; i++){
			if(!prosecuted.contains(i)){
				Geometry primeGeometry = inputGeometries.get(i);
				ArrayList<Integer> primeGeomComplaints = new ArrayList<>();
				for(int j = i + 1; j < inputGeometries.size(); j++){
					if(primeGeometry.intersects(inputGeometries.get(j))){
						primeGeomComplaints.add(j);
					}
				}
				prosecuted.addAll(primeGeomComplaints);
				fullComplaintsList.put(i, primeGeomComplaints);
			}
			
		}
		// Uncomment it to be printed if something goes wrong!
//		for(int key : fullComplaintsList.keySet()) {
//			System.out.println("Key = " + key + "\nprosecutes:\t" + fullComplaintsList.get(key) + "\n");
//		}
		System.out.println("\n" + prosecuted + " geoms with these indexes will be trimmed.");
		for(int i = 0; i < inputGeometries.size(); i++){
			if(!prosecuted.contains(i)) {
				trimedGeoms.add(inputGeometries.get(i));
			}
		}
		System.out.println(inputGeometries.size() + " number of inp geoms");
		System.out.println(trimedGeoms.size() + " number of returned geoms");
		return trimedGeoms;
	}

	public static ArrayList<Geometry> keepGeomsWithin(ArrayList<Geometry> inputGeometries, Geometry referenceGeometry) {
		System.out.println("\nChecking if " + inputGeometries.size() + " geometries are inside the referenceGeometry:\n" + referenceGeometry);
		ArrayList<Geometry> withinGeometries = new ArrayList<>();
		int k = 0;
		for (int i = 0; i < inputGeometries.size(); i++) {
			if (inputGeometries.get(i).within(referenceGeometry)) {
				withinGeometries.add(inputGeometries.get(i));
				k++;
			}
		}
		System.out.println(k + "\tgeometries found withing referenceGeometry.");
		System.out.println(withinGeometries.size() + "\tgeometries must have returned.");
		return withinGeometries;	
	}

	public static ArrayList<Geometry> rationalizeGeoms(ArrayList<Geometry> inputGeometries, Geometry referenceGeometry){
		ArrayList<Geometry> rationalizedGeoms = new ArrayList<>();
		double referenceArea = referenceGeometry.getArea();
		int inputArrayListSize = inputGeometries.size();
		int randomNum = inputArrayListSize;
		if(referenceArea < 0.0000009){
			randomNum = ThreadLocalRandom.current().nextInt(5, 15);
			System.out.println("\n" + referenceArea + " is the referenceArea. Number of accepted results = " + randomNum);
		}
		else if(referenceArea < 0.00009){
			randomNum = ThreadLocalRandom.current().nextInt(15, 30);
			System.out.println("\n" + referenceArea + " is the referenceArea. Number of accepted results = " + randomNum);
		}
		else if(referenceArea < 0.009){
			randomNum = ThreadLocalRandom.current().nextInt(35, 55);
			System.out.println("\n" + referenceArea + " is the referenceArea. Number of accepted results = " + randomNum);
		}
		else if(referenceArea < 0.9){
			randomNum = ThreadLocalRandom.current().nextInt(65, 100);
			System.out.println("\n" + referenceArea + " is the referenceArea. Number of accepted results = " + randomNum);
		}
		else {
			System.out.println("\nVery large area selected.. leaving result that has " + inputArrayListSize + " results, as it is :)");
		}
		if(randomNum < inputArrayListSize){
			int cropNum = inputArrayListSize - randomNum;
			System.out.println(cropNum + " last geometries will be cropped from inputGeometries ArrayList");
			for(int i = 0; i < randomNum; i++){
				rationalizedGeoms.add(inputGeometries.get(i));
			}
		}
		System.out.println(rationalizedGeoms.size() + " Geometries will be returned.");
		return rationalizedGeoms;
	}

	public static ArrayList<Geometry> resultChecker(ArrayList<String> inputStringGeometries, String referenceStringGeometry) {
		ArrayList<Geometry> inputGeometries = new ArrayList<>();
		WKTReader wkt = new WKTReader();
		for(String stringGeom : inputStringGeometries){
			try {
				Geometry geometry = wkt.read(stringGeom);
				inputGeometries.add(geometry);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Problem with input Geometries!");
				return inputGeometries;
			}
		}
		Geometry referenceGeometry;
		ArrayList<Geometry> purifiedGeoms = new ArrayList<>();
		try {
			referenceGeometry = wkt.read(referenceStringGeometry);
			ArrayList<Geometry> geomsWithinReferenceGeom = keepGeomsWithin(inputGeometries, referenceGeometry);
			ArrayList<Geometry> sortedGeomsByArea = sortGeomsByArea(geomsWithinReferenceGeom);
			ArrayList<Geometry> clearedGeoms = clearingGeoms(sortedGeomsByArea);
			purifiedGeoms = rationalizeGeoms(clearedGeoms, referenceGeometry);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Problem with reference Geometry! The known as selectedPolygon is somewhat false.");
			ArrayList<Geometry> sortedGeomsByArea = sortGeomsByArea(inputGeometries);
			ArrayList<Geometry> clearedGeoms = clearingGeoms(sortedGeomsByArea);
			return clearedGeoms;
		}
		return purifiedGeoms;
	}
	
	public static ArrayList<Geometry> sortGeomsByArea(ArrayList<Geometry> inputGeometries) {
		ArrayList<Geometry> sortedByArea = new ArrayList<>();
		System.out.println("\n" + inputGeometries.size() + " Geometries to extract area from.");
		// Mapping Areas' geometries to Geometries + sorted by key(=== area!)
		TreeMap<Double, Geometry> dupleAreaGeom = new TreeMap<>();
		for (int i = 0; i < inputGeometries.size(); i++) {
			double area = inputGeometries.get(i).getArea();
			dupleAreaGeom.put(area, inputGeometries.get(i));
		}
		System.out.println(dupleAreaGeom.size() + " Geometries are put in TreeMap dupleGeomArea.");
		System.out.println(inputGeometries.size() - dupleAreaGeom.size() + " Geometries had the same area and kicked out of the final TreeMap.");
		for(double key : dupleAreaGeom.keySet()) {
//			System.out.println("Area = " + key + "\nin Geometry:\t" + dupleAreaGeom.get(key) + "\n");
			sortedByArea.add(dupleAreaGeom.get(key));
		}
		return sortedByArea;
	}

}