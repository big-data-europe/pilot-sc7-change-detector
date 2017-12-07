package serialProcessingNew;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

import org.esa.snap.core.datamodel.Product;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class MyUtils {

	public static void main(String[] args) throws IOException {
		
		//Giannos's things
//		SerialProcessor processor = new SerialProcessor();
//		String[] selectedPolarisations=null;
//		String sourceFilePath = "/home/gvastakis/Desktop/sentinel-images-subsets/subset2_of_S1A_IW_GRDH_1SSV_20141225T142407_20141225T142436_003877_004A54_040F.dim";
//		File sourceFile = new File(sourceFilePath);
//		MyRead myRead = new MyRead(sourceFile, "read");
//		processor.getBufferedImage(myRead,selectedPolarisations);	
//		Product readProduct = myRead.getTargetProduct();
//		System.out.println(MyUtils.pixelsToPolygon(readProduct,2000,3000,8000,9000));
//		System.out.println(MyUtils.pixelToGeoLocation(readProduct,2000,3000));
		
		//Meine things
		MyUtils.pointToSomething();

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
	
	public static String pixelToGeoLocation(Product prod, int x, int y) throws IOException {
		if (prod.containsPixel(x,y) == false) {
			throw new IOException("Error:Pixel not contained in product");
		}
		String[] geoCoordinates  = parsePixelInfoString(prod,x,y);		
		return new String(geoCoordinates[1] + " " + geoCoordinates[0]);
	}
	
	public static String[] parsePixelInfoString(Product prod,int x,int y) throws IOException {
		String pixelInfoString = prod.createPixelInfoString(x,y);
//		System.out.println("pixelInfoString:\n" + pixelInfoString + "\nend of info");
		//System.out.println(pixelInfoString);
		String[] cordinates = new String[2];
		cordinates[0] = pixelInfoString.split("Latitude:")[1].split("\\t")[1];
		cordinates[1] = pixelInfoString.split("Longitude:")[1].split("\\t")[1];
		System.out.println(cordinates[0]);
		System.out.println(cordinates[1]);
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
			System.out.println(rationalizedGeoms.size() + " Geometries will be returned.");
			return rationalizedGeoms;
		}
		else {
			System.out.println(inputGeometries.size() + " (are all input) Geometries that are returned.");
			return inputGeometries;
		}
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
	
	//Utils for creating polygon from points.
	public static Polygon pointsToSomething(TreeSet<Coordinate> listOfCoordinates) throws ParseException {
		GeometryFactory geomFact = new GeometryFactory();
		Coordinate[] coords = new Coordinate[listOfCoordinates.size()];
		Iterator<Coordinate> itrCoor = listOfCoordinates.iterator();
		int j = 0;
		while(itrCoor.hasNext()) {
			coords[j] = itrCoor.next();
			j++;
		}
//		for (int i = 0; i < coords.length; i++) System.out.println(coords[i].toString());
		MultiPoint mPoint = geomFact.createMultiPoint(coords);
		System.out.println(mPoint.toString());
//		LineString line = geomFact.createLineString(coords);
//		System.out.println(line.toString());
//		LinearRing lRing = geomFact.createLinearRing(coords);
//		System.out.println(lRing.toString());
//		Polygon polyg = geomFact.createPolygon(coords);
//		System.out.println(polyg.toString());
		Geometry multiPointEnv = mPoint.getEnvelope();
		System.out.println(multiPointEnv.toString());
		
		// Attempt to use CenterPoint as basis.
//		Coordinate centerCoord = mPoint.getCentroid().getCoordinate();
//		System.out.println(centerCoord.toString());
//		TreeMap<Double, Coordinate> leftDownCoords = new TreeMap<Double, Coordinate>(Collections.reverseOrder());
//		TreeMap<Double, Coordinate> rightDownCoords = new TreeMap<Double, Coordinate>(Collections.reverseOrder());
//		TreeMap<Double, Coordinate> rightUpCoords = new TreeMap<Double, Coordinate>(Collections.reverseOrder());
//		TreeMap<Double, Coordinate> leftUpCoords = new TreeMap<Double, Coordinate>(Collections.reverseOrder());
//		for (int k = 0; k < coords.length; k++) {
//			double centerX = centerCoord.x;
//			double centerY = centerCoord.y;
//			if (coords[k].x < centerX && coords[k].y < centerY) {
//				double distance = centerCoord.distance(coords[k]);
//				leftDownCoords.put(distance, coords[k]);
//			}
//			else if (coords[k].x > centerX && coords[k].y < centerY) {
//				double distance = centerCoord.distance(coords[k]);
//				rightDownCoords.put(distance, coords[k]);
//			}
//			else if (coords[k].x > centerX && coords[k].y > centerY) {
//				double distance = centerCoord.distance(coords[k]);
//				rightUpCoords.put(distance, coords[k]);
//			}
//			else {
//				double distance = centerCoord.distance(coords[k]);
//				leftUpCoords.put(distance, coords[k]);
//			}
//		}
//		System.out.println("\nleftDownCoords");
//		for(double key : leftDownCoords.keySet()) {
//			System.out.println(key + "\t" + leftDownCoords.get(key).toString());
//		}
//		System.out.println("\nrightDownCoords");
//		for(double key : rightDownCoords.keySet()) {
//			System.out.println(key + "\t" + rightDownCoords.get(key).toString());
//		}
//		System.out.println("\nrightUpCoords");
//		for(double key : rightUpCoords.keySet()) {
//			System.out.println(key + "\t" + rightUpCoords.get(key).toString());
//		}
//		System.out.println("\nleftUpCoords");
//		for(double key : leftUpCoords.keySet()) {
//			System.out.println(key + "\t" + leftUpCoords.get(key).toString());
//		}
		
		
		Coordinate downPoint = new Coordinate();
		Coordinate rightPoint = new Coordinate();
		Coordinate upPoint = new Coordinate();
		Coordinate leftPoint = new Coordinate();
		Boolean findDownPoint = true;
		Boolean findRightPoint = true;
		Boolean findUpPoint = true;
		Boolean findLeftPoint = true;
		ArrayList<Coordinate> coordsInAList = new ArrayList<>();
		for (int k = 0; k < coords.length; k++) coordsInAList.add(coords[k]);
//		double minX = multiPointEnv.getEnvelopeInternal().getMinX();
//		double minY = multiPointEnv.getEnvelopeInternal().getMinY();
//		double maxX = multiPointEnv.getEnvelopeInternal().getMaxX();
//		double maxY = multiPointEnv.getEnvelopeInternal().getMaxY();
		TreeSet<Double> xCoords = new TreeSet<>();
		TreeSet<Double> yCoords = new TreeSet<>();
		for (int k = 0; k < coords.length; k++) {
			xCoords.add(coords[k].x);
			yCoords.add(coords[k].y);
		}
		ArrayList<Coordinate> finalCoords = new ArrayList<>();
		
		for (int k = 0; k < coords.length; k++) {
			if (findLeftPoint && coords[k].x == xCoords.first()){
				leftPoint = coords[k];
				finalCoords.add(coords[k]);
				findLeftPoint = false;
				if (xCoords.contains(coords[k].x)) {
					xCoords.remove(coords[k].x);
				}
				if (yCoords.contains(coords[k].y)) {
					yCoords.remove(coords[k].y);
				}
			}
		}
		for (int k = 0; k < coords.length; k++) {
			if (findRightPoint && coords[k].x == xCoords.last()){
				rightPoint = coords[k];
				finalCoords.add(coords[k]);
				findRightPoint = false;
				if (xCoords.contains(coords[k].x)) {
					xCoords.remove(coords[k].x);
				}
				if (yCoords.contains(coords[k].y)) {
					yCoords.remove(coords[k].y);
				}
			}
		}
		for (int k = 0; k < coords.length; k++) {
			if (findDownPoint && coords[k].y == yCoords.first()){
				downPoint = coords[k];
				finalCoords.add(coords[k]);
				findDownPoint = false;
				if (xCoords.contains(coords[k].x)) {
					xCoords.remove(coords[k].x);
				}
				if (yCoords.contains(coords[k].y)) {
					yCoords.remove(coords[k].y);
				}
			}
		}
		for (int k = 0; k < coords.length; k++) {
			if (findUpPoint && coords[k].y == yCoords.last()){
				upPoint = coords[k];
				finalCoords.add(coords[k]);
				findUpPoint = false;
				if (xCoords.contains(coords[k].x)) {
					xCoords.remove(coords[k].x);
				}
				if (yCoords.contains(coords[k].y)) {
					yCoords.remove(coords[k].y);
				}
			}
		}
		Coordinate[] visualCoords = new Coordinate[] {
														downPoint,
														rightPoint,
														upPoint,
														leftPoint,
														downPoint
													};
		Polygon visualPol = geomFact.createPolygon(visualCoords);
		System.out.println(visualPol.toString());
		WKTReader wkt = new WKTReader();
		Geometry theGeom = wkt.read("POLYGON ((36.32909148931503 32.295656182991735,36.33035749197006 32.295662984824176,36.33036553859711 32.29480141532075,36.32909685373306 32.29479688072278,36.32909148931503 32.295656182991735))");
		if (theGeom.contains(visualPol)) System.out.println("AYTO");
		return visualPol;
	}
	
	public static void pointToSomething() {
		GeometryFactory geomFact = new GeometryFactory();
		Coordinate[] coords = new Coordinate[] {
												new Coordinate(36.331886, 32.298294),
												new Coordinate(36.331867, 32.298386),
												new Coordinate(36.331974, 32.2984),
												new Coordinate(36.331955, 32.298492),
												new Coordinate(36.332077, 32.29842)
												};
		LineString line = geomFact.createLineString(coords);
		System.out.println(line.toString());
	}
	
	public static Coordinate pixelToGeoCoordinates(Product prod, int x, int y) throws IOException {
		Coordinate coords = new Coordinate();
		if (prod.containsPixel(x,y) == false) {
			throw new IOException("Error:Pixel not contained in product");
		}
		System.out.println(x);
		System.out.println(y);
		String[] geoCoordinates  = parsePixelInfoString(prod, x, y);
		if (geoCoordinates.length < 2) {
			System.out.println("Something is bad with converting points to GeoCoordinates");
			return coords;
		}
		else {
			coords.setOrdinate(0, compassToDecimal(geoCoordinates[1]));		//for Argyros' Change-Detected
			coords.setOrdinate(1, compassToDecimal(geoCoordinates[0]));		//for Argyros' Change-Detected
//			coords.setOrdinate(0, Double.parseDouble(geoCoordinates[1]));	//for original Change-Detected
//			coords.setOrdinate(1, Double.parseDouble(geoCoordinates[0]));	// for original Change-Detected
		}
		return coords; 
	}
	
	public static Double compassToDecimal(String rawCompassCoord) {
		if (String.valueOf(rawCompassCoord.charAt(0)).equals("-")) {
			String clear = rawCompassCoord.replaceAll("[^A-Za-z0-9]", " ");
			String[] parts = clear.split(" ");
			double d  = Double.parseDouble(parts[1]);
			double m  = Double.parseDouble(parts[2]);
			double s  = Double.parseDouble(parts[3]);
			double dd = - (Math.abs(d) + (m / 60.0) + (s / 3600.0));
			System.out.println(dd);
			return dd;
		}
		else {
			String clear = rawCompassCoord.replaceAll("[^A-Za-z0-9]", " ");
			String[] parts = clear.split(" ");
			double d  = Double.parseDouble(parts[0]);
			double m  = Double.parseDouble(parts[1]);
			double s  = Double.parseDouble(parts[2]);
			double dd = (Math.abs(d) + (m / 60.0) + (s / 3600.0));
			System.out.println(dd);
			return dd;
		}
	}

}