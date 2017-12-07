package serialProcessingNew;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.Raster;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;

import javax.media.jai.TiledImage;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;

public class DBScanSerial {
	
	public ArrayList<Point> checkedCorePoints;
	
	public DBScanSerial() {
		this.checkedCorePoints = new ArrayList<>();
	}

	public static void main(String[] args) throws IOException, ParseException {
		// TODO Auto-generated method stub
//		String filesPath = args[0];
//		String inputFileName = args[1];
//		String outputFileName = args[2];
//		String referencePolygon = args[3];
//		double threshold = Double.parseDouble(args[4]);
		String filesPath = "/media/indiana/data/docker-inout";
		String inputFileName = "cd2F5Avs82A5_noThres.tif";
		String outputFileName = "giorgosDB2F5Avs82A5subset.txt";
		String referencePolygon = "POLYGON ((36.30629539489746 32.27764132766419, 36.30629539489746 32.30601070258387, 36.34946823120117 32.30601070258387, 36.34946823120117 32.27764132766419, 36.30629539489746 32.27764132766419))";
		double threshold = 2;
		File inputFile = new File(filesPath, inputFileName);
		File outputFile = new File(filesPath, outputFileName);
		DBScanSerial dbScan = new DBScanSerial();
		dbScan.clusterChanges(inputFile, outputFile, referencePolygon, threshold);
		
	}
	
	public void clusterChanges(File inputFile, File outputFile, String referencePolygon, double threshold) throws IOException, ParseException {
		MyRead myRead = new MyRead(inputFile, "read");
		SerialProcessor sp = new SerialProcessor();
//		String[] selPol = new String[]{"VH", "VV"};
		String[] selPol = null;
		sp.getBufferedImage(myRead, selPol);
		Product targetProduct = myRead.getTargetProduct();
		Band targetBand = targetProduct.getBandAt(0);
		TiledImage inputImg = (TiledImage)targetBand.getSourceImage().getImage(0);
		int imageWidth = inputImg.getWidth();
		int imageHeight = inputImg.getHeight();
		System.out.println("\n\nWidth=" + imageWidth + ", Height=" + imageHeight);
		Raster raster = inputImg.getData(new Rectangle(0, 0, imageWidth, imageHeight));//give dimensions
//		Raster raster = inputImg.getData();

		
		ArrayList<Point> pointsOverThreshold = getChangedPoints(raster, threshold);
		HashMap<Point, ArrayList<Point>> coreToReachablesMap = findCoreAndReachables(pointsOverThreshold, 3, 10);
		ArrayList<Point> corePoints = new ArrayList<Point>(coreToReachablesMap.keySet());
		System.out.println("\n\tsize of corePoints = " + corePoints.size());
		ArrayList<ArrayList<Point>> clusterCores = clusteringCorePoints(corePoints, threshold);
		System.out.println("\n\nsize of clusterCores = " + clusterCores.size());
		int i = 0;
		for (ArrayList<Point> clusterCore : clusterCores) {
			i++;
			System.out.println("\nCluster of core Points No.: " + i);
			for (Point point : clusterCore) System.out.println(point.toString());
		}
		
		ArrayList<ArrayList<Point>> theClusters = new ArrayList<>();
		for (ArrayList<Point> coreCluster : clusterCores) {
			ArrayList<Point> cluster = new ArrayList<>();
			for (Point corePoint : coreCluster) {
				cluster.addAll(coreToReachablesMap.get(corePoint));
			}
			theClusters.add(cluster);
		}
		System.out.println("\n\nsize of theClusters = " + theClusters.size());
		for (int j = 0; j < theClusters.size(); j++) System.out.println("size of cluster" + j + " = " + theClusters.get(j).size());
		ArrayList<Polygon> visualPolygons = new ArrayList<>();
		for (int k = 0; k < theClusters.size(); k++) {
			TreeSet<Coordinate> clusterOfCoordinates = pointToCoord(theClusters.get(k), targetProduct);
			visualPolygons.add(MyUtils.pointsToSomething(clusterOfCoordinates));
		}
//		TreeSet<Coordinate> clusterOfCoordinates = pointToCoord(theClusters.get(0), targetProduct);
//		MyUtils.pointsToSomething(clusterOfCoordinates);
		System.out.println("\n\n\tFINAL RESULT:");
		for (Polygon vPol : visualPolygons) System.out.println(vPol.toString());
	}
	
	private TreeSet<Coordinate> pointToCoord(ArrayList<Point> points, Product product) throws IOException {
//		String multiPoint = "MULTIPOINT ((";
//		for (Point po : points) {
//			String coords = (MyUtils.pixelToGeoLocation(product, po.x , po.y) + ", ");
////			System.out.println("\nx=" + po.x + ", y=" + po.y + "\n" + coords);
//			multiPoint += coords;
//		}
//		System.out.println(multiPoint);
		TreeSet<Coordinate> clusterOfCoordinates = new TreeSet<>();
		for (Point po : points) {
			Coordinate coords = MyUtils.pixelToGeoCoordinates(product, po.x , po.y);
			clusterOfCoordinates.add(coords);
		}
		System.out.println("Cluster of points with duplicates size = " + points.size());
		System.out.println("Cluster of geoCoordinates WITHOUT duplicates size = " + clusterOfCoordinates.size());
		return clusterOfCoordinates;
	}

	private ArrayList<ArrayList<Point>> clusteringCorePoints(ArrayList<Point> corePoints, double eps) {
		HashMap<Point, ArrayList<Point>> corePointReachCorePoints = new HashMap<>();
		System.out.println("\n\n\tCORE-POINT with CLOSE CORE-POINTS:");
		for (Point point : corePoints) {
			ArrayList<Point> closeCorePoints = new ArrayList<>();
			for (int i = 0; i < corePoints.size(); i ++) {
				if (point.distance(corePoints.get(i)) <= eps) {
					closeCorePoints.add(corePoints.get(i));
				}
			}
			System.out.println("\nCORE-POINT:\t" + point.toString());
			System.out.println("CLOSE CORE-POINTS:" + closeCorePoints);
			corePointReachCorePoints.put(point, closeCorePoints);
		}
		System.out.println("\n" + corePointReachCorePoints.size() + " is the size of mapping core-points to close core-points.");
		System.out.println("\tend of CORE-POINT with CLOSE CORE-POINTS.");
		
		ArrayList<ArrayList<Point>> corePointsInClusters = new ArrayList<>();
		for(Point point : corePoints) {
			if(checkedCorePoints.isEmpty() || !checkedCorePoints.contains(point)){
				ArrayList<Point> cluster = new ArrayList<>();
				ArrayList<Point> pointsInValues = corePointReachCorePoints.get(point);
				for(Point pointAsValue : pointsInValues) {
					if (!cluster.contains(pointAsValue)) {
						cluster.add(pointAsValue);
					}
				}
				checkedCorePoints.add(point);
//				System.out.println("\nCluster before check: " + cluster);
//				System.out.println("checkedCorePoints before check: " + checkedCorePoints);
				ArrayList<Point> legitCluster = checkCluster(cluster, corePointReachCorePoints);
//				System.out.println("\nLegit Cluster returned!");
				corePointsInClusters.add(legitCluster);
			}
		}
		return corePointsInClusters;
	}
	
	private ArrayList<Point> checkCluster(ArrayList<Point> inputCluster, HashMap<Point, ArrayList<Point>> corePointReachCorePoints) {
		ArrayList<Point> outputCluster = new ArrayList<>();
		for (Point point : inputCluster) outputCluster.add(point);
		if(checkedCorePoints.containsAll(inputCluster)) {
			return inputCluster;
		}
		else {
			for(Point clusteredPoint : inputCluster) {
				if(!checkedCorePoints.contains(clusteredPoint)){
					ArrayList<Point> reachableCorePoints = corePointReachCorePoints.get(clusteredPoint);
					for(Point reachableCorePoint : reachableCorePoints) {
						if (!outputCluster.contains(reachableCorePoint)) {
							outputCluster.add(reachableCorePoint);
						}
					}
					checkedCorePoints.add(clusteredPoint);
				}
			}
			return checkCluster(outputCluster, corePointReachCorePoints);
		}
	}
	
	private ArrayList<Point> getChangedPoints(Raster raster, double changeThreshold) {
		ArrayList<Point> pointsOverThreshold = new ArrayList<>();
		int width = raster.getWidth();
		int height = raster.getHeight();
		for (int i = 0; i < width; i++) {
			for (int j = 0; j < height; j++) {
				double pixelValue = raster.getSampleDouble(i, j, 0);
				if (pixelValue > changeThreshold || pixelValue < -changeThreshold) pointsOverThreshold.add(new Point(i, j));
			}
		}
		System.out.println("\n" + pointsOverThreshold.size() + "\n points Over Threshold " + changeThreshold + " are found.");
		return pointsOverThreshold;
	}
	
	private HashMap<Point, ArrayList<Point>> findCoreAndReachables(ArrayList<Point> allPoints, double eps, int minPts) {
		HashMap<Point, ArrayList<Point>> coreToReachablesMap = new HashMap<>();
		System.out.println("\n\n\tCORE-POINT with REACHABLE POINTS:");
		for (Point point : allPoints) {
			ArrayList<Point> reachablePoints = new ArrayList<>();
			for (int i = 0; i < allPoints.size(); i ++) {
				if (point.distance(allPoints.get(i)) <= eps) {
					reachablePoints.add(allPoints.get(i));
				}
			}
			if (reachablePoints.size() >= minPts) {
				System.out.println("\nCORE-POINT:\t" + point.toString());
				System.out.println("REACHABLE POINTS:" + reachablePoints);
				coreToReachablesMap.put(point, reachablePoints);
			}
		}
		System.out.println("\n" + coreToReachablesMap.size() + " is the size of mapping CORE-POINTS to REACHABLE POINTS.");
		System.out.println("\tend of CORE-POINT with REACHABLE POINTS.");
		return coreToReachablesMap;
	}

}