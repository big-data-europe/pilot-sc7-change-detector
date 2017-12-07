package serialProcessingNew;

import java.awt.Point;
import java.awt.Polygon;
import java.awt.Rectangle;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.media.jai.TiledImage;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.esa.snap.core.datamodel.Band;

import com.vividsolutions.jts.geom.Geometry;

public class ChangePointsClusteringParallel implements Serializable {

	public static void main(String[] args) throws IOException {
		String filesPath = args[0];
		String inputFileName = args[1];
		String outputFileName = args[2];
		String referencePolygon = args[3];
		double threshold = Double.parseDouble(args[4]);
//		String filesPath = "/media/indiana/data/imgs/imgSaoPaolo/originalcode/originalCD";
//		String inputFileName = "originalCD.dim";
//		String outputFileName = "manosDB.txt";
//		String referencePolygon = "POLYGON ((-46.941093 -22.658804,-46.180542 -22.473888,-45.699978 -24.201136,-46.469711 -24.389177,-46.941093 -22.658804))";
//		double threshold = 1.3;
		File inputFile = new File(filesPath, inputFileName);
		File outputFile = new File(filesPath, outputFileName);
		ChangePointsClusteringParallel chalgo = new ChangePointsClusteringParallel();
		chalgo.clusterChanges(inputFile, outputFile, referencePolygon, threshold);
	}
	
	public void clusterChanges(File inputFile, File outputFile, String referencePolygon, double threshold) throws IOException {
		long startTime = System.currentTimeMillis();
		MyRead myRead = new MyRead(inputFile, "read");
		long readTime = System.currentTimeMillis();
		System.out.println("readTime " + (readTime - startTime));

		SerialProcessor sp = new SerialProcessor();
//		String[] selPol = new String[]{"VH", "VV"};
		String[] selPol = null;
		sp.getBufferedImage(myRead, selPol);
		Band targetBand = myRead.getTargetProduct().getBandAt(0);
		System.out.println("targetBand " + targetBand);
		
		TiledImage inputImg = (TiledImage)targetBand.getSourceImage().getImage(0);
//		GeoCoding geoc = targetBand.getGeoCoding();
		int imageWidth = inputImg.getWidth();
		int imageHeight = inputImg.getHeight();
		System.out.println(" width: " + imageWidth + " height: " + imageHeight);
		Raster raster = inputImg.getData(new Rectangle(0, 0, imageWidth, imageHeight));//give dimensions 
//		Raster raster = inputImg.getData(new Rectangle(0, 0, 100, 100));//give dimensions 
		int numNodes = 4;
		///////////// make array list
		int w = raster.getWidth();
		int h = raster.getHeight();
		boolean[][] isChangingBool = makeBool(raster, threshold).clone();
		List<boolean[][]> isChangingList = new ArrayList<boolean[][]>();
		for (int k = 0; k < numNodes; k++) {
			boolean[][] isChanging1 = new boolean[w/numNodes][h];
			for (int i = 0; i < w/numNodes; i++) {
				for (int j = 0; j < h; j++) {
					isChanging1[i][j] = isChangingBool[i + k * w/numNodes][j];
				}

			}
			isChangingList.add(k, isChanging1);
		}
		//threshold, eps, minPTS
		SparkConf config = new SparkConf().setMaster("local[*]").setAppName("Parallel DBScan in Spark"); //everywhere EXCEPT cluster
//		SparkConf config = new SparkConf().setAppName("Parallel DBScan in Spark"); //ONLY in clusters
		//configure spark to use Kryo serializer instead of the java serializer. 
		//All classes that should be serialized by kryo, are registered in MyRegitration class .
		config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		JavaSparkContext sc = new JavaSparkContext(config);

		JavaRDD<boolean[][]> boolArDD = sc.parallelize(isChangingList); //changeFIX
		JavaRDD<List<Set<Point>>> result = boolArDD.map(new Function<boolean[][], List<Set<Point>>>() {
			public List<Set<Point>> call(boolean[][] x) { 
				return dbScanClusters(x, 3, 10, w/numNodes, h);
				}
		});
		int nodeCnt = 0;
//		String coords[] = new String[100000];
		ArrayList<String> dbScanResult = new ArrayList<>();
//		int coordsCnt = 0;
		for (List<Set<Point>> ClustersRDD : result.collect()) {
			List<Polygon> ClustersAsPolygon = new ArrayList<Polygon>();
			int cnt = 0;
			Point location = new Point(0, 0);
			WritableRaster outraster = WritableRaster.createPackedRaster(1, inputImg.getMaxX(), inputImg.getMaxY(), 1, 1, location);
			System.out.println("PolygonNums: " + ClustersRDD.size());
			for (Set<Point> cl : ClustersRDD) {
				Polygon polygon = new Polygon();
				String polygonCoords = "POLYGON((";
//				System.out.println("polygon points:"+cl.size());
				int xmax = 0;
				int xmin = Integer.MAX_VALUE;
				int ymax = 0;
				int ymin = Integer.MAX_VALUE;
				for (Point pi : cl) {
//					System.out.print("- "+"\t"+(pi.x+nodeCnt*(w/numNodes))+"\t"+pi.y+"\t"); //add node array width (nodeCnt*numNodes) to x coordinates 
					polygon.addPoint((int) (pi.getX() + nodeCnt * (w / numNodes)), (int) pi.getY());
//					polygonCoords+=(pi.x+nodeCnt*(w/numNodes))+" "+pi.y+", ";
					if (pi.x < xmin) xmin = pi.x;
					if (pi.y < ymin) ymin = pi.y;
					if (pi.x > xmax) xmax = pi.x;
					if (pi.y > ymax) ymax = pi.y;						
				}
				System.out.println("xmin = " + xmin);
				System.out.println("xmax = " + xmax);
				System.out.println("ymin = " + ymin);
				System.out.println("nodeCnt = " + nodeCnt);
				System.out.println("numNodes = " + numNodes);
				polygonCoords += (MyUtils.pixelToGeoLocation(myRead.getTargetProduct(), xmin + nodeCnt * (w / numNodes), ymin) + ", ");
				polygonCoords += (MyUtils.pixelToGeoLocation(myRead.getTargetProduct(), xmax + nodeCnt * (w / numNodes), ymin) + ", ");
				polygonCoords += (MyUtils.pixelToGeoLocation(myRead.getTargetProduct(), xmax + nodeCnt * (w / numNodes), ymax) + ", ");
				polygonCoords += (MyUtils.pixelToGeoLocation(myRead.getTargetProduct(), xmin + nodeCnt * (w / numNodes), ymax) + ", ");
				polygonCoords += (MyUtils.pixelToGeoLocation(myRead.getTargetProduct(), xmin + nodeCnt * (w / numNodes), ymin));
				polygonCoords += "))";
//				coords[coordsCnt++] = polygonCoords;
				dbScanResult.add(polygonCoords);
				ClustersAsPolygon.add(polygon);
				for (Point pi : cl)
				{
					int[] fArray = new int[]{1};
					if (cnt > 0) outraster.setPixel((int) (pi.getX() + nodeCnt * (w / numNodes)), (int) pi.getY(), fArray);
				}
				cnt++;
			}
			nodeCnt++;
		}
		System.out.println("\n\n\tInitial dbScanResult:");
		for (int i = 0; i < dbScanResult.size(); i++){
			System.out.println(dbScanResult.get(i));
		}
		System.out.println("\tEND-OF Initial dbScanResult.\n");
		if(dbScanResult.size() > 0){
			ArrayList<Geometry> checkedDBScanResult = MyUtils.resultChecker(dbScanResult, referencePolygon);
			try {
			    PrintWriter writer = new PrintWriter(outputFile, "UTF-8");
			   for (int i = 0; i < checkedDBScanResult.size(); i++)
			   {
				    writer.println(checkedDBScanResult.get(i).toString());
			   }
			    writer.close();
			} catch (Exception e) {
				System.out.println("Could not create output File!!! Check local output filepath argument.");
			}
		}
		else {
			if(outputFile.exists()) {
				System.out.println("File exists!");
			}
			else {
				try {
					outputFile.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.out.println("Cannot create file!There will be problem with next stage.");
				}
			}
		}
		
	}

	private int checkChange(Raster raster, double changeThres) {
		int width = raster.getWidth();
		int height = raster.getHeight();
		double[] dArray = new double[1];
		int changed = 0;
		for (int i = 0; i < width; i++) {
			for (int j = 0; j < height; j++) {
				raster.getPixel(i, j, dArray);
				if (dArray[0] > changeThres || dArray[0] < -changeThres)
				{
					changed++;
				}
			}
		}
		return changed;
	}
	

	private boolean[][] makeBool(Raster raster, double changeThres) {
		int width = raster.getWidth();
		int height = raster.getHeight();
		double[] dArray = new double[1];
		boolean[][] isChanging = new boolean[width][height];
		for (int i = 0; i < width; i++) {
			for (int j = 0; j < height; j++) {
				raster.getPixel(i, j, dArray);
				if (dArray[0] > changeThres || dArray[0] < -changeThres)
				{
					isChanging[i][j]=true;
				}


			}
		}
		
		return isChanging;
	}
	
	private List<Set<Point>> dbScanClusters(boolean[][] isChanging,  int eps, int minPts, int width, int height ) {
		List<Set<Point>> Clusters = new ArrayList<Set<Point>>();
		Set<Point> visited = new HashSet<Point>();
		Set<Point> noise = new HashSet<Point>();
		Set<Point> clusterMember = new HashSet<Point>();
		for (int i = 0; i < width; i++) {
			for (int j = 0; j < height; j++) {
				if (!isChanging[i][j]) continue;
				Point point = new Point(i, j);			
				if (visited.contains(point)) 
				{
					continue;
				}				
	            List<Point> neighbors = getNeighbors(point, isChanging, eps);
	            if (neighbors.size() >= minPts)
	            {
	            	Clusters.add(expandCluster(point, neighbors, isChanging, visited, clusterMember,  noise, eps, minPts));
	            }
	            else
	            {
	            	visited.add(point);
	            	noise.add(point);
	            }
			}
		}
		return Clusters;
	}
	
	private Set<Point> expandCluster(Point point, List<Point> neighbors, boolean[][] isChanging,
			Set<Point> visited, Set<Point> clusterMember, Set<Point> noise, int eps, int minPts) {
		Set<Point> cluster = new HashSet<Point>();
		
		cluster.add(point);
		visited.add(point);
		clusterMember.add(point);
		
		List<Point> seeds = new ArrayList<Point>(neighbors);

		int index = 0;

        while (index < seeds.size()) {

            Point current = seeds.get(index);
            // only check non-visited points
            if (!(clusterMember.contains(current))) {
            	visited.add(current);
                List<Point> currentNeighbors = getNeighbors(current, isChanging, eps);
                if (currentNeighbors.size() >= minPts) {
                    seeds = merge(seeds, currentNeighbors);
                }
                noise.remove(current);
        		clusterMember.add(current);
                cluster.add(current);
            }
            index++;
        }		
		return cluster;
	}
	
	private List<Point> getNeighbors(Point point, boolean[][] points, int eps) {
        final List<Point> neighbors = new ArrayList<Point>();
        int px = (int) point.x;
        int py = (int) point.y;
        for (int i = Math.max(px-eps, 0); i < Math.min(px+eps+1, points.length); i++) {
			for (int j = Math.max(py-eps, 0); j < Math.min(py+eps+1, points[0].length); j++) {
				if (points[i][j] && (Math.sqrt(Math.pow((px-i), 2)+Math.pow((py-j), 2)) <= eps)) {
					Point p = new Point(i ,j);
	                neighbors.add(p);
				}
            }
			
        }
        return neighbors;
    }
	
	private List<Point> merge(List<Point> one, List<Point> two) {
        final Set<Point> oneSet = new HashSet<Point>(one);
        for (Point item : two) {
            if (!oneSet.contains(item)) {
                one.add(item);
            }
        }
        return one;
    }

}
