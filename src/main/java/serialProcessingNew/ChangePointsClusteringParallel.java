package serialProcessingNew;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.geom.Point2D;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.Polygon;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import serialProcessingNew.SerialProcessor;

import javax.media.jai.TiledImage;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.ujmp.core.doublematrix.calculation.general.missingvalues.Impute.ImputationMethod;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.dataio.dimap.DimapProductWriter;

public class ChangePointsClusteringParallel implements Serializable {

	public static void main(String[] args) throws IOException {
		String filesPath = args[0];
		String inputFileName=args[1];
		String outputFileName=args[2];
		File inputFile=new File(filesPath,inputFileName);
		File outputFile=new File(filesPath,outputFileName);
		ChangePointsClusteringParallel chalgo=new ChangePointsClusteringParallel();
		chalgo.clusterChanges(inputFile, outputFile);
//		String filesPath = "/home/ethanos/Desktop/BDEimages/VH";
//		String inputFileName="changeD-tile-based-tiledImage.dim";
//		File inputFile=new File(filesPath,inputFileName);
//		File outputFile=new File(filesPath,"clustersTEST_200_2.txt");
//		ChangePointsClusteringParallel chalgo=new ChangePointsClusteringParallel();
//		chalgo.clusterChanges(inputFile, outputFile);
	}
	
	public void clusterChanges(File inputFile, File outputFile) throws IOException {
		long startTime = System.currentTimeMillis();
		MyRead myRead = new MyRead(inputFile, "read");
		long readTime=System.currentTimeMillis();
		System.out.println("readTime "+(readTime-startTime));

		SerialProcessor sp=new SerialProcessor();
//		String[] selPol = new String[]{"VH", "VV"};
		String[] selPol = null;
		sp.getBufferedImage(myRead,selPol);
		Band targetBand= myRead.getTargetProduct().getBandAt(0);
		System.out.println("targetBand "+targetBand);
		
		TiledImage inputImg=(TiledImage)targetBand.getSourceImage().getImage(0);
		System.out.println("inputImg "+inputImg.toString());
		long bandTime=System.currentTimeMillis();
		System.out.println("bandTime "+(bandTime-readTime));
//		GeoCoding geoc =targetBand.getGeoCoding();
		int imageWidth= inputImg.getWidth();
		int imageHeight= inputImg.getHeight();
		System.out.println(" width: "+ imageWidth + " height: "+imageHeight);
		Raster raster = inputImg.getData(new Rectangle(0, 0, imageWidth, imageHeight));//give dimensions 
//		Raster raster = inputImg.getData(new Rectangle(0, 0, 100, 100));//give dimensions 
		int numNodes=4;
		///////////// make array list
		int w= raster.getWidth();
		int h= raster.getHeight();
		boolean[][] isChangingBool = makeBool(raster, 2.0).clone();
		List<boolean[][]> isChangingList = new ArrayList<boolean[][]>();
		for (int k = 0; k < numNodes; k++) {
			boolean[][] isChanging1 = new boolean[w/numNodes][h];
			for (int i = 0; i < w/numNodes; i++) {
				for (int j = 0; j < h; j++) {
					isChanging1[i][j]=isChangingBool[i+k*w/numNodes][j];
				}

			}
			isChangingList.add(k, isChanging1);
		}

		long createArrayTime=System.currentTimeMillis();
		System.out.println("createArrayTime "+(createArrayTime-bandTime));
		//threshold, eps, minPTS
//		SparkConf config = new SparkConf().setMaster("local[2]").setAppName("Img process per node"); //everywhere EXCEPT cluster
		SparkConf config = new SparkConf().setAppName("Parallel DBScan in Spark"); //ONLY in clusters
		//configure spark to use Kryo serializer instead of the java serializer. 
		//All classes that should be serialized by kryo, are registered in MyRegitration class .
		config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//		config.set("spark.kryo.registrator", "pernode_process.MyRegistrator").set("spark.kryoserializer.buffer.max", "550");
		//conf.set("spark.kryo.registrationRequired", "true");  //brought the catastrophe
		JavaSparkContext sc = new JavaSparkContext(config);

		long sparkConfigTime=System.currentTimeMillis();
		System.out.println("sparkConfigTime "+(sparkConfigTime-createArrayTime));
		JavaRDD<boolean[][]> boolArDD= sc.parallelize(isChangingList); //changeFIX
		JavaRDD<List<Set<Point>>> result= boolArDD.map(new Function<boolean[][], List<Set<Point>>>() {
			public List<Set<Point>> call(boolean[][] x) {return dbScanClusters(x, 4, 10, w/numNodes, h);}
		});
		int nodeCnt=0;
		int numPolygons =0;
		int totalPointsPar =0;
		String coords[] = new String[100000];
		int coordsCnt=0;
		for (List<Set<Point>> ClustersRDD : result.collect())
		{
			List<Polygon> ClustersAsPolygon = new ArrayList<Polygon>();
			int cnt=0;
			Point location = new Point(0,0);
			WritableRaster outraster = WritableRaster.createPackedRaster(1, inputImg.getMaxX(), inputImg.getMaxY(), 1, 1, location);
			numPolygons+=ClustersRDD.size();
			System.out.println("PolygonNums: "+ClustersRDD.size());
			for (Set<Point> cl : ClustersRDD)
			{
				Polygon polygon = new Polygon();
				String polygonCoords = "POLYGON((";
				System.out.println("polygon points:"+cl.size());
				int xmax=0;
				int xmin=Integer.MAX_VALUE;
				int ymax=0;
				int ymin=Integer.MAX_VALUE;
				for (Point pi : cl)
				{
					totalPointsPar++;
					System.out.print("- "+"\t"+(pi.x+nodeCnt*(w/numNodes))+"\t"+pi.y+"\t"); //add node array width (nodeCnt*numNodes) to x coordinates 
					polygon.addPoint((int) (pi.getX()+nodeCnt*(w/numNodes)), (int) pi.getY());
//					polygonCoords+=(pi.x+nodeCnt*(w/numNodes))+" "+pi.y+", ";
					if (pi.x<xmin) xmin=pi.x;
					if (pi.y<ymin) ymin=pi.y;
					if (pi.x>xmax) xmax=pi.x;
					if (pi.y>ymax) ymax=pi.y;						
				}
				polygonCoords+=(MyUtils.pixelToGeoLocation(myRead.getTargetProduct(), xmin+nodeCnt*(w/numNodes), ymin)+", ");
				polygonCoords+=(MyUtils.pixelToGeoLocation(myRead.getTargetProduct(), xmax+nodeCnt*(w/numNodes), ymin)+", ");
				polygonCoords+=(MyUtils.pixelToGeoLocation(myRead.getTargetProduct(), xmax+nodeCnt*(w/numNodes), ymax)+", ");
				polygonCoords+=(MyUtils.pixelToGeoLocation(myRead.getTargetProduct(), xmin+nodeCnt*(w/numNodes), ymax)+", ");
				polygonCoords+=(MyUtils.pixelToGeoLocation(myRead.getTargetProduct(), xmin+nodeCnt*(w/numNodes), ymin));
				polygonCoords+="))";
				System.out.println();
				System.out.println(polygonCoords);
				coords[coordsCnt++]=polygonCoords;
				ClustersAsPolygon.add(polygon);
				Rectangle rect = polygon.getBounds();
				for (Point pi : cl)
				{
					int[] fArray = new int[]{1};
					if (cnt>0) outraster.setPixel((int) (pi.getX()+nodeCnt*(w/numNodes)), (int) pi.getY(), fArray);
				}
				cnt++;
			}
			nodeCnt++;
		}
		long getClustersParallelTime=System.currentTimeMillis();
		System.out.println("getClustersParallelTime "+(getClustersParallelTime-sparkConfigTime));
		//write to file the coords
		try{
		    PrintWriter writer = new PrintWriter(outputFile, "UTF-8");
		   for (int i=0; i<coordsCnt; i++)
		   {
			    writer.println(coords[i]);
				System.out.println(coords[i]);
		   }
		    writer.close();
		} catch (Exception e) {
		   // do something
		}
//		System.out.println("NOT PARALLEL NOT PARALLEL");
//		
//		
//		List<Set<Point>> Clusters = this.dbScanClusters(makeBool(raster, 2.0), 4, 10, raster.getWidth(), raster.getHeight());
//		List<Polygon> ClustersAsPolygon = new ArrayList<Polygon>();
//		int cnt=0;
//		Point location = new Point(0,0);
//		WritableRaster outraster = WritableRaster.createPackedRaster(1, inputImg.getMaxX(), inputImg.getMaxY(), 1, 1, location);
//
//		int totalPointsSer =0;
//		System.out.println("PolygonNums: "+Clusters.size());
//		for (Set<Point> cl : Clusters)
//		{
//			Polygon polygon = new Polygon();
//			String polygonCoords = "POLYGON((";
//			System.out.println("polygon points:"+cl.size());
//			for (Point pi : cl)
//			{
//				totalPointsSer++;
//				System.out.print("- "+"\t"+pi.x+"\t"+pi.y+"\t");
//				polygon.addPoint((int) pi.getX(), (int) pi.getY());
//				polygonCoords+=pi.x+" "+pi.y+", ";
//			}
//			polygonCoords+="))";
//			System.out.println();
//			System.out.println(polygonCoords);
//			ClustersAsPolygon.add(polygon);
//			Rectangle rect = polygon.getBounds();
//			for (Point pi : cl)
//			{
//				int[] fArray = new int[]{1};
//				if (cnt>0) outraster.setPixel((int) pi.getX(), (int) pi.getY(), fArray);
//			}
//			cnt++;
//		}
//		long getClustersSerialTime=System.currentTimeMillis();
//		System.out.println("getClustersSerialTime "+(getClustersSerialTime-getClustersParallelTime));
//
//		System.out.println("readTime "+(readTime-startTime));
//		System.out.println("bandTime "+(bandTime-readTime));
//		System.out.println("createArrayTime "+(createArrayTime-bandTime));
//		System.out.println("sparkConfigTime "+(sparkConfigTime-createArrayTime));
//		System.out.println("getClustersParallelTime "+(getClustersParallelTime-sparkConfigTime));
//		System.out.println("getClustersSerialTime "+(getClustersSerialTime-getClustersParallelTime));
//
//		System.out.println("PolygonNumsParallel: "+numPolygons);
//		System.out.println("PolygonNumsSerial: "+Clusters.size());
//		System.out.println("totalPointsPar: "+totalPointsPar);
//		System.out.println("totalPointsSer: "+totalPointsSer);		
//		System.out.println("geoPos "+MyUtils.pixelToGeoLocation(myRead.getTargetProduct(), 54, 32));
//		System.out.println("geoPos "+MyUtils.pixelToGeoLocation(myRead.getTargetProduct(), 27, 82));
//			TiledImage outImg=new TiledImage(inputImg, raster.getWidth(), raster.getWidth());
//			outImg.setData(outraster);
//			Band tb = myRead.getTargetProduct().getBandAt(0);
//			tb.setSourceImage(outImg);
//			Product prod = myRead.getTargetProduct();
//			prod.removeBand(targetBand);
//			prod.addBand(tb);
//			prod.setFileLocation(outputFile);
//			MyWrite mywrite = new MyWrite(prod, outputFile, "BEAM-DIMAP");
//			mywrite.setId("write");
//			sp.initOperatorForMultipleBands(mywrite);
//			Product targetProduct = mywrite.getTargetProduct();
//			LoopLimits limits = new LoopLimits(targetProduct);
//			int noOfBands = targetProduct.getNumBands();
//			for (int i = 0; i < noOfBands; i++) {
//				Band band = targetProduct.getBandAt(i);
//				for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
//					for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
//						if (band.getClass() == Band.class && band.isSourceImageSet()) {
//							GetTile getTile = new GetTile(band, mywrite);
//							getTile.computeTile(tileX, tileY);
//						}
//					}
//				}
//			}


		
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
