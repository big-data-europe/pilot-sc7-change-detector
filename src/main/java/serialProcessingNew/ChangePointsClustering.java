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
import java.util.ArrayList;
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
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.dataio.dimap.DimapProductWriter;
public class ChangePointsClustering {

	public static void main(String[] args) throws IOException {
		String filesPath = "/media/indiana/data/imgs/subseting/subset01_Larisa/Larisa_ncsr_result/";
		String inputFileName = "SparkChangeDetResult.dim";
		File inputFile = new File(filesPath, inputFileName);
		File outputFile = new File("/media/indiana/data/imgs/subseting/subset01_Larisa/Larisa_ncsr_result/", "CLUSTERS600" + inputFileName);
		long startTime = System.currentTimeMillis();
		ChangePointsClustering chalgo=new ChangePointsClustering();
		chalgo.clusterChanges(inputFile, outputFile);
		long endTime = System.currentTimeMillis();
		long totalTime=endTime-startTime;
		System.out.println("totalTime "+totalTime);
	}
	
	public void clusterChanges(File inputFile, File outputFile) throws IOException {
		
		MyRead myRead = new MyRead(inputFile, "read");
		SerialProcessor sp=new SerialProcessor();
//		String[] selPol = new String[]{"VH", "VV"};
		String[] selPol = null;
		sp.getBufferedImage(myRead,selPol);
		Band targetBand= myRead.getTargetProduct().getBandAt(0);
		System.out.println("targetBand "+targetBand);
		TiledImage inputImg=(TiledImage)targetBand.getSourceImage().getImage(0);
		System.out.println("inputImg "+inputImg.toString());

//		GeoCoding geoc =targetBand.getGeoCoding();
		Raster raster = inputImg.getData(new Rectangle(0, 0, 600, 600));//give dimensions 
		//threshold, eps, minPTS
		List<Set<Point>> Clusters = this.dbScanClusters(raster, 1.0, 4, 10);
		List<Polygon> ClustersAsPolygon = new ArrayList<Polygon>();
		int cnt = 0;
		Point location = new Point(0,0);
		WritableRaster outraster = WritableRaster.createPackedRaster(1, inputImg.getMaxX(), inputImg.getMaxY(), 1, 1, location);
		for (Set<Point> cl : Clusters)
		{
			Polygon polygon = new Polygon();
			String polygonCoords = "POLYGON((";
			for (Point pi : cl)
			{
//				System.out.print("- " + "\t" + pi.x + "\t" + pi.y + "\t");
				polygon.addPoint((int) pi.getX(), (int) pi.getY());
				polygonCoords+=pi.x+" "+pi.y+", ";
			}
			polygonCoords+="))";
//			System.out.println();
//			System.out.println(polygonCoords);
			ClustersAsPolygon.add(polygon);
			Rectangle rect = polygon.getBounds();
			for (Point pi : cl)
			{
				int[] fArray = new int[]{1};
				if (cnt>0) outraster.setPixel((int) pi.getX(), (int) pi.getY(), fArray);
			}
			cnt++;
		}
		System.out.println("\nEO Original code. Manos Thanos' printing:\n");
		
		//Manos Thanos' printing polygons!
		int nodeCnt = 0;
		int numPolygons2 = Clusters.size();
		int totalPointsPar = 0;
		
		int w = raster.getWidth();
		int numNodes = 4;
		
		String coords[] = new String[100000];
		int coordsCnt = 0;
		
		List<Polygon> ClustersAsPolygon2 = new ArrayList<Polygon>();
		int cnt2 = 0;
		Point location2 = new Point(0, 0);
		WritableRaster outraster2 = WritableRaster.createPackedRaster(1, inputImg.getMaxX(), inputImg.getMaxY(), 1, 1, location2);
		for (Set<Point> cl : Clusters) {
			Polygon polygon = new Polygon();
			String polygonCoords = "POLYGON((";
			int xmax = 0;
			int xmin = Integer.MAX_VALUE;
			int ymax = 0;
			int ymin = Integer.MAX_VALUE;
			for (Point pi : cl) {
				totalPointsPar++;
				polygon.addPoint((int) (pi.getX() + nodeCnt * (w / numNodes)), (int) pi.getY());
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
			coords[coordsCnt++] = polygonCoords;
			ClustersAsPolygon2.add(polygon);
			Rectangle rect = polygon.getBounds();
			for (Point pi : cl) {
				int[] fArray = new int[]{1};
				if (cnt2 > 0) outraster2.setPixel((int) (pi.getX() + nodeCnt * (w / numNodes)), (int) pi.getY(), fArray);
			}
			cnt2++;
//			nodeCnt++;
		}
		for (int j = 0; j < coordsCnt; j++){
			System.out.println(coords[j]);
		}
	}


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
	
	private List<Set<Point>> dbScanClusters(Raster raster, double changeThres, int eps, int minPts) {
		
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
        for (int i = 0; i < points.length; i++) {
			for (int j = 0; j < points[0].length; j++) {
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
