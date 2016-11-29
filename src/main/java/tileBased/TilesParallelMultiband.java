package tileBased;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.media.jai.PlanarImage;
import javax.media.jai.RasterFactory;
import javax.media.jai.RenderedOp;
import javax.media.jai.WarpOpImage;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.esa.s1tbx.insar.gpf.coregistration.GCPManager;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Placemark;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductNodeGroup;
import org.esa.snap.core.image.ImageManager;
import org.esa.snap.core.util.ImageUtils;
import org.esa.snap.engine_utilities.gpf.StackUtils;

import com.google.common.collect.Lists;

import scala.Tuple2;
import scala.Tuple3;
import serialProcessingNew.LoopLimits;
import serialProcessingNew.MyChangeDetection;
import serialProcessingNew.MyCreateStack;
import serialProcessingNew.MyGCPSelection;
import serialProcessingNew.MyRead;
import serialProcessingNew.MyWarp;
import serialProcessingNew.MyWarp.WarpData;
import serialProcessingNew.SerialProcessor;
import serialProcessingNew.Utils;
import serialProcessingNew.calibration.MyCalibration;
import tileBased.metadata.CalibrationMetadata;
import tileBased.metadata.ChangeDetectionMetadata;
import tileBased.metadata.GCPMetadata;
import tileBased.metadata.ImageMetadata;
import tileBased.metadata.WarpMetadata;
import tileBased.model.MyTile;
import tileBased.operator.CreateStack;
import tileBased.operator.GCPSelection;
import tileBased.operator.Sentinel1Calibrator;
import tileBased.operator.Warp;

public class TilesParallelMultiband {

	public static void main(String[] args) throws Exception {
		String filesPath = "/home/efi/SNAP/sentinel-images/";
		 File masterFile = new File(filesPath,
		 "subset_0_of_S1A_IW_GRDH_1SDV_20151110T145915_20151110T145940_008544_00C1A6_F175.dim");
		 File slaveFile = new File(filesPath,
		 "subset_1_of_S1A_IW_GRDH_1SDV_20151029T145915_20151029T145940_008369_00BD0B_334C.dim");
//		File masterFile = new File(filesPath,
//				"subset3_of_S1A_IW_GRDH_1SSV_20141225T142407_20141225T142436_003877_004A54_040F.dim");
//		File slaveFile = new File(filesPath,
//				"subset3_of_S1A_IW_GRDH_1SSV_20150518T142409_20150518T142438_005977_007B49_AF76.dim");
		TilesParallelMultiband parallelTiles = new TilesParallelMultiband();
		parallelTiles.processTiles(masterFile, slaveFile);
	}

	public void processTiles(File masterFile, File slaveFile) throws Exception {
//		long startTime = System.currentTimeMillis();
//		List<Tuple2<String, MyTile>> slaveRasters = null;
//		List<Tuple2<String, MyTile>> masterRasters = null;
//		Map<String, List<Tuple2<Point, Rectangle>>> dependRects = new HashMap<String, List<Tuple2<Point, Rectangle>>>();
//
//		// read slave and master files as usual
//		SerialProcessor sp = new SerialProcessor();
//		MyRead myRead1 = null;
//		MyRead myRead2 = null;
//		try {
//			myRead1 = new MyRead(masterFile, "read1");
//			masterRasters = sp.readTiles(myRead1);
//		} catch (Exception e) {
//			throw e;
//		}
//
//		try {
//			myRead2 = new MyRead(slaveFile, "read2");
//			slaveRasters = sp.readTiles(myRead2);
//		} catch (Exception e) {
//			throw e;
//		}
//		Boolean[] bParams1 = { false, false, false, false, true, false, false, false };
//		MyCalibration myCalibration1 = new MyCalibration(null, bParams1, null);
//		myCalibration1.setSourceProduct(myRead1.getTargetProduct());
//		myCalibration1.setId("cal1");
//		sp.initOperator(myCalibration1);
//
//		MyCalibration myCalibration2 = new MyCalibration(null, bParams1, null);
//		myCalibration2.setSourceProduct(myRead2.getTargetProduct());
//		myCalibration2.setId("cal2");
//		sp.initOperator(myCalibration2);
//
//		Product[] sourcesForCreateStack = new Product[2];
//		sourcesForCreateStack[0] = myCalibration1.getTargetProduct();
//		sourcesForCreateStack[1] = myCalibration2.getTargetProduct();
//		Point[] ps=myCalibration1.getTargetProduct().getBandAt(0).getSourceImage().getTileIndices(new Rectangle(0,0,525,634));
//		// init all operators as usual
//		String[] parameters = { "NONE", "Master", "Orbit" };
//		MyCreateStack myCreateStack2 = new MyCreateStack(parameters);
//		myCreateStack2.setSourceProduct(sourcesForCreateStack);
//		myCreateStack2.setId("stack");
//		sp.initOperator(myCreateStack2);
//
//		boolean[] bParams = { false, false, false, false };
//		int[] iParams = { 1000, 10, 3 };
//		double[] dParams = { 0.25, 0.6 };
//		String[] sParams = { "128", "128", "4", "4", "32", "32" };
//		GCPMetadata GCPMetadata = new GCPMetadata(bParams, dParams, iParams, sParams);
//		MyGCPSelection gcpSelectionOp = new MyGCPSelection(bParams, dParams, iParams, sParams);
//		gcpSelectionOp.setSourceProduct(myCreateStack2.getTargetProduct());
//		gcpSelectionOp.setId("gcp");
//		sp.initOperator(gcpSelectionOp);
//
//		boolean[] bParams2 = { false, false };
//		MyWarp warpOp = new MyWarp(bParams2, 0.05f, 1, "Bilinear interpolation");
//		warpOp.setSourceProduct(gcpSelectionOp.getTargetProduct());
//		warpOp.setId("warp");
//		sp.initOperator(warpOp);
//
//		boolean[] bParams3 = { false, false };
//		float[] fParams = { 2.0f, -2.0f };
//		MyChangeDetection myChangeDetection = new MyChangeDetection(bParams3, fParams, null);
//		myChangeDetection.setSourceProduct(warpOp.getTargetProduct());
//		myChangeDetection.setId("changeD");
//		sp.initOperator(myChangeDetection);
//
//		OpMetadataCreator opMetadataCreator = new OpMetadataCreator();
//		// calibration1 metadata.
//		Map<String, ImageMetadata> masterImageMetadataCal = new HashMap<String, ImageMetadata>(
//				myCalibration1.getTargetProduct().getNumBands() * 2);
//		Map<String, String> sourceTargetMapCal1 = new HashMap<String, String>(
//				myCalibration1.getTargetProduct().getNumBands());
//		Map<String, CalibrationMetadata> masterCalMetadata = new HashMap<String, CalibrationMetadata>(
//				myCalibration1.getTargetProduct().getNumBands() * 2);
//		opMetadataCreator.createCalImgMetadata(masterImageMetadataCal, sourceTargetMapCal1, myCalibration1,
//				masterCalMetadata, bParams1);
//
//		// calibration2 metadata.
//
//		Map<String, ImageMetadata> slaveImageMetadataCal = new HashMap<String, ImageMetadata>(
//				myCalibration2.getTargetProduct().getNumBands() * 2);
//		Map<String, String> sourceTargetMapCal2 = new HashMap<String, String>(
//				myCalibration2.getTargetProduct().getNumBands());
//		Map<String, CalibrationMetadata> slaveCalMetadata = new HashMap<String, CalibrationMetadata>(
//				myCalibration2.getTargetProduct().getNumBands() * 2);
//		opMetadataCreator.createCalImgMetadata(slaveImageMetadataCal, sourceTargetMapCal2, myCalibration2,
//				slaveCalMetadata, bParams1);
//
//		// createstack metadata.
//
//		Map<String, ImageMetadata> imageMetadataStack = new HashMap<String, ImageMetadata>(
//				myCreateStack2.getTargetProduct().getNumBands() * 2);
//		Map<String, String> sourceTargetMapstack = new HashMap<String, String>(
//				myCreateStack2.getTargetProduct().getNumBands());
//		Map<String, int[]> slaveOffsetMapBands = new HashMap<String, int[]>(
//				myCreateStack2.getTargetProduct().getNumBands());
//
//		opMetadataCreator.createOpImgMetadata(imageMetadataStack, sourceTargetMapstack, myCreateStack2,
//				slaveOffsetMapBands);
//		Map<Product, int[]> slaveOffsetMap = myCreateStack2.getSlaveOffsettMap();
//
//		Map<Band, Band> sourceRasterMap = myCreateStack2.getSourceRasterMap();
//		Product targetProductStack = myCreateStack2.getTargetProduct();
//		String[] masterBandNames = StackUtils.getMasterBandNames(targetProductStack);
//		Set<String> masterBands = new HashSet(Arrays.asList(masterBandNames));
//
//		LoopLimits limits = new LoopLimits(targetProductStack);
//		for (int i = 0; i < targetProductStack.getNumBands(); i++) {
//			if (masterBands.contains(targetProductStack.getBandAt(i).getName())) {
//				continue;
//			}
//			ImageMetadata srcMetadata = imageMetadataStack
//					.get(sourceRasterMap.get(targetProductStack.getBandAt(i)).getName()+"_"+"stack");
//			ImageMetadata trgMetadata = imageMetadataStack.get(targetProductStack.getBandAt(i).getName()+"_"+"stack");
//			for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
//				for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
//					Rectangle dependRect = OperatorDependencies.createStack(trgMetadata.getRectangle(tileX, tileY),
//							slaveOffsetMap.get(sourceRasterMap.get(targetProductStack.getBandAt(i)).getProduct()),
//							srcMetadata.getImageWidth(), srcMetadata.getImageHeight());
//					String bandName = sourceRasterMap.get(targetProductStack.getBandAt(i)).getName();
//					if (dependRects.containsKey(bandName)) {
//						dependRects.get(bandName)
//								.add(new Tuple2<Point, Rectangle>(new Point(tileX, tileY), dependRect));
//					} else {
//						List<Tuple2<Point, Rectangle>> rectsList = new ArrayList<Tuple2<Point, Rectangle>>();
//						rectsList.add(new Tuple2<Point, Rectangle>(new Point(tileX, tileY), dependRect));
//						dependRects.put(bandName, rectsList);
//					}
//				}
//			}
//
//		}
//		// gcpmetadata metadata.
//
//		Map<String, ImageMetadata> imageMetadataGCP = new HashMap<String, ImageMetadata>(
//				gcpSelectionOp.getTargetProduct().getNumBands() * 2);
//		Map<String, String> sourceTargetMapGCP = new HashMap<String, String>(
//				gcpSelectionOp.getTargetProduct().getNumBands());
//
//		opMetadataCreator.createOpImgMetadata(imageMetadataGCP, sourceTargetMapGCP, gcpSelectionOp);
//		Map<String, String> bandsListGCP = gcpSelectionOp.slavebandsForGCP();
//
//		// Warp image and other metadata
//
//		Map<String, ImageMetadata> imageMetadataWarp = new HashMap<String, ImageMetadata>();
//		opMetadataCreator.createOpImgMetadata(imageMetadataWarp, null, warpOp);
//		WarpMetadata warpMetadata = opMetadataCreator.createWarpMetadata(warpOp);
//
//		// Change detection image and other metadata
//		Map<String, ImageMetadata> imageMetadataChD = new HashMap<String, ImageMetadata>();
//		Map<String, String> sourceTargetMapChD = new HashMap<String, String>();
//		opMetadataCreator.createOpImgMetadata(imageMetadataWarp, null, warpOp);
//		ChangeDetectionMetadata changeDMetadata = opMetadataCreator.createChangeDMetadata(myChangeDetection, bParams3);
//
//		// init sparkConf
//		SparkConf conf = new SparkConf().setMaster("local[4]").set("spark.driver.maxResultSize", "3g")
//				.setAppName("First test on tile parallel processing");
//
//		// configure spark to use Kryo serializer instead of the java
//		// serializer.
//		// All classes that should be serialized by kryo, are registered in
//		// MyRegitration class .
//		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//		conf.set("spark.kryo.registrator", "tilesParallel.MyRegistrator").set("spark.kryoserializer.buffer.max", "550");
//		JavaSparkContext sc = new JavaSparkContext(conf);
//
//		// broadcast calibration metadata
//		Broadcast<Map<String, ImageMetadata>> masterImgMetadataCalB = sc.broadcast(masterImageMetadataCal);
//		Broadcast<Map<String, ImageMetadata>> slaveImgMetadataCalB = sc.broadcast(slaveImageMetadataCal);
//		Broadcast<Map<String, CalibrationMetadata>> masterCalMetadataB = sc.broadcast(masterCalMetadata);
//		Broadcast<Map<String, CalibrationMetadata>> slaveCalMetadataB = sc.broadcast(slaveCalMetadata);
//		Broadcast<Map<String, String>> sourceTargetMapCal1B = sc.broadcast(sourceTargetMapCal1);
//		Broadcast<Map<String, String>> sourceTargetMapCal2B = sc.broadcast(sourceTargetMapCal2);
//		// broadcast create stack metadata
//		Broadcast<Map<String, String>> sourceTargetMapStackB = sc.broadcast(sourceTargetMapstack);
//		Broadcast<Map<String, ImageMetadata>> imgMetadataStackB = sc.broadcast(imageMetadataStack);
//		Broadcast<Map<String, int[]>> slaveBandsOffset = sc.broadcast(slaveOffsetMapBands);
//		Broadcast<Map<String, List<Tuple2<Point, Rectangle>>>> dependRectsB = sc.broadcast(dependRects);
//		// broadcast gcp metadata
//		Broadcast<Map<String, String>> sourceTargetMapGCPB = sc.broadcast(sourceTargetMapGCP);
//		Broadcast<Map<String, ImageMetadata>> imgMetadataGCPB = sc.broadcast(imageMetadataGCP);
//		Broadcast<ProductNodeGroup<Placemark>> masterGcps = sc.broadcast(gcpSelectionOp.getMasterGcpGroup());
//		Broadcast<GCPMetadata> GCPMetadataBroad = sc.broadcast(GCPMetadata);
//		Broadcast<Map<String, String>> bandsListGCPB = sc.broadcast(bandsListGCP);
//
//		// broadcast warp metadata
//		Broadcast<Map<String, ImageMetadata>> imgMetadataWarpB = sc.broadcast(imageMetadataWarp);
//		Broadcast<WarpMetadata> warpMetadataB = sc.broadcast(warpMetadata);
//
//		Broadcast<Integer> rows = sc.broadcast(limits.getNumXTiles());
//
//		LoopLimits limits2 = new LoopLimits(warpOp.getTargetProduct());
//		Broadcast<LoopLimits> limitsBroad = sc.broadcast(limits2);
//
//		JavaPairRDD<String, MyTile> masterRastersRdd = sc.parallelizePairs(masterRasters);
//		JavaPairRDD<String, MyTile> slaveRastersRdd = sc.parallelizePairs(slaveRasters);
//
//		JavaPairRDD<Tuple2<Point, String>, MyTile> masterRastersCal = masterRastersRdd
//				.mapToPair((Tuple2<String, MyTile> tuple) -> {
//					Map<String, ImageMetadata> map = masterImgMetadataCalB.getValue();
//					ImageMetadata trgImgMetadataCal1 = map.get(sourceTargetMapCal1B.getValue().get(tuple._1+"_"+"cal1")+"_"+"cal1");
//					ImageMetadata srcImgMetadataCal1 = masterImgMetadataCalB.getValue().get(tuple._1+"_"+"cal1");
//					Point targetPoint = trgImgMetadataCal1.getTileIndices(tuple._2.getMinX(), tuple._2.getMinY());
//					CalibrationMetadata calMeatadata = masterCalMetadataB.getValue().get(tuple._1+"_"+"cal1");
//					Sentinel1Calibrator sentinel1Calibrator = new Sentinel1Calibrator(calMeatadata);
//					MyTile targetTile = new MyTile(trgImgMetadataCal1.getWritableRaster(targetPoint.x, targetPoint.y),
//							trgImgMetadataCal1.getRectangle(targetPoint.x, targetPoint.y),
//							trgImgMetadataCal1.getDataType());
//					sentinel1Calibrator.computeTile(tuple._2, null, targetTile, srcImgMetadataCal1.getNoDataValue(),
//							trgImgMetadataCal1.getBandName());
//					return new Tuple2<Tuple2<Point, String>, MyTile>(
//							new Tuple2<Point, String>(targetPoint, trgImgMetadataCal1.getBandName()), targetTile);
//
//				});
//
//		JavaPairRDD<String, MyTile> slaveRastersCal = slaveRastersRdd.mapToPair((Tuple2<String, MyTile> tuple) -> {
//			ImageMetadata trgImgMetadataCal2 = slaveImgMetadataCalB.getValue()
//					.get(sourceTargetMapCal2B.getValue().get(tuple._1+"_"+"cal2")+"_"+"cal2");
//			ImageMetadata srcImgMetadataCal2 = slaveImgMetadataCalB.getValue().get(tuple._1+"_"+"cal2");
//			Point targetPoint = trgImgMetadataCal2.getTileIndices(tuple._2.getMinX(), tuple._2.getMinY());
//			CalibrationMetadata calMeatadata = slaveCalMetadataB.getValue().get(tuple._1+"_"+"cal2");
//			Sentinel1Calibrator sentinel1Calibrator = new Sentinel1Calibrator(calMeatadata);
//			MyTile targetTile = new MyTile(trgImgMetadataCal2.getWritableRaster(targetPoint.x, targetPoint.y),
//					trgImgMetadataCal2.getRectangle(targetPoint.x, targetPoint.y), trgImgMetadataCal2.getDataType());
//			sentinel1Calibrator.computeTile(tuple._2, null, targetTile, srcImgMetadataCal2.getNoDataValue(),
//					trgImgMetadataCal2.getBandName());
//			return new Tuple2<String, MyTile>(trgImgMetadataCal2.getBandName(), targetTile);
//
//		});
//
//		// check calibration
//		// List<Tuple2<Tuple2<Point, String>, MyTile>> masterCalres=
//		// masterRastersCal.collect();
//		// List<Tuple2<Tuple2<Point, String>, MyTile>>
//		// slaveCalres=slaveRastersCal.collect();
//		// Checks.checkCalibration(masterCalres, slaveCalres);
//
//		JavaPairRDD<Tuple3<Point, String, Rectangle>, MyTile> dependentPairs = slaveRastersCal
//				.flatMapToPair((Tuple2<String, MyTile> pair) -> {
//					List<Tuple2<Tuple3<Point, String, Rectangle>, MyTile>> pairs = new ArrayList<Tuple2<Tuple3<Point, String, Rectangle>, MyTile>>();
//					Map<String, List<Tuple2<Point, Rectangle>>> dependRectsMap = dependRectsB.getValue();
//					List<Tuple2<Point, Rectangle>> dependRectangles = dependRectsMap.get(pair._1);
//					for (int i = 0; i < dependRectangles.size(); i++) {
//						Tuple2<Point, Rectangle> tuple = dependRectangles.get(i);
//						if (tuple._2().intersects(pair._2.getRectangle())) {
//							pairs.add(new Tuple2<Tuple3<Point, String, Rectangle>, MyTile>(
//									new Tuple3<Point, String, Rectangle>(tuple._1(), pair._1(), tuple._2()), pair._2));
//						}
//					}
//					return pairs;
//				});
//				
//
//		JavaPairRDD<Tuple2<Point, String>, MyTile> createstackResults  = dependentPairs.groupByKey()
//				.mapToPair((Tuple2<Tuple3<Point, String, Rectangle>, Iterable<MyTile>> pair) -> {
//					Rectangle dependentRect = pair._1._3();
//					Iterator<MyTile> iterator = pair._2.iterator();
//					boolean first = true;
//					MyTile sourceTile = null;
//					Rectangle currRect = null;
//					WritableRaster destinationRaster = null;
//					int tileType = -1;
//					while (iterator.hasNext()) {
//						MyTile myTile = (MyTile) iterator.next();
//						tileType = myTile.getType();
//						currRect = dependentRect.intersection(myTile.getRectangle());
//						if (first && !iterator.hasNext()) {
//							WritableRaster currRaster = myTile.getWritableRaster();
//							Raster sourceRaster = currRaster.getBounds().equals(currRect) ? currRaster
//									: currRaster.createChild(currRect.x, currRect.y, currRect.width, currRect.height,
//											currRect.x, currRect.y, null);
//							destinationRaster = Raster.createWritableRaster(sourceRaster.getSampleModel(),
//									sourceRaster.getDataBuffer(), currRaster.getBounds().getLocation());
//							break;
//						} else {
//							if (first) {
//
//								destinationRaster = Utils.createWritableRaster(dependentRect, myTile.getType());
//								first = false;
//							}
//							Object dataBuffer = ImageUtils.createDataBufferArray(
//									myTile.getWritableRaster().getTransferType(),
//									(int) currRect.getWidth() * (int) currRect.getHeight());
//							myTile.getWritableRaster().getDataElements((int) currRect.getMinX(),
//									(int) currRect.getMinY(), (int) currRect.getWidth(), (int) currRect.getHeight(),
//									dataBuffer);
//							destinationRaster.setDataElements((int) currRect.getMinX(), (int) currRect.getMinY(),
//									(int) currRect.getWidth(), (int) currRect.getHeight(), dataBuffer);
//						}
//					}
//					sourceTile = new MyTile(destinationRaster, dependentRect, tileType);
//					
//					//compute tile, createstack
//					CreateStack createStack = new CreateStack();
//					ImageMetadata trgImgMetadataStack = imgMetadataStackB.getValue()
//							.get(sourceTargetMapStackB.getValue().get(pair._1._2()+"_"+"stack")+"_"+"stack");
//					ImageMetadata srcImgMetadataStack = imgMetadataStackB.getValue().get(pair._1._2()+"_"+"stack");
//					MyTile targetTile = new MyTile(
//							trgImgMetadataStack.getWritableRaster(pair._1._1().x, pair._1._1().y),
//							trgImgMetadataStack.getRectangle(pair._1._1().x, pair._1._1().y),
//							trgImgMetadataStack.getDataType());
//					createStack.computeTile(targetTile, sourceTile, slaveBandsOffset.getValue().get(pair._1._2()+"_"+"stack"),
//								srcImgMetadataStack.getImageWidth(), srcImgMetadataStack.getImageHeight(),
//								trgImgMetadataStack.getNoDataValue());
//					return new Tuple2<Tuple2<Point, String>, MyTile>(
//							new Tuple2<Point, String>(pair._1._1(), trgImgMetadataStack.getBandName()), targetTile);
//				});
//	
////		// List<Tuple2<Tuple3<Point, String, Rectangle>, MyTile>>
////		// createStackRes= createstackDepTiles.collect();
////		// Checks.checkDepends(createStackRes);
////		// JavaPairRDD<Tuple2<Point, String>, MyTile>
////		JavaPairRDD<Tuple2<Point, String>, MyTile> createstackResults = createstackDepTiles
////				.mapToPair((Tuple2<Tuple3<Point, String, Rectangle>, MyTile> pair) -> {
////
////					CreateStack createStack = new CreateStack();
////					ImageMetadata trgImgMetadataStack = imgMetadataStackB.getValue()
////							.get(sourceTargetMapStackB.getValue().get(pair._1._2()));
////					ImageMetadata srcImgMetadataStack = imgMetadataStackB.getValue().get(pair._1._2());
////					MyTile targetTile = new MyTile(
////							trgImgMetadataStack.getWritableRaster(pair._1._1().x, pair._1._1().y),
////							trgImgMetadataStack.getRectangle(pair._1._1().x, pair._1._1().y),
////							trgImgMetadataStack.getDataType());
////					try {
////						createStack.computeTile(targetTile, pair._2, slaveBandsOffset.getValue().get(pair._1._2()),
////								srcImgMetadataStack.getImageWidth(), srcImgMetadataStack.getImageHeight(),
////								trgImgMetadataStack.getNoDataValue());
////					} catch (ArrayIndexOutOfBoundsException e) {
////
////						System.out.println("key: " + pair._1);
////						System.out.println("");
////					}
////					return new Tuple2<Tuple2<Point, String>, MyTile>(
////							new Tuple2<Point, String>(pair._1._1(), trgImgMetadataStack.getBandName()), targetTile);
////				});
//
//
//		// check createstack
////		 List<Tuple2<Tuple2<Point, String>, MyTile>> createStackRes2=
////		 createstackResults.collect();
////		 Checks.checkCreatestack(createStackRes2);
////
//		JavaPairRDD<Tuple2<Integer, String>, MyTile> createstackResultsRows = createstackResults
//				.filter((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
//					Map<String, String> bandsList = bandsListGCPB.getValue();
//					return bandsList.containsKey(pair._1._2);
//				}).flatMapToPair((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
//					List<Tuple2<Tuple2<Integer, String>, MyTile>> pairs = new ArrayList<Tuple2<Tuple2<Integer, String>, MyTile>>();
//					int rowsCount = rows.getValue();
//					int nOfKeys = (int) Math.ceil((float) rowsCount / (float) 4);
//					int key = 0;
//					for (int i = 1; i <= nOfKeys; i++) {
//						if (pair._1._1.y < 4 * i && pair._1._1.y >= 4 * (i - 1)) {
//							key = i;
//							pairs.add(new Tuple2<Tuple2<Integer, String>, MyTile>(
//									new Tuple2<Integer, String>(key, pair._1._2), pair._2));
//							if ((pair._1._1.y == 4 * i - 1) || (pair._1._1.y == 4 * i - 2))
//								pairs.add(new Tuple2<Tuple2<Integer, String>, MyTile>(
//										new Tuple2<Integer, String>(key + 1, pair._1._2), pair._2));
//						}
//					}
//
//					return pairs;
//				});
//
//		JavaPairRDD<Tuple2<Integer, String>, MyTile> masterRastersRdd2 = masterRastersCal
//				.filter((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
//					String name = sourceTargetMapStackB.getValue().get(pair._1._2+"_stack");
//					Map<String, String> bandsList = bandsListGCPB.getValue();
//					return bandsList.containsKey(name);
//				}).flatMapToPair((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
//					List<Tuple2<Tuple2<Integer, String>, MyTile>> pairs = new ArrayList<Tuple2<Tuple2<Integer, String>, MyTile>>();
//					ImageMetadata trgImgMetadataCal1 = masterImgMetadataCalB.getValue().get(pair._1._2+"_cal1");
//					int rowsCount = rows.getValue();
//					int nOfKeys = (int) Math.ceil((float) rowsCount / (float) 4);
//					int key = 0;
//					int y = trgImgMetadataCal1.getTileIndices(pair._2.getMinX(), pair._2.getMinY()).y;
//					for (int i = 1; i <= nOfKeys; i++) {
//						if (y < 4 * i && y >= 4 * (i - 1)) {
//							key = i;
//							pairs.add(new Tuple2<Tuple2<Integer, String>, MyTile>(
//									new Tuple2<Integer, String>(key, sourceTargetMapStackB.getValue().get(pair._1._2+"_stack")),
//									pair._2));
//							if ((y == 4 * i - 1) || (y == 4 * i - 2))
//								pairs.add(new Tuple2<Tuple2<Integer, String>, MyTile>(new Tuple2<Integer, String>(
//										key + 1, sourceTargetMapStackB.getValue().get(pair._1._2+"_stack")), pair._2));
//						}
//					}
//
//					return pairs;
//
//				});
//		JavaPairRDD<Tuple2<Integer, String>, Iterable<MyTile>> masterRows = masterRastersRdd2.groupByKey();
//		JavaPairRDD<Tuple2<Integer, String>, Iterable<MyTile>> stacktilesRows = createstackResultsRows.groupByKey();
//		List<Tuple2<String, Tuple2<Integer, Placemark>>> slaveGCPs = masterRows.join(stacktilesRows)
//				.flatMap((Tuple2<Tuple2<Integer, String>, Tuple2<Iterable<MyTile>, Iterable<MyTile>>> pair) -> {
//					List<MyTile> masterTiles = Lists.newArrayList(pair._2._1.iterator());
//					List<MyTile> slaveTiles = Lists.newArrayList(pair._2._2.iterator());
//					int x = masterTiles.get(0).getMinX();
//					int y = masterTiles.get(0).getMinY();
//					int width = 0;
//					int height = 0;
//					int type = 0;
//					for (int i = 1; i < masterTiles.size(); i++) {
//						MyTile masterTile = masterTiles.get(i);
//						if (masterTile.getMinX() < x)
//							x = masterTile.getMinX();
//						if (masterTile.getMinY() < y)
//							y = masterTile.getMinY();
//						type = masterTile.getType();
//					}
//					for (int i = 0; i < masterTiles.size(); i++) {
//						MyTile masterTile = masterTiles.get(i);
//						if (masterTile.getMinX() == x)
//							height += masterTile.getHeight();
//						if (masterTile.getMinY() == y)
//							width += masterTile.getWidth();
//					}
//					WritableRaster masterRaster = Utils.createWritableRaster(new Rectangle(x, y, width, height), type);
//					for (int i = 0; i < masterTiles.size(); i++) {
//						MyTile t = masterTiles.get(i);
//						masterRaster.setDataElements(masterTiles.get(i).getMinX(), masterTiles.get(i).getMinY(),
//								masterTiles.get(i).getWidth(), masterTiles.get(i).getHeight(),
//								masterTiles.get(i).getRawSamples().getElems());
//					}
//					x = slaveTiles.get(0).getMinX();
//					y = slaveTiles.get(0).getMinY();
//					width = 0;
//					height = 0;
//					for (int i = 1; i < slaveTiles.size(); i++) {
//						MyTile slaveTile = slaveTiles.get(i);
//						if (slaveTile.getMinX() < x)
//							x = slaveTile.getMinX();
//						if (slaveTile.getMinY() < y)
//							y = slaveTile.getMinY();
//						type = slaveTile.getType();
//					}
//					for (int i = 0; i < slaveTiles.size(); i++) {
//						MyTile slaveTile = slaveTiles.get(i);
//						if (slaveTile.getMinX() == x)
//							height += slaveTile.getHeight();
//						if (slaveTile.getMinY() == y)
//							width += slaveTile.getWidth();
//					}
//					WritableRaster slaveRaster = Utils.createWritableRaster(new Rectangle(x, y, width, height), type);
//					for (int i = 0; i < slaveTiles.size(); i++) {
//						slaveRaster.setDataElements(slaveTiles.get(i).getMinX(), slaveTiles.get(i).getMinY(),
//								slaveTiles.get(i).getWidth(), slaveTiles.get(i).getHeight(),
//								slaveTiles.get(i).getRawSamples().getElems());
//					}
//					MyTile masterTile = new MyTile(masterRaster,
//							new Rectangle(x, y, masterRaster.getWidth(), masterRaster.getHeight()), type);
//					MyTile slaveTile = new MyTile(slaveRaster,
//							new Rectangle(x, y, slaveRaster.getWidth(), slaveRaster.getHeight()), type);
//
//					List<Tuple2<String, Tuple2<Integer, Placemark>>> slaveGCPsRes = new ArrayList<Tuple2<String, Tuple2<Integer, Placemark>>>();
//					ProductNodeGroup<Placemark> masterGcpGroup = masterGcps.getValue();
//					final int numberOfMasterGCPs = masterGcpGroup.getNodeCount();
//					GCPMetadata GCPMetadataBroad2 = GCPMetadataBroad.getValue();
//
//					ImageMetadata trgImgMetadataGCP = imgMetadataGCPB.getValue().get(pair._1._2 +"_gcp"+ "_target");
//					ImageMetadata srcImgMetadataGCP = imgMetadataGCPB.getValue().get(pair._1._2 +"_gcp"+ "_source");
//
//					int[] iParams2 = { Integer.parseInt(GCPMetadataBroad2.getCoarseRegistrationWindowWidth()),
//							Integer.parseInt(GCPMetadataBroad2.getCoarseRegistrationWindowHeight()),
//							GCPMetadataBroad2.getMaxIteration(),
//							Integer.parseInt(GCPMetadataBroad2.getRowInterpFactor()),
//							Integer.parseInt(GCPMetadataBroad2.getColumnInterpFactor()),
//							srcImgMetadataGCP.getImageWidth(), srcImgMetadataGCP.getImageHeight() };
//					double[] dParams3 = { GCPMetadataBroad2.getGcpTolerance(), trgImgMetadataGCP.getNoDataValue(),
//							srcImgMetadataGCP.getNoDataValue() };
//
//					final int[] offset2 = new int[2];
//					for (int i = 0; i < numberOfMasterGCPs; i++) {
//						final Placemark mPin = masterGcpGroup.get(i);
//
//						final PixelPos sGCPPixelPos = new PixelPos(mPin.getPixelPos().x + offset2[0],
//								mPin.getPixelPos().y + offset2[1]);
//						if (masterTile.getRectangle().contains(new Point((int) sGCPPixelPos.x, (int) sGCPPixelPos.y))) {
//							GCPSelection GCPSelection = new GCPSelection(iParams2, dParams3,
//									trgImgMetadataGCP.getGeoCoding(), masterTile, slaveTile);
//							if (GCPSelection.checkMasterGCPValidity(mPin)
//									&& GCPSelection.checkSlaveGCPValidity(sGCPPixelPos)) {
//								Placemark sPin = GCPSelection.computeSlaveGCP(mPin, sGCPPixelPos);
//
//								if (sPin != null)
//									slaveGCPsRes.add(new Tuple2<String, Tuple2<Integer, Placemark>>(pair._1._2,
//											new Tuple2<Integer, Placemark>(i, sPin)));
//							}
//
//						}
//					}
//					return slaveGCPsRes;
//				}).collect();
////		
//		//imageMetadataStack = null;
//		imageMetadataGCP = null;
//		sourceTargetMapCal1 = null;
//		sourceTargetMapCal2 = null;
//		//sourceTargetMapstack = null;
//		//System.gc();
//		Map<String, Map<Integer, Placemark>> gcpsMap = new HashMap<String, Map<Integer, Placemark>>();
//		for (int i = 0; i < slaveGCPs.size(); i++) {
//			String bandName = slaveGCPs.get(i)._1;
//			if (gcpsMap.containsKey(bandName)) {
//				gcpsMap.get(bandName).put(slaveGCPs.get(i)._2._1, slaveGCPs.get(i)._2._2);
//			} else {
//				Map<Integer, Placemark> placemarksMap = new HashMap<Integer, Placemark>();
//				placemarksMap.put(slaveGCPs.get(i)._2._1, slaveGCPs.get(i)._2._2);
//				gcpsMap.put(bandName, placemarksMap);
//			}
//		}
//			//Checks.checkGCPs(gcpsMap, gcpSelectionOp.getMasterGcpGroup());
//		for (String name : bandsListGCP.keySet()) {
//			final ProductNodeGroup<Placemark> targetGCPGroup = GCPManager.instance()
//					.getGcpGroup(gcpSelectionOp.getTargetProduct().getBand(name));
//			Map<Integer, Placemark> map = gcpsMap.get(name);
//			for (Placemark p : map.values()) {
//				targetGCPGroup.add(p);
//			}
//		}
////
//		long startWarpTime = System.currentTimeMillis();
//		warpOp.getWarpData();
//		Map<String, WarpData> warpdataMap = new HashMap<String, WarpData>();
//		Product targetProductWarp = warpOp.getTargetProduct();
//		String[] masterBandNamesWarp = StackUtils.getMasterBandNames(targetProductWarp);
//		Set<String> masterBandsWarp = new HashSet(Arrays.asList(masterBandNamesWarp));
//		for (int i = 0; i < targetProductWarp.getNumBands(); i++) {
//			if (masterBandsWarp.contains(targetProductWarp.getBandAt(i).getName()))
//				continue;
//			Band srcBandWarp = warpOp.getSourceRasterMap().get(warpOp.getTargetProduct().getBandAt(i));
//			Band realSrcBandWarp = warpOp.getComplexSrcMap().get(srcBandWarp);
//			if (realSrcBandWarp == null) {
//				realSrcBandWarp = srcBandWarp;
//			}
//			WarpData warpData = warpOp.getWarpDataMap().get(realSrcBandWarp);
//			warpdataMap.put(targetProductWarp.getBandAt(i).getName(), warpData);
//		}
//		Broadcast<Map<String, WarpData>> warpDataMapB = sc.broadcast(warpdataMap);
//
//		//Map<String, Set<Tuple2<Point, Rectangle>>> dependRectsWarp = new HashMap<String, Set<Tuple2<Point, Rectangle>>>(
//			//	2);
//		Map<Tuple2<String,Rectangle>, List<Point>> dependRectsWarp2 = new HashMap<Tuple2<String,Rectangle>, List<Point>>();
//		for (int i = 0; i < targetProductWarp.getNumBands(); i++) {
//			if (masterBandsWarp.contains(targetProductWarp.getBandAt(i).getName()))
//				continue;
//			String bandName = targetProductWarp.getBandAt(i).getName();
//			targetProductWarp.getBandAt(i).setSourceImage(sp.createSourceImages(targetProductWarp.getBandAt(i)));
//			final RenderedOp warpedImage = warpOp.createWarpImage(
//					warpdataMap.get(targetProductWarp.getBandAt(i).getName()).jaiWarp,
//					(RenderedImage) targetProductWarp.getBandAt(i).getSourceImage());
//
//			ImageMetadata trgImgMetadataWarp = imageMetadataWarp
//					.get(targetProductWarp.getBandAt(i).getName() +"_warp"+ "_target");
//			WarpOpImage wimg = (WarpOpImage) warpedImage.getRendering();
//			for (int tileY = 0; tileY < limits2.getNumYTiles(); tileY++) {
//				for (int tileX = 0; tileX < limits2.getNumXTiles(); tileX++) {
//					Point[] points = warpedImage.getTileIndices(trgImgMetadataWarp.getRectangle(tileX, tileY));
//					Rectangle finalRect = null;
//					for (int j = 0; j < points.length; j++) {
//						Rectangle rect = wimg.getTileRect(points[j].x, points[j].y);
//						Rectangle rect2 = wimg.mapDestRect(rect, 0);
//						if (j == 0)
//							finalRect = rect2;
//						else
//							finalRect = finalRect.union(rect2);
//
//					}
//					if (finalRect != null) {
////						if (dependRectsWarp.containsKey(bandName)) {
////							dependRectsWarp.get(bandName)
////									.add(new Tuple2<Point, Rectangle>(new Point(tileX, tileY), finalRect));
//						if (dependRectsWarp2.containsKey(new Tuple2<String,Rectangle>(bandName,finalRect))) {
////							dependRectsWarp.get(bandName)
////									.add(new Tuple2<Point, Rectangle>(new Point(tileX, tileY), finalRect));
//							dependRectsWarp2.get(new Tuple2<String,Rectangle>(bandName,finalRect))
//							.add(new Point(tileX, tileY));
//						} else {
////							Set<Tuple2<Point, Rectangle>> rectsList = new HashSet<Tuple2<Point, Rectangle>>();
////							rectsList.add(new Tuple2<Point, Rectangle>(new Point(tileX, tileY), finalRect));
//							//dependRectsWarp.put(bandName, rectsList);
//							List<Point> rectsList = new ArrayList<Point>();
//							rectsList.add(new Point(tileX, tileY));
//							dependRectsWarp2.put(new Tuple2<String,Rectangle>(bandName,finalRect), rectsList);
//						}
//					}
//				}
//
//				// ImageMetadata warpImgMetadata=new
//				// ImageMetadata(warpedImage.getWidth(),warpedImage.getHeight());
//			}
//		}
//////		//Broadcast<Map<String, Set<Tuple2<Point, Rectangle>>>> dependRectsWarpB = sc.broadcast(dependRectsWarp);
//		Broadcast<Map<Tuple2<String,Rectangle>, List<Point>>> dependRectsWarpB2 = sc.broadcast(dependRectsWarp2);
////////		JavaPairRDD<Tuple3<Point, String, Rectangle>, MyTile> dependentPairsWarp = createstackResults
////////				.flatMapToPair((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
////////					List<Tuple2<Tuple3<Point, String, Rectangle>, MyTile>> pairs = new ArrayList<Tuple2<Tuple3<Point, String, Rectangle>, MyTile>>();
////////					Map<String, Set<Tuple2<Point, Rectangle>>> dependRectsMap = dependRectsWarpB.getValue();
////////					Set<Tuple2<Point, Rectangle>> dependRectangles = dependRectsMap.get(pair._1._2);
////////					Iterator<Tuple2<Point, Rectangle>> it = dependRectangles.iterator();
////////					while (it.hasNext()) {
////////						Tuple2<Point, Rectangle> tuple = it.next();
////////						if (tuple._2().intersects(pair._2.getRectangle())) {
//////////							Rectangle rect=pair._2.getRectangle();
//////////							if(tuple._1().x==5&&tuple._1().y==4){
//////////								System.out.println("tile "+pair._1()+" rectangle "+rect);
//////////								}
////////							pairs.add(new Tuple2<Tuple3<Point, String, Rectangle>, MyTile>(
////////									new Tuple3<Point, String, Rectangle>(tuple._1(), pair._1._2(), tuple._2()),
////////									pair._2));
////////						}
////////					}
////////					return pairs;
////////				});
//////		
//		JavaPairRDD<Tuple2<String, Rectangle>, MyTile> dependentPairsWarp = createstackResults
//				.flatMapToPair((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
//					List<Tuple2<Tuple2<String, Rectangle>, MyTile>> pairs = new ArrayList<Tuple2<Tuple2< String, Rectangle>, MyTile>>();
//					Map<Tuple2<String,Rectangle>, List<Point>> dependRectsMap = dependRectsWarpB2.getValue();
//					for(Tuple2<String,Rectangle> tuple: dependRectsMap.keySet()) {
//						if (tuple._2().intersects(pair._2.getRectangle())&&tuple._1().equals(pair._1._2)) {
////							Rectangle rect=pair._2.getRectangle();
////							if(tuple._1().x==5&&tuple._1().y==4){
////								System.out.println("tile "+pair._1()+" rectangle "+rect);
////								}
//							pairs.add(new Tuple2<Tuple2<String, Rectangle>, MyTile>(
//									new Tuple2<String, Rectangle>(tuple._1, tuple._2()),
//									pair._2));
//						}
//					}
//					return pairs;
//				});
//		long endWarpTime = System.currentTimeMillis();
//		long totalWarpTime = endWarpTime - startWarpTime;
//		System.out.println("warp time "+totalWarpTime);
//////		JavaPairRDD<Tuple2<Point, String>,MyTile> warpResults = dependentPairsWarp.groupByKey().mapToPair((Tuple2<Tuple3<Point, String, Rectangle>, Iterable<MyTile>> pair) -> {
//////
//////			ImageMetadata srcImgMetadataWarp=imgMetadataWarpB.getValue().get(pair._1._2()+"_source");
//////			
//////			int bufferType = ImageManager.getDataBufferType(srcImgMetadataWarp.getDataType());
//////			final SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(bufferType, srcImgMetadataWarp.getImageWidth(), srcImgMetadataWarp.getImageHeight());
//////			final ColorModel cm = PlanarImage.createColorModel(sampleModel);
//////			WritableRaster raster = RasterFactory.createWritableRaster(sampleModel, new Point(0, 0));
//////			BufferedImage img = new BufferedImage(cm, raster, false, new java.util.Hashtable());
//////			Iterator<MyTile> it = pair._2.iterator();
//////			while (it.hasNext()) {
//////				MyTile myTile = it.next();
//////				img.getRaster().setDataElements(myTile.getMinX(), myTile.getMinY(), myTile.getWidth(),
//////						myTile.getHeight(), myTile.getRawSamples().getElems());
//////
//////			}
//////			WarpMetadata w1 = warpMetadataB.getValue();
//////			Map<String,WarpData> map=warpDataMapB.getValue();
//////			WarpData w =map.get(pair._1._2());
//////			Warp warp = new Warp(w1.getInterp(), w, w1.getInterpTable());
//////			// get warped image
//////			ImageMetadata trgImgMetadataWarp=imgMetadataWarpB.getValue().get(pair._1._2()+"_target");
//////			MyTile targetTile = new MyTile(
//////							trgImgMetadataWarp.getWritableRaster(pair._1._1().x, pair._1._1().y),
//////							trgImgMetadataWarp.getRectangle(pair._1._1().x, pair._1._1().y),
//////							trgImgMetadataWarp.getDataType());
//////					warp.computeTile(targetTile, img);
//////
//////			return new Tuple2<Tuple2<Point, String>, MyTile>(new Tuple2<Point, String>(pair._1._1(),pair._1._2()), targetTile);
//////		});
//		JavaPairRDD<Tuple2<Point, String>,MyTile> warpResults = dependentPairsWarp.groupByKey().flatMapToPair((Tuple2<Tuple2<String, Rectangle>, Iterable<MyTile>> pair) -> {
//			List<Tuple2<Tuple2<Point, String>,MyTile>> trgtiles=new ArrayList<Tuple2<Tuple2<Point, String>,MyTile>>();
//			Map<Tuple2<String,Rectangle>, List<Point>> dependRectsMap = dependRectsWarpB2.getValue();
//			ImageMetadata srcImgMetadataWarp=imgMetadataWarpB.getValue().get(pair._1._1()+"_warp"+"_source");
//			
//			int bufferType = ImageManager.getDataBufferType(srcImgMetadataWarp.getDataType());
//			final SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(bufferType, srcImgMetadataWarp.getImageWidth(), srcImgMetadataWarp.getImageHeight());
//			final ColorModel cm = PlanarImage.createColorModel(sampleModel);
//			WritableRaster raster = RasterFactory.createWritableRaster(sampleModel, new Point(0, 0));
//			BufferedImage img = new BufferedImage(cm, raster, false, new java.util.Hashtable());
//			Iterator<MyTile> it = pair._2.iterator();
//			while (it.hasNext()) {
//				MyTile myTile = it.next();
//				img.getRaster().setDataElements(myTile.getMinX(), myTile.getMinY(), myTile.getWidth(),
//						myTile.getHeight(), myTile.getRawSamples().getElems());
//
//			}
//			WarpMetadata w1 = warpMetadataB.getValue();
//			Map<String,WarpData> map=warpDataMapB.getValue();
//			WarpData w =map.get(pair._1._1());
//			Warp warp = new Warp(w1.getInterp(), w, w1.getInterpTable());
//			// get warped image
//			ImageMetadata trgImgMetadataWarp=imgMetadataWarpB.getValue().get(pair._1._1()+"_warp"+"_target");
//			
//			List<Point> targetPoints=dependRectsMap.get(pair._1);
//			for(Point p: targetPoints){
//			MyTile targetTile = new MyTile(
//							trgImgMetadataWarp.getWritableRaster(p.x, p.y),
//							trgImgMetadataWarp.getRectangle(p.x, p.y),
//							trgImgMetadataWarp.getDataType());
//					warp.computeTile(targetTile, img);
//
//			trgtiles.add( new Tuple2<Tuple2<Point, String>, MyTile>(new Tuple2<Point, String>(p,pair._1._1()), targetTile));
//			}		
//			return trgtiles;
//		});
//		 List<Tuple2<Tuple2<Point, String>, MyTile>>
//		 warpRes=warpResults.collect();
//		 dependRectsWarpB2.destroy();
//			dependRectsWarp2=null;
//			dependRectsB.destroy();
//			dependRects = null;
//			System.gc();
////		 Checks.checkWarp(warpRes);
//		long endTime   = System.currentTimeMillis();
//		long totalTime = endTime - startTime;
//		System.out.println(totalTime);
	}

}
