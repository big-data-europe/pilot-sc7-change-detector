package eu.bde.sc7pilot.tilebased;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.ColorModel;
import java.awt.image.SampleModel;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.media.jai.PlanarImage;
import javax.media.jai.RenderedOp;
import javax.media.jai.TiledImage;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.esa.s1tbx.insar.gpf.coregistration.GCPManager;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Placemark;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductNodeGroup;
import org.esa.snap.core.image.ImageManager;
import org.esa.snap.core.util.ImageUtils;
import org.esa.snap.engine_utilities.gpf.OperatorUtils;
import org.esa.snap.engine_utilities.gpf.StackUtils;

import com.google.common.collect.Lists;

import eu.bde.sc7pilot.hdfsreader.BandInfo;
import eu.bde.sc7pilot.hdfsreader.ZipHandler2;
import eu.bde.sc7pilot.taskbased.LoopLimits;
import eu.bde.sc7pilot.taskbased.MyBandSelect;
import eu.bde.sc7pilot.taskbased.MyChangeDetection;
import eu.bde.sc7pilot.taskbased.MyCreateStack;
import eu.bde.sc7pilot.taskbased.MyGCPSelection;
import eu.bde.sc7pilot.taskbased.MyWarp;
import eu.bde.sc7pilot.taskbased.MyWarp.WarpData;
import eu.bde.sc7pilot.taskbased.MyWrite;
import eu.bde.sc7pilot.taskbased.SerialProcessor;
import eu.bde.sc7pilot.taskbased.calibration.MyCalibration;
import eu.bde.sc7pilot.tilebased.mappers.CalibrationMappers;
import eu.bde.sc7pilot.tilebased.mappers.CreateStackMappers;
import eu.bde.sc7pilot.tilebased.mappers.GCPMappers;
import eu.bde.sc7pilot.tilebased.metadata.CalibrationMetadata;
import eu.bde.sc7pilot.tilebased.metadata.ChangeDetectionMetadata;
import eu.bde.sc7pilot.tilebased.metadata.GCPMetadata;
import eu.bde.sc7pilot.tilebased.metadata.ImageMetadata;
import eu.bde.sc7pilot.tilebased.metadata.WarpMetadata;
import eu.bde.sc7pilot.tilebased.model.MyTile;
import eu.bde.sc7pilot.tilebased.operator.ChangeDetection;
import eu.bde.sc7pilot.tilebased.operator.Warp;
import eu.bde.sc7pilot.tilebased.operator.Write;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import scala.Tuple2;
import scala.Tuple3;

public class TileBasedFinal {
	
	public static void main(String[] args) throws Exception {
		// args[0]: dir in HDFS to store .tiff(s)
		// args[1]: .dim (master image) local filepath
		// args[2]: .tiff (master image) local filepath
		// args[3]: .dim (slave image) local filepath
		// args[4]: .tiff (slave image) local filepath
		// args[5]: dir local filepath to write the final result
		// args[6]: integer defining partition number (8 - 36, preferably 24)
		if (args.length < 7) {
			System.out.println("args[0]: dir in HDFS to store .tiff(s)");
			System.out.println("args[1]: .dim (master image) local filepath");
			System.out.println("args[2]: .tiff (master image) local filepath");
			System.out.println("args[3]: .dim (slave image) local filepath");
			System.out.println("args[4]: .tiff (slave image) local filepath");
			System.out.println("args[5]: dir local filepath to write the final result");
			System.out.println("args[6]: integer defining partition number (8 - 36, preferably 24)");
			throw new IOException("Error: Invalid Args");
			
		}
		String devsMSG = "[Dev's MSG]\t";
		
		TileBasedFinal parallelTiles = new TileBasedFinal();
		
		long startAll = System.currentTimeMillis();
		
		parallelTiles.processTiles(args[0], args[1], args[2], args[3], args[4], args[5], Integer.parseInt(args[6]));
		
		long endAll = System.currentTimeMillis();
		long totalAll = endAll - startAll;
        System.out.println(devsMSG + totalAll + " ms [time4], for storing to HDFS and running all operators including Write (except deleting unwanted files).\n");
        
        //***Deleting unnecessary files***
        File masterTiff = new File(args[2]);
        File slaveTiff = new File(args[4]);
        if (masterTiff.exists()) {
        	masterTiff.delete();
        	System.out.println(devsMSG + masterTiff.getName() + " deleted succesfully!");
        }
        else {
        	System.out.println(devsMSG + "Cannot delete: " + masterTiff.getName());
        }
        if (slaveTiff.exists()) {
        	slaveTiff.delete();
        	System.out.println(devsMSG + slaveTiff.getName() + " deleted succesfully!\n");
        }
        else {
        	System.out.println(devsMSG + "Cannot delete: " + slaveTiff.getName() + "\n");
        }
        
	}

	public void processTiles(String hdfsPath, String masterDimFilePath, String masterTiffFilePath, String slaveDimFilePath, String slaveTiffFilePath, String targetPath, int partitionsNumber)
			throws Exception {
		String devsMSG = "[Dev's MSG]\t";
		//***Storing tiffs to HDFS***
		ZipHandler2 zipHandler = new ZipHandler2();
//		String masterTiffInHDFS = "/media/indiana/data/imgs/subseting/subset03_SaoPaulo/subset_of_S1A_S6_GRDH_1SDV_20160815T214331_20160815T214400_012616_013C9D_2495.tif";
		String masterTiffInHDFS = zipHandler.tiffLocalToHDFS(masterTiffFilePath, hdfsPath);
		System.out.println(devsMSG + "Master's tiff HDFS-URI: " + masterTiffInHDFS);
//		String slaveTiffInHDFS = "/media/indiana/data/imgs/subseting/subset03_SaoPaulo/subset_of_S1A_S6_GRDH_1SDV_20160908T214332_20160908T214401_012966_014840_ABDC.tif";
		String slaveTiffInHDFS = zipHandler.tiffLocalToHDFS(slaveTiffFilePath, hdfsPath);
		System.out.println(devsMSG + "Slave's tiff HDFS-URI: " + slaveTiffInHDFS + "\n");	
		
		System.out.println(devsMSG + "Serial Processing to acquire metadata...\n\n");
		long startProcessing = System.currentTimeMillis();
		
		/* Extracting metadata through serial processing */
		List<Tuple2<String, Point>> slaveIndices = null;
		List<Tuple2<String, Point>> masterIndices = null;
		Map<String, BandInfo> bandInfos = new HashMap<>(2);
		Object2ObjectMap<String, Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>>> dependRects = new Object2ObjectOpenHashMap<String, Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>>>();
		
		SerialProcessor sp = new SerialProcessor();
		//String[] selectedPolarisations = null;
		String[] selectedPolarisations= {"VV"};
		Product masterTargetProduct = null;
		Product slaveTargetProduct = null;

		masterTargetProduct = zipHandler.findTargetProduct(masterDimFilePath);
		for (int i = 0; i < masterTargetProduct.getNumBands(); i++) {
			Band band = masterTargetProduct.getBandAt(i);
			if (selectedPolarisations != null) {
                Set<String> selectedPols = new HashSet(Arrays.asList(Arrays.stream(selectedPolarisations).map(s -> s.toLowerCase()).toArray(String[]::new)));
                String pol = OperatorUtils.getPolarizationFromBandName(band.getName());
                if (!selectedPols.contains(pol.toLowerCase())) {
                	continue;
                }
			}
			if (band.getClass() == Band.class) {
				BandInfo bandInfo = new BandInfo(masterTiffInHDFS, i, 0);
				bandInfos.put(band.getName() + "_read1", bandInfo);
			}
		}
		masterIndices = sp.readTilesIndices(masterTargetProduct, selectedPolarisations);

		slaveTargetProduct = zipHandler.findTargetProduct(slaveDimFilePath);
		for (int i = 0; i < slaveTargetProduct.getNumBands(); i++) {
			Band band = slaveTargetProduct.getBandAt(i);
			if (selectedPolarisations != null) {
                Set<String> selectedPols = new HashSet(Arrays.asList(Arrays.stream(selectedPolarisations).map(s -> s.toLowerCase()).toArray(String[]::new)));
                String pol = OperatorUtils.getPolarizationFromBandName(band.getName());
                if (!selectedPols.contains(pol.toLowerCase())) {
                	continue;
                }
			}
			if (band.getClass() == Band.class) {
				BandInfo bandInfo = new BandInfo(slaveTiffInHDFS, i, 0);
				bandInfos.put(band.getName() + "_read2", bandInfo);
			}
		}
		slaveIndices = sp.readTilesIndices(slaveTargetProduct, selectedPolarisations);

		MyBandSelect bandselect1 = new MyBandSelect(selectedPolarisations, null);
		bandselect1.setSourceProduct(masterTargetProduct);
		sp.initOperator(bandselect1);
		MyBandSelect bandselect2 = new MyBandSelect(selectedPolarisations, null);
		bandselect2.setSourceProduct(slaveTargetProduct);
		sp.initOperator(bandselect2);

		Boolean[] bParams1 = {false, false, false, false, true, false, false, false};
		MyCalibration myCalibration1 = new MyCalibration(null, bParams1, selectedPolarisations);
		myCalibration1.setSourceProduct(bandselect1.getTargetProduct());
		myCalibration1.setId("cal1");
		sp.initOperator(myCalibration1);

		MyCalibration myCalibration2 = new MyCalibration(null, bParams1, selectedPolarisations);
		myCalibration2.setSourceProduct(bandselect2.getTargetProduct());
		myCalibration2.setId("cal2");
		sp.initOperator(myCalibration2);

		Product[] sourcesForCreateStack = new Product[2];
		sourcesForCreateStack[0] = myCalibration1.getTargetProduct();
		sourcesForCreateStack[1] = myCalibration2.getTargetProduct();

		String[] parameters = {"NONE", "Master", "Orbit"};
		MyCreateStack myCreateStack = new MyCreateStack(parameters);
		myCreateStack.setSourceProduct(sourcesForCreateStack);
		myCreateStack.setId("stack");
		sp.initOperator(myCreateStack);

		boolean[] bParams = {false, false, false, false};
		int[] iParams = {2000, 10, 3};
		double[] dParams = {0.25, 0.6};
		String[] sParams = {"128", "128", "4", "4", "32", "32"};
		GCPMetadata GCPMetadata = new GCPMetadata(bParams, dParams, iParams, sParams);
		MyGCPSelection myGCPSelection = new MyGCPSelection(bParams, dParams, iParams, sParams);
		myGCPSelection.setSourceProduct(myCreateStack.getTargetProduct());
		myGCPSelection.setId("gcp");
		sp.initOperator(myGCPSelection);

		boolean[] bParams2 = { false, false };
		MyWarp myWarp = new MyWarp(bParams2, 0.05f, 1, "Bilinear interpolation");
		myWarp.setSourceProduct(myGCPSelection.getTargetProduct());
		myWarp.setId("warp");
		sp.initOperator(myWarp);

		boolean[] bParams3 = { false, false };
		float[] fParams = { 2.0f, -2.0f };
		MyChangeDetection myChangeDetection = new MyChangeDetection(bParams3, fParams, null);
		myChangeDetection.setSourceProduct(myWarp.getTargetProduct());
		myChangeDetection.setId("changeD");
		sp.initOperator(myChangeDetection);
		
		File targetFile = new File(targetPath, "SparkChangeDetResult");
		MyWrite writeOp = new MyWrite(myChangeDetection.getTargetProduct(), targetFile, "BEAM-DIMAP");
		writeOp.setId("write");
		sp.initOperator(writeOp);
		
		/* The imageMetadata class has replaced the band class and contains only the absolutely essential metadata for tile computations */
		Object2ObjectMap<String, ImageMetadata> imageMetadata = new Object2ObjectOpenHashMap<String, ImageMetadata>(myCalibration1.getTargetProduct().getNumBands() * 3);
		OpMetadataCreator opMetadataCreator = new OpMetadataCreator();
		Object2ObjectMap<String, CalibrationMetadata> calMetadata = new Object2ObjectOpenHashMap<String, CalibrationMetadata>(myCalibration1.getTargetProduct().getNumBands() * 4);
		// read metadata.
		opMetadataCreator.createProdImgMetadata(imageMetadata, masterTargetProduct, "read1", selectedPolarisations);
		opMetadataCreator.createProdImgMetadata(imageMetadata, slaveTargetProduct, "read2", selectedPolarisations);
		// calibration1 metadata.
		opMetadataCreator.createCalImgMetadata(imageMetadata, myCalibration1, calMetadata, bParams1);
		// calibration2 metadata.
		opMetadataCreator.createCalImgMetadata(imageMetadata, myCalibration2, calMetadata, bParams1);
		// createstack metadata.
		opMetadataCreator.createOpImgMetadata(imageMetadata, myCreateStack, true);
		TileBasedUtils.getDependRects(imageMetadata, myCreateStack, dependRects);
		LoopLimits limits = new LoopLimits(myCreateStack.getTargetProduct());
		// gcpmetadata metadata.
		opMetadataCreator.createOpImgMetadata(imageMetadata, myGCPSelection);
		Map<String, String> bandsListGCP = myGCPSelection.slavebandsForGCP();
		// Warp image metadata
		opMetadataCreator.createOpImgMetadata(imageMetadata, myWarp);
		WarpMetadata warpMetadata = opMetadataCreator.createWarpMetadata(myWarp);
		// Change detection metadata
		opMetadataCreator.createOpImgMetadata(imageMetadata, myChangeDetection);
		ChangeDetectionMetadata changeDMetadata = opMetadataCreator.createChangeDMetadata(myChangeDetection, bParams3);

		long endSerialProcessing = System.currentTimeMillis();
		long durationSerialProcessing = endSerialProcessing - startProcessing;
		System.out.println("\n" + devsMSG + durationSerialProcessing + " ms [time1], for extracting metadata through Serial Processing");
		
		System.out.println("\n" + devsMSG + "Parallel Processing using Spark...\n\n");		

		/* Parallel Processing using Spark */
//		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Parallelized at local mode"); //everywhere EXCEPT cluster
		SparkConf conf = new SparkConf().set("spark.driver.maxResultSize", "5g").setAppName("Parallel Change-Detector in Spark");

		/* configure spark to use Kryo serializer instead of the java serializer. */
		/* All classes that should be serialized by kryo, are registered in MyRegitration class */
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryo.registrator", "eu.bde.sc7pilot.tilebased.MyRegistrator").set("spark.kryoserializer.buffer.max", "2047m");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// broadcast image metadata
		Broadcast<Map<String, ImageMetadata>> imgMetadataB = sc.broadcast(imageMetadata);
		Broadcast<Map<String, CalibrationMetadata>> calMetadataB = sc.broadcast(calMetadata);
		Broadcast<Object2ObjectMap<String, Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>>>> dependRectsB = sc.broadcast(dependRects);
		Broadcast<ProductNodeGroup<Placemark>> masterGcps = sc.broadcast(myGCPSelection.getMasterGcpGroup());
		Broadcast<GCPMetadata> GCPMetadataBroad = sc.broadcast(GCPMetadata);
		Broadcast<Map<String, String>> bandsListGCPB = sc.broadcast(bandsListGCP);
		Broadcast<WarpMetadata> warpMetadataB = sc.broadcast(warpMetadata);
		Broadcast<ChangeDetectionMetadata> changeDMetadataB = sc.broadcast(changeDMetadata);
		Broadcast<Integer> rows = sc.broadcast(limits.getNumXTiles());
		Broadcast<Map<String, BandInfo>> bandInfosB = sc.broadcast(bandInfos);

		JavaPairRDD<String, Point> masterRastersRdd = sc.parallelizePairs(masterIndices).partitionBy(new HashPartitioner(partitionsNumber));
		JavaPairRDD<String, Point> slaveRastersRdd = sc.parallelizePairs(slaveIndices).partitionBy(new HashPartitioner(partitionsNumber));
		
		// master image calibration
		JavaPairRDD<Tuple2<Point, String>, MyTile> masterRastersCal = masterRastersRdd.mapPartitionsToPair((Iterator<Tuple2<String, Point>> iterator) -> {
			return CalibrationMappers.calibrationMaster(iterator, bandInfosB.getValue(), imgMetadataB.getValue(), calMetadataB.getValue());
			}
		).cache();
		
		JavaPairRDD<String, MyTile> slaveRastersCal = slaveRastersRdd.mapPartitionsToPair((Iterator<Tuple2<String, Point>> iterator) -> {
			return CalibrationMappers.calibrationSlave(iterator, bandInfosB.getValue(), imgMetadataB.getValue(), calMetadataB.getValue());
			}
		);
		
		JavaPairRDD<Tuple3<Point, String, Rectangle>, MyTile> dependentPairs = slaveRastersCal.flatMapToPair((Tuple2<String, MyTile> pair) -> {
			ImageMetadata srcImgMetadataStack = imgMetadataB.getValue().get(pair._1 + "_" + "stack");
			List<Tuple2<Tuple3<Point, String, Rectangle>, MyTile>> pairs = new ArrayList<Tuple2<Tuple3<Point, String, Rectangle>, MyTile>>();
			Object2ObjectMap<String, Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>>> dependRectsMap = dependRectsB.getValue();
			Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>> dependRectangles = dependRectsMap.get(pair._1);
			Point sourcePoint = srcImgMetadataStack.getTileIndices(pair._2.getMinX(), pair._2.getMinY());
			List<Tuple2<Point, Rectangle>> tuples = dependRectangles.get(sourcePoint);
			if(tuples!=null) {
				for (Tuple2<Point, Rectangle> tuple : tuples)
					pairs.add(new Tuple2<Tuple3<Point, String, Rectangle>, MyTile>(new Tuple3<Point, String, Rectangle>(tuple._1(), pair._1(), tuple._2()), pair._2));
			}
			return pairs;
			}
		);
		
		JavaPairRDD<Tuple2<Point, String>, MyTile> createstackResults = dependentPairs.groupByKey()
																		.mapToPair((Tuple2<Tuple3<Point, String, Rectangle>, Iterable<MyTile>> pair) -> {
			return CreateStackMappers.createStack(pair, imgMetadataB.getValue());
			}
		).cache();

		// split the createstack tiles in groups of rows with a unique key to each group
		JavaPairRDD<Tuple2<Integer, String>, MyTile> createstackResultsRows = createstackResults.filter((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
			Map<String, String> bandsList = bandsListGCPB.getValue();
			return bandsList.containsKey(pair._1._2);
			}
		).flatMapToPair((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
			return CreateStackMappers.mapToRows(pair, imgMetadataB.getValue(), rows.getValue());
			}
		);

		// split the master tiles in groups of rows with a unique key to each group
		JavaPairRDD<Tuple2<Integer, String>, MyTile> masterRastersRdd2 = masterRastersCal.filter((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
			String name = imgMetadataB.getValue().get(pair._1._2 + "_stack").getBandPairName();
			Map<String, String> bandsList = bandsListGCPB.getValue();
			return bandsList.containsKey(name);
			}
		).flatMapToPair((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
			return CalibrationMappers.mapToRows(pair, imgMetadataB.getValue(), rows.getValue());
			}
		);
		
		// gcps computation. group by key the groups of rows and and compute the gcps contained to each group. Then, collect the gcps to the master node.
		JavaPairRDD<Tuple2<Integer, String>, Iterable<MyTile>> masterRows = masterRastersRdd2.groupByKey();
		JavaPairRDD<Tuple2<Integer, String>, Iterable<MyTile>> stacktilesRows = createstackResultsRows.groupByKey();
		List<Tuple2<String, Tuple2<Integer, Placemark>>> slaveGCPs = masterRows.join(stacktilesRows)
																.flatMap((Tuple2<Tuple2<Integer, String>, Tuple2<Iterable<MyTile>, Iterable<MyTile>>> pair) -> {
			return GCPMappers.GCPSelection(pair, GCPMetadataBroad.getValue(), imgMetadataB.getValue(), masterGcps.getValue(), rows.getValue());
			}
		).collect();
		if(slaveGCPs.isEmpty())
		{
			System.out.println("\n" + devsMSG + "not enough GCPs detected");
			return;
		}
		
		// process put the gcps into a hashmap to eliminate some duplicates
		Map<String, Map<Integer, Placemark>> gcpsMap = new HashMap<String, Map<Integer, Placemark>>();
		for (int i = 0; i < slaveGCPs.size(); i++) {
			String bandName = slaveGCPs.get(i)._1;
			if (gcpsMap.containsKey(bandName)) {
				gcpsMap.get(bandName).put(slaveGCPs.get(i)._2._1, slaveGCPs.get(i)._2._2);
			} 
			else {
				Map<Integer, Placemark> placemarksMap = new HashMap<Integer, Placemark>();
				placemarksMap.put(slaveGCPs.get(i)._2._1, slaveGCPs.get(i)._2._2);
				gcpsMap.put(bandName, placemarksMap);
			}
		}
		for (String name : bandsListGCP.keySet()) {
			final ProductNodeGroup<Placemark> targetGCPGroup = GCPManager.instance().getGcpGroup(myGCPSelection.getTargetProduct().getBand(name));
			Map<Integer, Placemark> map = gcpsMap.get(name);
			for (Placemark p : map.values()) {
				targetGCPGroup.add(p);
			}
		}
		
		// compute the warp function
		myWarp.getWarpData();
		Map<String, WarpData> warpdataMap = new HashMap<String, WarpData>();
		Product targetProductWarp = myWarp.getTargetProduct();
		String[] masterBandNamesWarp = StackUtils.getMasterBandNames(targetProductWarp);
		Set<String> masterBandsWarp = new HashSet(Arrays.asList(masterBandNamesWarp));
		for (int i = 0; i < targetProductWarp.getNumBands(); i++) {
			if (masterBandsWarp.contains(targetProductWarp.getBandAt(i).getName()))
				continue;
			Band srcBandWarp = myWarp.getSourceRasterMap().get(myWarp.getTargetProduct().getBandAt(i));
			Band realSrcBandWarp = myWarp.getComplexSrcMap().get(srcBandWarp);
			if (realSrcBandWarp == null) {
				realSrcBandWarp = srcBandWarp;
			}
			WarpData warpData = myWarp.getWarpDataMap().get(realSrcBandWarp);
			warpdataMap.put(targetProductWarp.getBandAt(i).getName(), warpData);
		}
		
		Broadcast<Map<String, WarpData>> warpDataMapB = sc.broadcast(warpdataMap);
		Object2ObjectMap<String, Object2ObjectMap<Point, ObjectSet<Rectangle>>> dependRectsWarp2 = new Object2ObjectOpenHashMap<String, Object2ObjectMap<Point, ObjectSet<Rectangle>>>();
		Object2ObjectMap<String, Object2ObjectMap<Rectangle, ObjectList<Point>>> dependPointsWarp = new Object2ObjectOpenHashMap<String, Object2ObjectMap<Rectangle, ObjectList<Point>>>();
		TileBasedUtils.getSourceDependWarp(myWarp, warpdataMap, imageMetadata, dependRectsWarp2, dependPointsWarp);
		Broadcast<Object2ObjectMap<String, Object2ObjectMap<Point, ObjectSet<Rectangle>>>> dependRectsWarpB2 = sc.broadcast(dependRectsWarp2);
		Broadcast<Object2ObjectMap<String, Object2ObjectMap<Rectangle, ObjectList<Point>>>> dependPointsWarpB = sc.broadcast(dependPointsWarp);

		JavaPairRDD<Tuple2<String, Rectangle>, MyTile> dependentPairsWarp = createstackResults.flatMapToPair((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
			List<Tuple2<Tuple2<String, Rectangle>, MyTile>> pairs = new ArrayList<Tuple2<Tuple2<String, Rectangle>, MyTile>>();
			Object2ObjectMap<String, Object2ObjectMap<Point, ObjectSet<Rectangle>>> dependRectsMap = dependRectsWarpB2.getValue();
			Object2ObjectMap<Point, ObjectSet<Rectangle>> bandRects = dependRectsMap.get(pair._1._2);
			ImageMetadata srcImgMetadataWarp = imgMetadataB.getValue().get(pair._1._2 + "_warp" + "_source");
			Point sourcePoint = srcImgMetadataWarp.getTileIndices(pair._2.getMinX(), pair._2.getMinY());
			Set<Rectangle> tuples = bandRects.get(sourcePoint);
			if(tuples!=null) {
				for (Rectangle rect : tuples) {
					pairs.add(new Tuple2<Tuple2<String, Rectangle>, MyTile>(new Tuple2<String, Rectangle>(pair._1._2, rect), pair._2));
				}
			}
			return pairs;
			}
		);
		createstackResults.unpersist();
		
		JavaPairRDD<Tuple2<String, Rectangle>, Iterable<MyTile>> warpResults1 = dependentPairsWarp.groupByKey();
		JavaPairRDD<Tuple2<Point, String>, MyTile> warpResults = warpResults1.flatMapToPair((Tuple2<Tuple2<String, Rectangle>, Iterable<MyTile>> pair) -> {
			List<Tuple2<Tuple2<Point, String>, MyTile>> trgtiles = new ArrayList<Tuple2<Tuple2<Point, String>, MyTile>>();
			Object2ObjectMap<String, Object2ObjectMap<Rectangle, ObjectList<Point>>> dependRectsMap = dependPointsWarpB.getValue();
			Object2ObjectMap<Rectangle, ObjectList<Point>> pointsRect = dependRectsMap.get(pair._1._1);
			ImageMetadata srcImgMetadataWarp = imgMetadataB.getValue().get(pair._1._1() + "_warp" + "_source");
			int bufferType = ImageManager.getDataBufferType(srcImgMetadataWarp.getDataType());
			final SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(bufferType, srcImgMetadataWarp.getTileSize().width, srcImgMetadataWarp.getTileSize().height);
			final ColorModel cm = PlanarImage.createColorModel(sampleModel);
			TiledImage img = new TiledImage(0, 0, srcImgMetadataWarp.getImageWidth(), srcImgMetadataWarp.getImageHeight(), 0, 0, sampleModel, cm);
			List<MyTile> tiles = Lists.newArrayList(pair._2.iterator());
			List<Point> targetPoints = pointsRect.get(pair._1._2);
			for (MyTile myTile : tiles) {
				img.setData(myTile.getWritableRaster());
			}
			tiles = null;
			Map<String, WarpData> map = warpDataMapB.getValue();
			WarpData w = map.get(pair._1._1());
			Warp warp = new Warp(warpMetadataB.getValue().getInterp(), w, warpMetadataB.getValue().getInterpTable());
			
			// get warped image
			ImageMetadata trgImgMetadataWarp = imgMetadataB.getValue().get(pair._1._1() + "_warp" + "_target");
			Map<String, ImageMetadata> map2 = imgMetadataB.getValue();
			ImageMetadata stackImgMetadata = map2.get(pair._1._1() + "_" + "stack");
			RenderedOp warpedImage = warp.createWarpImage(warp.getWarpData().jaiWarp, img);
			for (Point p : targetPoints) {
				long startWarpPoint = System.currentTimeMillis();
				MyTile targetTile = new MyTile(trgImgMetadataWarp.getWritableRaster(p.x, p.y),
													trgImgMetadataWarp.getRectangle(p.x, p.y),
													trgImgMetadataWarp.getDataType());
				warp.computeTile(targetTile, warpedImage);
				trgtiles.add(new Tuple2<Tuple2<Point, String>, MyTile>(new Tuple2<Point, String>(p, stackImgMetadata.getBandPairName()), targetTile));
				}
			return trgtiles;
			}
		);
		
		JavaPairRDD<Tuple2<Point, String>, MyTile> changeDResults = masterRastersCal.join(warpResults)
																	.mapToPair((Tuple2<Tuple2<Point, String>, Tuple2<MyTile, MyTile>> pair) -> {
			ChangeDetectionMetadata metadata = changeDMetadataB.getValue();
			ChangeDetection changeDetection = new ChangeDetection(metadata.isOutputLogRatio(), metadata.getNoDataValueN(), metadata.getNoDataValueD());
			ImageMetadata trgImgMetadata = imgMetadataB.getValue().get("ratio_changeD");
			MyTile targetTile = new MyTile(trgImgMetadata.getWritableRaster(pair._1._1.x, pair._1._1.y),
												trgImgMetadata.getRectangle(pair._1._1.x, pair._1._1.y),
												trgImgMetadata.getDataType());
			try {
				changeDetection.computeTile(pair._2._1, pair._2._2, targetTile, targetTile.getRectangle());
			}
			catch (ArrayIndexOutOfBoundsException e) {
				System.out.println("key: " + pair._1 + "\n");
			}
			return new Tuple2<Tuple2<Point, String>, MyTile>(new Tuple2<Point, String>(pair._1._1, "ratio"), targetTile);
			}
		);
		List<Tuple2<Tuple2<Point, String>, MyTile>> changeResults = changeDResults.collect();
		
		long endParallelProcessing = System.currentTimeMillis();
		
		Write write = new Write(myChangeDetection.getTargetProduct(), targetFile, "BEAM-DIMAP");
		for (int i = 0; i < changeResults.size(); i++) {
			Band targetBand = writeOp.getTargetProduct().getBand(changeResults.get(i)._1._2);
			write.storeTile(targetBand, changeResults.get(i)._2);
		}
		
		// Final Messages!
		System.out.println("\n" + devsMSG + "Result Tiles: " + changeResults.size());
		
		long durationParallelProcessing = endParallelProcessing - endSerialProcessing;
		System.out.println("\n" + devsMSG + durationParallelProcessing + " ms [time2], for all parallelized procedures.");
		
		long durationImageProcessing = endParallelProcessing - startProcessing;
        System.out.println("\n" + devsMSG + durationImageProcessing + " ms [time3], for running all operators serial and parallel. No HDFS. No Write\n");



		
		//sc.close();
	}
	
}
