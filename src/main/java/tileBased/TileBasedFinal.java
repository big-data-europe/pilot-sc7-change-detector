package tileBased;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
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
import org.esa.snap.engine_utilities.gpf.StackUtils;

import com.google.common.collect.Lists;

import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import readerHDFS.BandInfo;
import readerHDFS.ReadHDFSTile;
import readerHDFS.ZipHandler2;
import scala.Tuple2;
import scala.Tuple3;
import serialProcessingNew.LoopLimits;
import serialProcessingNew.MyBandSelect;
import serialProcessingNew.MyChangeDetection;
import serialProcessingNew.MyCreateStack;
import serialProcessingNew.MyGCPSelection;
import serialProcessingNew.MyWarp;
import serialProcessingNew.MyWarp.WarpData;
import serialProcessingNew.MyWrite;
import serialProcessingNew.SerialProcessor;
import serialProcessingNew.calibration.MyCalibration;
import tileBased.mappers.CalibrationMappers;
import tileBased.mappers.CreateStackMappers;
import tileBased.mappers.GCPMappers;
import tileBased.metadata.CalibrationMetadata;
import tileBased.metadata.ChangeDetectionMetadata;
import tileBased.metadata.GCPMetadata;
import tileBased.metadata.ImageMetadata;
import tileBased.metadata.WarpMetadata;
import tileBased.model.MyTile;
import tileBased.operator.ChangeDetection;
import tileBased.operator.Warp;
import tileBased.operator.Write;
import tileBased.utils.TileBasedUtils;

public class TileBasedFinal {
	public static void main(String[] args) throws Exception {
		String filesPath = "/home/ethanos/Desktop/BDEimages/";
		String hdfsPath = "/home/ethanos/Desktop/BDEimages/VH";
		// String filesPath = "hdfs://localhost:9000/sentinel-images/";
		String masterFile = filesPath + "subset3_of_S1A_IW_GRDH_1SSV_20141225T142407_20141225T142436_003877_004A54_040F.dim";
		String slaveFile = filesPath + "subset3_of_S1A_IW_GRDH_1SSV_20150518T142409_20150518T142438_005977_007B49_AF76.dim";
		// File masterFile = new File(filesPath,
		// "S1A_IW_GRDH_1SSV_20141225T142407_20141225T142436_003877_004A54_040F.zip");
		// File slaveFile = new File(filesPath,
		// "S1A_IW_GRDH_1SSV_20150518T142409_20150518T142438_005977_007B49_AF76.zip");

		// File masterFile = new File(filesPath,
		// "subset_0_of_S1A_IW_GRDH_1SDV_20151110T145915_20151110T145940_008544_00C1A6_F175.dim");
		// File slaveFile = new File(filesPath,
		// "subset_1_of_S1A_IW_GRDH_1SDV_20151029T145915_20151029T145940_008369_00BD0B_334C.dim");
		// File masterFile = new File(filesPath,
		// "subset3_of_S1A_IW_GRDH_1SSV_20141225T142407_20141225T142436_003877_004A54_040F.dim");
		// File slaveFile = new File(filesPath,
		// "subset3_of_S1A_IW_GRDH_1SSV_20150518T142409_20150518T142438_005977_007B49_AF76.dim");

		TileBasedFinal parallelTiles = new TileBasedFinal();
		//parallelTiles.processTiles(args[0], args[1], args[2], args[3]);
		parallelTiles.processTiles(hdfsPath,masterFile,slaveFile,hdfsPath);
	}

	public void processTiles(String hdfsPath, String masterZipFilePath, String slaveZipFilePath, String targetPath)
			throws Exception {
		// String slaveZipFilePath = args[2];
		// String outFile = args[3];

		ZipHandler2 zipHandler = new ZipHandler2();
//		String masterTiffInHDFS = "/home/ethanos/SNAP/tiffs/ss1a-iw-grd-vv-20141225t142407-20141225t142436-003877-004a54-001.tiff";
//		String slaveTiffInHDFS = "/home/ethanos/SNAP/tiffs/ss1a-iw-grd-vv-20150518t142409-20150518t142438-005977-007b49-001.tiff";
		String masterTiffInHDFS = "/home/ethanos/UntitledFolder/s1a-iw-grd-vv-20141225t142407-20141225t142436-003877-004a54-001.tiff";
		String slaveTiffInHDFS = "/home/ethanos/UntitledFolder/s1a-iw-grd-vv-20150518t142409-20150518t142438-005977-007b49-001.tiff";

//		 String masterTiffInHDFS = "";
//		 String slaveTiffInHDFS = "";
//		 try {
//		 masterTiffInHDFS = zipHandler.tiffToHDFS(masterZipFilePath,hdfsPath);
//		 slaveTiffInHDFS = zipHandler.tiffToHDFS(slaveZipFilePath, hdfsPath);
//		 } catch (IOException e) {
//		 // TODO Auto-generated catch block
//		 e.printStackTrace();
//		 }
		System.out.println(masterTiffInHDFS);
		System.out.println(slaveTiffInHDFS);
		long startTime = System.currentTimeMillis();
		// List<Tuple2<String, MyTile>> slaveRasters = null;
		// List<Tuple2<String, MyTile>> masterRasters = null;
		List<Tuple2<String, Point>> slaveIndices = null;
		List<Tuple2<String, Point>> masterIndices = null;
		Map<String, BandInfo> bandInfos = new HashMap<>(2);
		Object2ObjectMap<String, Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>>> dependRects = new Object2ObjectOpenHashMap<String, Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>>>();
		System.out.println("POINT 1");

		// read slave and master files as usual
		SerialProcessor sp = new SerialProcessor();
		String[] selectedPolarisations = null;
		Product masterTargetProduct = null;
		Product slaveTargetProduct = null;
		try {
			System.out.println("POINT 1.1");
			masterTargetProduct = zipHandler.findTargetProduct(masterZipFilePath);
			// myRead1 = new MyRead(masterFile, "read1");
			// masterRasters = sp.readTiles(myRead1,selectedPolarisations);
			System.out.println("POINT 1.2");
			for (int i = 0; i < masterTargetProduct.getNumBands(); i++) {
				Band band = masterTargetProduct.getBandAt(i);
				if (band.getClass() == Band.class) {
					BandInfo bandInfo = new BandInfo(masterTiffInHDFS, i, 0);
					bandInfos.put(band.getName() + "_read1", bandInfo);
				}
			}
			System.out.println("POINT 1.3");
			masterIndices = sp.readTilesIndices(masterTargetProduct, selectedPolarisations);
		} catch (Exception e) {
			throw e;
		}
		System.out.println("POINT 2");

		try {
			slaveTargetProduct = zipHandler.findTargetProduct(slaveZipFilePath);
			// myRead2 = new MyRead(slaveFile, "read2");
			// slaveRasters = sp.readTiles(myRead2,selectedPolarisations);
			// Product targetProduct=myRead2.getTargetProduct();
			for (int i = 0; i < slaveTargetProduct.getNumBands(); i++) {
				Band band = slaveTargetProduct.getBandAt(i);
				if (band.getClass() == Band.class) {
					BandInfo bandInfo = new BandInfo(slaveTiffInHDFS, i, 0);
					bandInfos.put(band.getName() + "_read2", bandInfo);
				}
			}
			slaveIndices = sp.readTilesIndices(slaveTargetProduct, selectedPolarisations);
		} catch (Exception e) {
			throw e;
		}
		// initialize all operators as usual to acquire the necessary metadata
		MyBandSelect bandselect1 = new MyBandSelect(selectedPolarisations, null);
		bandselect1.setSourceProduct(masterTargetProduct);
		sp.initOperator(bandselect1);
		MyBandSelect bandselect2 = new MyBandSelect(selectedPolarisations, null);
		bandselect2.setSourceProduct(slaveTargetProduct);
		sp.initOperator(bandselect2);
		System.out.println("POINT 3");

		Boolean[] bParams1 = { false, false, false, false, true, false, false, false };
		MyCalibration myCalibration1 = new MyCalibration(null, bParams1, null);
		myCalibration1.setSourceProduct(bandselect1.getTargetProduct());
		myCalibration1.setId("cal1");
		sp.initOperator(myCalibration1);

		MyCalibration myCalibration2 = new MyCalibration(null, bParams1, null);
		myCalibration2.setSourceProduct(bandselect2.getTargetProduct());
		myCalibration2.setId("cal2");
		sp.initOperator(myCalibration2);

		Product[] sourcesForCreateStack = new Product[2];
		sourcesForCreateStack[0] = myCalibration1.getTargetProduct();
		sourcesForCreateStack[1] = myCalibration2.getTargetProduct();

		String[] parameters = { "NONE", "Master", "Orbit" };
		MyCreateStack myCreateStack = new MyCreateStack(parameters);
		myCreateStack.setSourceProduct(sourcesForCreateStack);
		myCreateStack.setId("stack");
		sp.initOperator(myCreateStack);
		System.out.println("POINT 4");

		boolean[] bParams = { false, false, false, false };
		int[] iParams = { 2000, 10, 3 };
		double[] dParams = { 0.25, 0.6 };
		String[] sParams = { "128", "128", "4", "4", "32", "32" };
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
		System.out.println("POINT 5");

		File targetFile = new File(targetPath, "changeD-tile-based-tiledImage");
		MyWrite writeOp = new MyWrite(myChangeDetection.getTargetProduct(), targetFile, "BEAM-DIMAP");
		writeOp.setId("write");
		sp.initOperator(writeOp);
		// initialize the imageMetadata for all bands and operators.
		// The imageMetadata class has replaced the band class and contains only
		// the absolutely essential metadata for tile computations
		Object2ObjectMap<String, ImageMetadata> imageMetadata = new Object2ObjectOpenHashMap<String, ImageMetadata>(
				myCalibration1.getTargetProduct().getNumBands() * 3);
		// Map<String, String> sourceTargetMap = new HashMap<String, String>(
		// myCalibration1.getTargetProduct().getNumBands()* 3);
		OpMetadataCreator opMetadataCreator = new OpMetadataCreator();
		Object2ObjectMap<String, CalibrationMetadata> calMetadata = new Object2ObjectOpenHashMap<String, CalibrationMetadata>(
				myCalibration1.getTargetProduct().getNumBands() * 4);
		// read metadata.
		opMetadataCreator.createProdImgMetadata(imageMetadata, masterTargetProduct, "read1");
		opMetadataCreator.createProdImgMetadata(imageMetadata, slaveTargetProduct, "read2");
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
		System.out.println("POINT 6");

		// Change detection metadata
		opMetadataCreator.createOpImgMetadata(imageMetadata, myChangeDetection);
		ChangeDetectionMetadata changeDMetadata = opMetadataCreator.createChangeDMetadata(myChangeDetection, bParams3);

		// init sparkConf
		// SparkConf conf = new
		// SparkConf().setMaster("local[4]").set("spark.driver.maxResultSize",
		// "3g")
		// .setAppName("First test on tile parallel processing");
		SparkConf conf = new SparkConf().setMaster("local[4]").set("spark.driver.maxResultSize", "5g")
				.setAppName("First test on tile parallel processing");
				// SparkConf conf = new SparkConf()
				// .setAppName("First test on tile parallel processing");

		// configure spark to use Kryo serializer instead of the java
		// serializer.
		// All classes that should be serialized by kryo, are registered in
		// MyRegitration class .
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryo.registrator", "tileBased.MyRegistrator").set("spark.kryoserializer.buffer.max", "2047m");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// broadcast image metadata
		Broadcast<Map<String, ImageMetadata>> imgMetadataB = sc.broadcast(imageMetadata);
		Broadcast<Map<String, CalibrationMetadata>> calMetadataB = sc.broadcast(calMetadata);

		Broadcast<Object2ObjectMap<String, Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>>>> dependRectsB = sc
				.broadcast(dependRects);
		Broadcast<ProductNodeGroup<Placemark>> masterGcps = sc.broadcast(myGCPSelection.getMasterGcpGroup());
		Broadcast<GCPMetadata> GCPMetadataBroad = sc.broadcast(GCPMetadata);
		Broadcast<Map<String, String>> bandsListGCPB = sc.broadcast(bandsListGCP);

		Broadcast<WarpMetadata> warpMetadataB = sc.broadcast(warpMetadata);
		Broadcast<ChangeDetectionMetadata> changeDMetadataB = sc.broadcast(changeDMetadata);

		Broadcast<Integer> rows = sc.broadcast(limits.getNumXTiles());
		Broadcast<Map<String, BandInfo>> bandInfosB = sc.broadcast(bandInfos);

		LoopLimits limits2 = new LoopLimits(myWarp.getTargetProduct());
		// Broadcast<LoopLimits> limitsBroad = sc.broadcast(limits2);
		// JavaPairRDD<String, MyTile> masterRastersRdd =
		// sc.parallelizePairs(masterRasters);
		// JavaPairRDD<String, MyTile> slaveRastersRdd =
		// sc.parallelizePairs(slaveRasters);
		JavaPairRDD<String, Point> masterRastersRdd = sc.parallelizePairs(masterIndices)
				.partitionBy(new HashPartitioner(12));
		JavaPairRDD<String, Point> slaveRastersRdd = sc.parallelizePairs(slaveIndices)
				.partitionBy(new HashPartitioner(12));
		long startWithGCPTime = System.currentTimeMillis();
		// master image calibration
		JavaPairRDD<Tuple2<Point, String>, MyTile> masterRastersCal = masterRastersRdd
				.mapPartitionsToPair((Iterator<Tuple2<String, Point>> iterator) -> {
					return CalibrationMappers.calibrationMasterFromHdfs(iterator, bandInfosB.getValue(),
							imgMetadataB.getValue(), calMetadataB.getValue());
				}).cache();
		JavaPairRDD<String, MyTile> slaveRastersCal = slaveRastersRdd
				.mapPartitionsToPair((Iterator<Tuple2<String, Point>> iterator) -> {
					return CalibrationMappers.calibrationSlaveFromHdfs(iterator, bandInfosB.getValue(), imgMetadataB.getValue(),
							calMetadataB.getValue());
				});
		JavaPairRDD<Tuple3<Point, String, Rectangle>, MyTile> dependentPairs = slaveRastersCal
				.flatMapToPair((Tuple2<String, MyTile> pair) -> {
					ImageMetadata srcImgMetadataStack = imgMetadataB.getValue().get(pair._1 + "_" + "stack");
					List<Tuple2<Tuple3<Point, String, Rectangle>, MyTile>> pairs = new ArrayList<Tuple2<Tuple3<Point, String, Rectangle>, MyTile>>();
					Object2ObjectMap<String, Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>>> dependRectsMap = dependRectsB
							.getValue();
					Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>> dependRectangles = dependRectsMap
							.get(pair._1);

					Point sourcePoint = srcImgMetadataStack.getTileIndices(pair._2.getMinX(), pair._2.getMinY());
					List<Tuple2<Point, Rectangle>> tuples = dependRectangles.get(sourcePoint);
					for (Tuple2<Point, Rectangle> tuple : tuples)
						pairs.add(new Tuple2<Tuple3<Point, String, Rectangle>, MyTile>(
								new Tuple3<Point, String, Rectangle>(tuple._1(), pair._1(), tuple._2()), pair._2));
					return pairs;
				});

		JavaPairRDD<Tuple2<Point, String>, MyTile> createstackResults = dependentPairs.groupByKey()
				.mapToPair((Tuple2<Tuple3<Point, String, Rectangle>, Iterable<MyTile>> pair) -> {
					return CreateStackMappers.createStack(pair, imgMetadataB.getValue());
				}).cache();
		//
		// //split the createstack tiles in groups of rows with a unique key to
		// each group
		JavaPairRDD<Tuple2<Integer, String>, MyTile> createstackResultsRows = createstackResults
				.filter((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
					Map<String, String> bandsList = bandsListGCPB.getValue();
					return bandsList.containsKey(pair._1._2);
				}).flatMapToPair((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
					return CreateStackMappers.mapToRows(pair, imgMetadataB.getValue(), rows.getValue());
				});
		// //split the master tiles in groups of rows with a unique key to each
		// group
		JavaPairRDD<Tuple2<Integer, String>, MyTile> masterRastersRdd2 = masterRastersCal
				.filter((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
					String name = imgMetadataB.getValue().get(pair._1._2 + "_stack").getBandPairName();
					Map<String, String> bandsList = bandsListGCPB.getValue();
					return bandsList.containsKey(name);
				}).flatMapToPair((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {

					return CalibrationMappers.mapToRows(pair, imgMetadataB.getValue(), rows.getValue());

				});
		// //gcps computation. group by key the groups of rows and and compute
		// the gcps contained to each group
		// //Then, collect the gcps to the master node.
		JavaPairRDD<Tuple2<Integer, String>, Iterable<MyTile>> masterRows = masterRastersRdd2.groupByKey();
		JavaPairRDD<Tuple2<Integer, String>, Iterable<MyTile>> stacktilesRows = createstackResultsRows.groupByKey();
		List<Tuple2<String, Tuple2<Integer, Placemark>>> slaveGCPs = masterRows.join(stacktilesRows)
				.flatMap((Tuple2<Tuple2<Integer, String>, Tuple2<Iterable<MyTile>, Iterable<MyTile>>> pair) -> {
					return GCPMappers.GCPSelection(pair, GCPMetadataBroad.getValue(), imgMetadataB.getValue(),
							masterGcps.getValue(), rows.getValue());
				}).collect();

		long endWithGCPTime = System.currentTimeMillis();
		long totalwithGCPTime = endWithGCPTime - startTime;
		System.out.println(" GCP " + totalwithGCPTime);
		// process
		// put the gcps into a hashmap to eliminate some duplicates
		Map<String, Map<Integer, Placemark>> gcpsMap = new HashMap<String, Map<Integer, Placemark>>();
		for (int i = 0; i < slaveGCPs.size(); i++) {
			String bandName = slaveGCPs.get(i)._1;
			if (gcpsMap.containsKey(bandName)) {
				gcpsMap.get(bandName).put(slaveGCPs.get(i)._2._1, slaveGCPs.get(i)._2._2);
			} else {
				Map<Integer, Placemark> placemarksMap = new HashMap<Integer, Placemark>();
				placemarksMap.put(slaveGCPs.get(i)._2._1, slaveGCPs.get(i)._2._2);
				gcpsMap.put(bandName, placemarksMap);
			}
		}
		System.out.println("GCPs size" + slaveGCPs.size());
		// Checks.checkGCPs(gcpsMap, myGCPSelection.getMasterGcpGroup());
		// //add gcps to the GCPManager
		for (String name : bandsListGCP.keySet()) {
			final ProductNodeGroup<Placemark> targetGCPGroup = GCPManager.instance()
					.getGcpGroup(myGCPSelection.getTargetProduct().getBand(name));
			Map<Integer, Placemark> map = gcpsMap.get(name);
			for (Placemark p : map.values()) {
				targetGCPGroup.add(p);
			}
		}
		// compute the warp function
		long startWarpTime = System.currentTimeMillis();
		System.out.println("start computing warp function");
		myWarp.getWarpData();
		Map<String, WarpData> warpdataMap = new HashMap<String, WarpData>();
		Product targetProductWarp = myWarp.getTargetProduct();
		String[] masterBandNamesWarp = StackUtils.getMasterBandNames(targetProductWarp);
		Set<String> masterBandsWarp = new HashSet(Arrays.asList(masterBandNamesWarp));
		System.out.println("start computing warp dependent rectangles");
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
		Broadcast<Object2ObjectMap<String, Object2ObjectMap<Point, ObjectSet<Rectangle>>>> dependRectsWarpB2 = sc
				.broadcast(dependRectsWarp2);
		Broadcast<Object2ObjectMap<String, Object2ObjectMap<Rectangle, ObjectList<Point>>>> dependPointsWarpB = sc
				.broadcast(dependPointsWarp);
		System.out.println("start warp");

		JavaPairRDD<Tuple2<String, Rectangle>, MyTile> dependentPairsWarp = createstackResults
				.flatMapToPair((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
					List<Tuple2<Tuple2<String, Rectangle>, MyTile>> pairs = new ArrayList<Tuple2<Tuple2<String, Rectangle>, MyTile>>();
					Object2ObjectMap<String, Object2ObjectMap<Point, ObjectSet<Rectangle>>> dependRectsMap = dependRectsWarpB2
							.getValue();
					Object2ObjectMap<Point, ObjectSet<Rectangle>> bandRects = dependRectsMap.get(pair._1._2);
					ImageMetadata srcImgMetadataWarp = imgMetadataB.getValue().get(pair._1._2 + "_warp" + "_source");
					Point sourcePoint = srcImgMetadataWarp.getTileIndices(pair._2.getMinX(), pair._2.getMinY());
					Set<Rectangle> tuples = bandRects.get(sourcePoint);
					for (Rectangle rect : tuples) {
						pairs.add(new Tuple2<Tuple2<String, Rectangle>, MyTile>(
								new Tuple2<String, Rectangle>(pair._1._2, rect), pair._2));
						// System.out.println(pair._1._2+" "+rect);
					}
					// System.out.println("pairs ok");
					// System.out.println(System.currentTimeMillis());
					return pairs;
				});
		createstackResults.unpersist();
		// List<Tuple2<Tuple2<String, Rectangle>, MyTile>>
		// list=dependentPairsWarp.collect();
		// Map<Tuple2<String, Rectangle>,List<MyTile>> mapWarp=new
		// HashMap<Tuple2<String, Rectangle>,List<MyTile>>();
		// for (int i = 0; i < list.size(); i++) {
		// Tuple2<String, Rectangle> bandName = list.get(i)._1;
		// if (mapWarp.containsKey(bandName)) {
		// mapWarp.get(bandName).add(list.get(i)._2);
		// } else {
		// List<MyTile> tiles = new ArrayList<MyTile>();
		// tiles.add(list.get(i)._2);
		// mapWarp.put(bandName, tiles);
		// }
		// }
		// System.out.println("warp rectangles "+mapWarp.size());
		// for(Map.Entry<Tuple2<String, Rectangle>, List<MyTile>>
		// entry:mapWarp.entrySet())
		// {
		// System.out.println("entry key is "+entry.getKey()+" and entry value
		// size is "+entry.getValue().size());
		//
		// }
		JavaPairRDD<Tuple2<String, Rectangle>, Iterable<MyTile>> warpResults1 = dependentPairsWarp.groupByKey();
		// List<Tuple2<Tuple2<String, Rectangle>, Iterable<MyTile>>>
		// list=warpResults1.collect();
		// int totalPoints=0;
		// for(int i=0;i<list.size();i++){
		// System.out.println(list.get(i)._1);
		// List<MyTile> tiles=Lists.newArrayList(list.get(i)._2);
		// totalPoints+=tiles.size();
		// System.out.println(tiles.size());
		// Object2ObjectMap<Rectangle, ObjectList<Point>>
		// pointsRect=dependPointsWarp.get(list.get(i)._1._1);
		// System.out.println(pointsRect.get(list.get(i)._1._2));
		// }
		// System.out.println(totalPoints);
		JavaPairRDD<Tuple2<Point, String>, MyTile> warpResults = warpResults1
				.flatMapToPair((Tuple2<Tuple2<String, Rectangle>, Iterable<MyTile>> pair) -> {
					List<Tuple2<Tuple2<Point, String>, MyTile>> trgtiles = new ArrayList<Tuple2<Tuple2<Point, String>, MyTile>>();
					Object2ObjectMap<String, Object2ObjectMap<Rectangle, ObjectList<Point>>> dependRectsMap = dependPointsWarpB
							.getValue();
					Object2ObjectMap<Rectangle, ObjectList<Point>> pointsRect = dependRectsMap.get(pair._1._1);
					ImageMetadata srcImgMetadataWarp = imgMetadataB.getValue().get(pair._1._1() + "_warp" + "_source");

					int bufferType = ImageManager.getDataBufferType(srcImgMetadataWarp.getDataType());
//					final SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(bufferType,
//							srcImgMetadataWarp.getImageWidth(), srcImgMetadataWarp.getImageHeight());
					final SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(bufferType,
							srcImgMetadataWarp.getTileSize().width, srcImgMetadataWarp.getTileSize().height);
					final ColorModel cm = PlanarImage.createColorModel(sampleModel);
					//WritableRaster raster = RasterFactory.createWritableRaster(sampleModel, new Point(0, 0));
					//BufferedImage img1 = new BufferedImage(cm, raster, false, new java.util.Hashtable());

//					TiledImage img = new TiledImage(img1, srcImgMetadataWarp.getTileSize().width,
//							srcImgMetadataWarp.getTileSize().height);

					TiledImage img = new TiledImage(0,0, srcImgMetadataWarp.getImageWidth(),srcImgMetadataWarp.getImageHeight(),0,0,
							sampleModel,cm);

					// WritableRaster raster =
					// RasterFactory.createWritableRaster(sampleModel, new
					// Point(0, 0));
					// BufferedImage img = new BufferedImage(cm, raster, false,
					// new java.util.Hashtable());
					List<MyTile> tiles = Lists.newArrayList(pair._2.iterator());
					List<Point> targetPoints = pointsRect.get(pair._1._2);
					for (MyTile myTile : tiles) {
						// img.getRaster().setDataElements(myTile.getMinX(),
						// myTile.getMinY(), myTile.getWidth(),
						// myTile.getHeight(),
						// myTile.getRawSamples().getElems());
						img.setData(myTile.getWritableRaster());

					}
					tiles = null;
					Map<String, WarpData> map = warpDataMapB.getValue();
					WarpData w = map.get(pair._1._1());
					Warp warp = new Warp(warpMetadataB.getValue().getInterp(), w,
							warpMetadataB.getValue().getInterpTable());
					// get warped image
					ImageMetadata trgImgMetadataWarp = imgMetadataB.getValue().get(pair._1._1() + "_warp" + "_target");
					Map<String, ImageMetadata> map2 = imgMetadataB.getValue();
					ImageMetadata stackImgMetadata = map2.get(pair._1._1() + "_" + "stack");
					RenderedOp warpedImage = warp.createWarpImage(warp.getWarpData().jaiWarp, img);
					for (Point p : targetPoints) {
						long startWarpPoint = System.currentTimeMillis();
						MyTile targetTile = new MyTile(trgImgMetadataWarp.getWritableRaster(p.x, p.y),
								trgImgMetadataWarp.getRectangle(p.x, p.y), trgImgMetadataWarp.getDataType());

						warp.computeTile(targetTile, warpedImage);

						trgtiles.add(new Tuple2<Tuple2<Point, String>, MyTile>(
								new Tuple2<Point, String>(p, stackImgMetadata.getBandPairName()), targetTile));
						System.out.println(System.currentTimeMillis() - startWarpPoint + " ends warp for point " + p);
					}
					// System.out.println("warp results ok");
					return trgtiles;
				});
		JavaPairRDD<Tuple2<Point, String>, MyTile> changeDResults = masterRastersCal.join(warpResults)
				.mapToPair((Tuple2<Tuple2<Point, String>, Tuple2<MyTile, MyTile>> pair) -> {
					ChangeDetectionMetadata metadata = changeDMetadataB.getValue();
					ChangeDetection changeDetection = new ChangeDetection(metadata.isOutputLogRatio(),
							metadata.getNoDataValueN(), metadata.getNoDataValueD());
					ImageMetadata trgImgMetadata = imgMetadataB.getValue().get("ratio_changeD");
					MyTile targetTile = new MyTile(trgImgMetadata.getWritableRaster(pair._1._1.x, pair._1._1.y),
							trgImgMetadata.getRectangle(pair._1._1.x, pair._1._1.y), trgImgMetadata.getDataType());
					// MyTile
					// denominatorTile=Utils.getSubTile(pair._2._2,targetTile.getRectangle());
					// MyTile
					// nominatorTile=Utils.getSubTile(pair._2._1,targetTile.getRectangle());
					try {
						changeDetection.computeTile(pair._2._1, pair._2._2, targetTile, targetTile.getRectangle());
					} catch (ArrayIndexOutOfBoundsException e) {

						System.out.println("key: " + pair._1);
						System.out.println("");
					}

					return new Tuple2<Tuple2<Point, String>, MyTile>(new Tuple2<Point, String>(pair._1._1, "ratio"),
							targetTile);
				});
		// List<Tuple2<Tuple2<Point, String>, MyTile>> warpRes =
		// warpResults.collect();
		List<Tuple2<Tuple2<Point, String>, MyTile>> changeResults = changeDResults.collect();
		System.out.println("result tiles " + changeResults.size());
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		// System.out.println("warp + change detection time
		// "+(startWarpTime-totalTime));
		System.out.println("total time " + totalTime);
		Write write = new Write(myChangeDetection.getTargetProduct(), targetFile, "BEAM-DIMAP");
		for (int i = 0; i < changeResults.size(); i++) {
			Band targetBand = writeOp.getTargetProduct().getBand(changeResults.get(i)._1._2);
			write.storeTile(targetBand, changeResults.get(i)._2);
		}
		// //Checks.checkWarp(warpRes);

	}
}
