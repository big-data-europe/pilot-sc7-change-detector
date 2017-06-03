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
		
		TileBasedFinal parallelTiles = new TileBasedFinal();
		
		long startAll = System.currentTimeMillis();
		
		parallelTiles.processTiles(args[0], args[1], args[2], args[3], args[4], args[5], Integer.parseInt(args[6]));
		
		long endAll = System.currentTimeMillis();
		long totalAll = endAll - startAll;
        System.out.println("\n" + totalAll + " ms, for storing to HDFS and running all operators including Write.\n");
        
        //***Deleting unnecessary files***
//        File masterDim = new File(args[1]);
        File masterTiff = new File(args[2]);
//        File slaveDim = new File(args[3]);
        File slaveTiff = new File(args[4]);
//        if (masterDim.exists()) {
//        	masterDim.delete();
//        	System.out.println(masterDim.getName() + " deleted succesfully!");
//        }
//        else {
//        	System.out.println("Cannot delete: " + masterDim.getName());
//        }
        if (masterTiff.exists()) {
        	//masterTiff.delete();
        	System.out.println(masterTiff.getName() + " deleted succesfully!");
        }
        else {
        	System.out.println("Cannot delete: " + masterTiff.getName());
        }
//        if (slaveDim.exists()) {
//        	slaveDim.delete();
//        	System.out.println(slaveDim.getName() + " deleted succesfully!");
//        }
//        else {
//        	System.out.println("Cannot delete: " + slaveDim.getName());
//        }
        if (slaveTiff.exists()) {
        	//slaveTiff.delete();
        	System.out.println(slaveTiff.getName() + " deleted succesfully!");
        }
        else {
        	System.out.println("Cannot delete: " + slaveTiff.getName());
        }
        
	}

	public void processTiles(String hdfsPath, String masterDimFilePath, String masterTiffFilePath, String slaveDimFilePath, String slaveTiffFilePath, String targetPath, int partitionsNumber)
			throws Exception {
		//***Storing tiffs to HDFS***
		ZipHandler2 zipHandler = new ZipHandler2();
//		String masterTiffInHDFS = zipHandler.tiffLocalToHDFS(masterTiffFilePath, hdfsPath);
//		System.out.println(masterTiffInHDFS);
//		String slaveTiffInHDFS = zipHandler.tiffLocalToHDFS(slaveTiffFilePath, hdfsPath);
//		System.out.println(slaveTiffInHDFS);
		//  SP-WHOLE_SUBSET
		//String masterTiffInHDFS = "/media/indiana/data/imgs/subseting/s1a-s6-grd-vv-20160815t214331-20160815t214400-012616-013c9d-001.tiff";
		//String slaveTiffInHDFS = "/media/indiana/data/imgs/subseting/s1a-s6-grd-vv-20160908t214332-20160908t214401-012966-014840-001.tiff";
		//String masterTiffInHDFS = "/media/indiana/data/imgs/subseting/subsets-for-ttesting-cd/la/subset_of_S1A_IW_GRDH_1SSV_20160601T135202_20160601T135227_011518_011929_0EE2.tif";
		//String slaveTiffInHDFS = "/media/indiana/data/imgs/subseting/subsets-for-ttesting-cd/la/subset_of_S1A_IW_GRDH_1SSV_20160905T135207_20160905T135232_012918_0146C0_ECCC.tif";
		// LARISSA-SUBSETS
		//String masterTiffInHDFS = "/media/indiana/data/imgs/subseting/subset01_Larisa/subset_of_S1A_IW_GRDH_1SDV_20170330T162335_20170330T162400_015924_01A408_8894.tif";
		//String slaveTiffInHDFS = "/media/indiana/data/imgs/subseting/subset01_Larisa/subset_of_S1B_IW_GRDH_1SDV_20170228T162251_20170228T162316_004503_007D66_2D84.tif";
		// PEIRAEUS-SUBSETS
		//String masterTiffInHDFS = "/media/indiana/data/imgs/subseting/subset02_Peiraeus/subset_of_S1B_IW_GRDH_1SDV_20170301T042242_20170301T042307_004510_007DA0_F2AB.tif";
		//String slaveTiffInHDFS = "/media/indiana/data/imgs/subseting/subset02_Peiraeus/subset_of_S1B_IW_GRDH_1SDV_20161101T042246_20161101T042311_002760_004ABE_2A88.tif";
		// SAO-PAULO-SUBSETS
		//String masterTiffInHDFS = "/media/indiana/data/imgs/subseting/subset03_SaoPaulo/subset_of_S1A_S6_GRDH_1SDV_20160815T214331_20160815T214400_012616_013C9D_2495.tif";
		//String slaveTiffInHDFS = "/media/indiana/data/imgs/subseting/subset03_SaoPaulo/subset_of_S1A_S6_GRDH_1SDV_20160908T214332_20160908T214401_012966_014840_ABDC.tif";
		// ZAATARI-SUBSET
		//String masterTiffInHDFS = "/media/indiana/data/imgs/subseting/subset04_Zaatari/subset_of_S1A_IW_GRDH_1SDV_20161101T033537_20161101T033602_013743_0160ED_EBD6.tif";
		//String slaveTiffInHDFS = "/media/indiana/data/imgs/subseting/subset04_Zaatari/subset_of_S1A_IW_GRDH_1SDV_20160130T033528_20160130T033553_009718_00E305_A298.tif";
		// SAN-FRAN-SUBSET
		String masterTiffInHDFS = "/media/indiana/data/imgs/subseting/subset05_SanFran/subset_of_S1B_IW_GRDH_1SDV_20170219T015829_20170219T015854_004363_007958_2E9A.tif";
		String slaveTiffInHDFS = "/media/indiana/data/imgs/subseting/subset05_SanFran/subset_of_S1A_IW_GRDH_1SSV_20160922T015912_20160922T015937_013159_014E98_9960.tif";
		//  ROTTERDAM-SUBSET
		//String masterTiffInHDFS = "/media/indiana/data/imgs/subseting/subset04_Roterdam/subset_of_S1B_IW_GRDH_1SDV_20161217T172403_20161217T172428_003439_005DFD_56E8.tif";
		//String slaveTiffInHDFS = "/media/indiana/data/imgs/subseting/subset04_Roterdam/subset_of_S1A_IW_GRDH_1SDV_20160915T055007_20160915T055032_013059_014B3E_D9C8.tif";
		
		
		
		System.out.println("Serial Processing to acquire metadata...");
		long startProcessing = System.currentTimeMillis();
		
		//***Extracting metadata through serial processing***
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

		String[] parameters = { "NONE", "Master", "Orbit" };
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
		int bandsLeft = myCalibration1.getTargetProduct().getNumBands();
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
		System.out.println(durationSerialProcessing + " ms, for extracting metadata through Serial Processing");
		
		System.out.println("Parallel Processing using Spark...");		

		//***Parallel Processing using Spark***
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Parallelized at local mode"); //everywhere EXCEPT cluster
		//SparkConf conf = new SparkConf().set("spark.driver.maxResultSize", "5g").setAppName("Parallelized Operators in Spark");

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

		//long startWithGCPTime = System.currentTimeMillis(); //Efi's time-counter
		
		// master image calibration
		JavaPairRDD<Tuple2<Point, String>, MyTile> masterRastersCal = masterRastersRdd.mapPartitionsToPair((Iterator<Tuple2<String, Point>> iterator) -> {
			return CalibrationMappers.calibrationMaster(iterator, bandInfosB.getValue(), imgMetadataB.getValue(), calMetadataB.getValue());
			}
		).cache();
		
		JavaPairRDD<String, MyTile> slaveRastersCal = slaveRastersRdd.mapPartitionsToPair((Iterator<Tuple2<String, Point>> iterator) -> {
			return CalibrationMappers.calibrationSlave(iterator, bandInfosB.getValue(), imgMetadataB.getValue(), calMetadataB.getValue());
			}
		);
//		List<Tuple2<Tuple2<Point, String>, MyTile>> debugMasterRastersCal = masterRastersCal.collect();
		
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
//		System.out.println("NOW?");
		JavaPairRDD<Tuple2<Integer, String>, MyTile> masterRastersRdd2 = masterRastersCal.filter((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
			String name = imgMetadataB.getValue().get(pair._1._2 + "_stack").getBandPairName();
			Map<String, String> bandsList = bandsListGCPB.getValue();
			return bandsList.containsKey(name);
			}
		).flatMapToPair((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
			return CalibrationMappers.mapToRows(pair, imgMetadataB.getValue(), rows.getValue());
			}
		);
//		System.out.println("OR NOT?");
		
		// gcps computation. group by key the groups of rows and and compute the gcps contained to each group. Then, collect the gcps to the master node.
		JavaPairRDD<Tuple2<Integer, String>, Iterable<MyTile>> masterRows = masterRastersRdd2.groupByKey();
//		List<Tuple2<Tuple2<Integer, String>, Iterable<MyTile>>> debugMasterRows = masterRows.collect();
//		System.out.println("DEBUGMASTERROWS: " + debugMasterRows.toString());
		JavaPairRDD<Tuple2<Integer, String>, Iterable<MyTile>> stacktilesRows = createstackResultsRows.groupByKey();
//		List<Tuple2<Tuple2<Integer, String>, Iterable<MyTile>>> debugStacktilesRows = stacktilesRows.collect();
//		System.out.println("DEBUGSTACKTILEROWS: " + debugStacktilesRows.toString());
//		List<Tuple2<Tuple2<Integer, String>, Tuple2<Iterable<MyTile>, Iterable<MyTile>>>> debugJoin = masterRows.join(stacktilesRows).collect();
//		System.out.println("DEBUGJOIN:" + debugJoin.toString());
//		System.out.println("GCPMETADATABROAD: " + GCPMetadataBroad.getValue());
//		System.out.println("IMGMETADATAB: " + imgMetadataB.getValue());
//		System.out.println("MASTERGCPS: " + masterGcps.getValue());
//		System.out.println("ROWS: " + rows.getValue());
//		List<Tuple2<String, Tuple2<Integer, Placemark>>> debugSlaveGCPs = null;
//		System.out.println("SIZE OF DEBUGJOIN: " + debugJoin.size());
//		for(int j = 0; j < debugJoin.size(); j++) {
//			System.out.println("FOR i = " + j);
//			List<Tuple2<String, Tuple2<Integer, Placemark>>> listDebugSlaveGCPs = GCPMappers.GCPSelection(debugJoin.get(j), GCPMetadataBroad.getValue(), imgMetadataB.getValue(), masterGcps.getValue(), rows.getValue());
//			System.out.println("listDebugSlaveGCPs is: " + listDebugSlaveGCPs.toString());
//			// listDebugSlaveGCPs.addAll(debugSlaveGCPs); // den leitourgei. Eytyxws h for trexei mono gia mia epanalipsi
//		}
			//System.out.println("ALL debugSlaveGCPs ARE: " + debugSlaveGCPs.toString());
			// List<Tuple2<String, Tuple2<Integer, Placemark>>> debugSlaveGCPs = GCPMappers.GCPSelection(debugJoin, GCPMetadataBroad.getValue(), imgMetadataB.getValue(), masterGcps.getValue(), rows.getValue());
			// List<Tuple2<String, Tuple2<Integer, Placemark>>>
		List<Tuple2<String, Tuple2<Integer, Placemark>>> slaveGCPs = masterRows.join(stacktilesRows)
																.flatMap((Tuple2<Tuple2<Integer, String>, Tuple2<Iterable<MyTile>, Iterable<MyTile>>> pair) -> {
			return GCPMappers.GCPSelection(pair, GCPMetadataBroad.getValue(), imgMetadataB.getValue(), masterGcps.getValue(), rows.getValue());
			}
		).collect();
		if(slaveGCPs.isEmpty())
		{
			System.out.println("not enough GCPs detected");
			return;
		}

//		long endWithGCPTime = System.currentTimeMillis(); //Efi's time-counter
//		long totalwithGCPTime = endWithGCPTime - startProcessing; //Efi's time-counter
//		System.out.println(" GCP " + totalwithGCPTime); //Efi's time-counter
		
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
		System.out.println("GCPs size:\t" + slaveGCPs.size());
		for (String name : bandsListGCP.keySet()) {
			final ProductNodeGroup<Placemark> targetGCPGroup = GCPManager.instance().getGcpGroup(myGCPSelection.getTargetProduct().getBand(name));
			Map<Integer, Placemark> map = gcpsMap.get(name);
			for (Placemark p : map.values()) {
				targetGCPGroup.add(p);
			}
		}
		
		// compute the warp function
		//long startWarpTime = System.currentTimeMillis(); //Efi's time-counter
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
			final SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(bufferType,
														srcImgMetadataWarp.getTileSize().width,
														srcImgMetadataWarp.getTileSize().height);
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
				//System.out.println(System.currentTimeMillis() - startWarpPoint + " ends warp for point " + p); //Efi's time-counter
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
		System.out.println("result tiles " + changeResults.size());
		
		long endParallelProcessing = System.currentTimeMillis();
		long durationParallelProcessing = endParallelProcessing - endSerialProcessing;
		System.out.println(durationParallelProcessing + " ms, for all parallelized procedures.");
		long durationImageProcessing = endParallelProcessing - startProcessing;
        System.out.println(durationImageProcessing + " ms, for running all operators serial and parallel. No HDFS. No Write");

//    	long endTime = System.currentTimeMillis(); //Efi's time-counter
//    	long totalTime = endTime - startProcessing; //Efi's time-counter
//		System.out.println("total time " + totalTime); //Efi's time-counter
		
		Write write = new Write(myChangeDetection.getTargetProduct(), targetFile, "BEAM-DIMAP");
		for (int i = 0; i < changeResults.size(); i++) {
			Band targetBand = writeOp.getTargetProduct().getBand(changeResults.get(i)._1._2);
			write.storeTile(targetBand, changeResults.get(i)._2);
		}
		
		//sc.close();
	}
	
}