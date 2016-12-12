package tileBased;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.RenderingHints;
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

import javax.media.jai.ImageLayout;
import javax.media.jai.JAI;
import javax.media.jai.PlanarImage;
import javax.media.jai.RenderedOp;
import javax.media.jai.TiledImage;
import javax.media.jai.WarpOpImage;
import javax.media.jai.operator.FormatDescriptor;

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
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import scala.Tuple2;
import scala.Tuple3;
import serialProcessingNew.LoopLimits;
import serialProcessingNew.MyBandSelect;
import serialProcessingNew.MyChangeDetection;
import serialProcessingNew.MyCreateStack;
import serialProcessingNew.MyGCPSelection;
import serialProcessingNew.MyRead;
import serialProcessingNew.MyWarp;
import serialProcessingNew.MyWarp.WarpData;
import serialProcessingNew.MyWrite;
import serialProcessingNew.SerialProcessor;
import serialProcessingNew.Utils;
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
import tileBased.operator.CreateStack;
import tileBased.operator.Sentinel1Calibrator;
import tileBased.operator.Warp;
import tileBased.operator.Write;

/*
 * This class is for local testing.
 * It reads images from the local file system, NOT the hdfs. 
 * The images are loaded as Tiles into memory and are processed by spark spark.
 */

public class TileBasedNew {
	public static void main(String[] args) throws Exception {
		String filesPath = "/home/efi/SNAP/sentinel-images/";
	   File slaveFile = new File(filesPath,
				 "subset_1_of_S1A_IW_GRDH_1SSV_20150518T142409_20150518T142438_005977_007B49_AF76.dim");
	   File masterFile = new File(filesPath,
						 "subset_11_of_S1A_IW_GRDH_1SSV_20141225T142407_20141225T142436_003877_004A54_040F.dim");
		TileBasedNew parallelTiles = new TileBasedNew();
		parallelTiles.processTiles(masterFile, slaveFile);
	}

	public void processTiles(File masterFile, File slaveFile) throws Exception {
		long startTime = System.currentTimeMillis();
		List<Tuple2<String, MyTile>> slaveRasters = null;
		List<Tuple2<String, MyTile>> masterRasters = null;
		Object2ObjectMap<String, Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>>> dependRects = new Object2ObjectOpenHashMap<String, Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>>>();

		
		SerialProcessor sp = new SerialProcessor();
		
		//if you set polarizations !=null, from the multi band images, only the bands corresponding to the selecte polarizations will be processed.
		//String[] selectedPolarisations={"VH"};
		//String[] selectedPolarisations={"VH","VV"};
		String[] selectedPolarisations=null;
		
		
		//read master and slave images from the local file system.
		//using the Read operators. The read operators are the same with the serialProcessor.
		MyRead myRead1 = null;
		MyRead myRead2 = null;
		try {
			myRead1 = new MyRead(masterFile, "read1");
			masterRasters = sp.readTiles(myRead1,selectedPolarisations);
		} catch (Exception e) {
			throw e;
		}

		try {
			myRead2 = new MyRead(slaveFile, "read2");
			slaveRasters = sp.readTiles(myRead2,selectedPolarisations);
		} catch (Exception e) {
			throw e;
		}
		
		//Initialize all operators exactly as in serial Processor.
		//This step just initializes the metadata for operators and images, no processing occurs here.
		//It is necessary for extracting the metadata which are included in the Band, Product etc classes.
		MyBandSelect bandselect1=new MyBandSelect(selectedPolarisations,null);
		bandselect1.setSourceProduct(myRead1.getTargetProduct());
		sp.initOperator(bandselect1);
		MyBandSelect bandselect2=new MyBandSelect(selectedPolarisations,null);
		bandselect2.setSourceProduct(myRead2.getTargetProduct());
		sp.initOperator(bandselect2);
		
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

		boolean[] bParams = { false, false, false, false };
		int[] iParams = { 1000, 10, 3 };
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

		String filesPath = "/home/efi/SNAP/sentinel-images/";
		File targetFile = new File(filesPath, "WARP-TEST-SUBS3");
		MyWrite writeOp = new MyWrite(myWarp.getTargetProduct(), targetFile, "BEAM-DIMAP");
		writeOp.setId("write");
		sp.initOperator(writeOp);
		
		//Here initialize the imageMetadata for all bands and operators. The actual Band and Product class cannot easily be passed to Spark workers
		//because it is not serializable. So I extracted ONLY the absolutely essential fields from Band, Product etc and added them to the
		//ImageMetadata class.
		
		//this map contains, for each combination of band-operator, the metadata.
		Object2ObjectMap<String, ImageMetadata> imageMetadata = new Object2ObjectOpenHashMap<String, ImageMetadata>(
				myCalibration1.getTargetProduct().getNumBands() * 3);
		OpMetadataCreator opMetadataCreator = new OpMetadataCreator();
		Object2ObjectMap<String, CalibrationMetadata> calMetadata = new Object2ObjectOpenHashMap<String, CalibrationMetadata>(
				myCalibration1.getTargetProduct().getNumBands() * 4);
		
		// calibration1 metadata.
		opMetadataCreator.createCalImgMetadata(imageMetadata, myCalibration1,
				calMetadata, bParams1);
		
		// calibration2 metadata.
		opMetadataCreator.createCalImgMetadata(imageMetadata, myCalibration2,
				calMetadata, bParams1);
		
		// createstack metadata.
		opMetadataCreator.createOpImgMetadata(imageMetadata, myCreateStack,
				true);
		getDependRects(imageMetadata,myCreateStack,dependRects);
		
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

		
		// init sparkConf
		SparkConf conf = new SparkConf().setMaster("local[4]").set("spark.driver.maxResultSize", "3g")
				.setAppName("First test on tile parallel processing");
		
		// configure spark to use Kryo serializer instead of the java
		// serializer.
		// All classes that should be serialized by kryo, are registered in
		// tileBased.MyRegitration class .
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryo.registrator", "tileBased.MyRegistrator").set("spark.kryoserializer.buffer.max", "550");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// broadcast all imagemetadata. The broadcasting mechanism makes accessible variables to all Spark workers.
		Broadcast<Map<String, ImageMetadata>> imgMetadataB = sc.broadcast(imageMetadata);
		Broadcast<Map<String, CalibrationMetadata>> calMetadataB = sc.broadcast(calMetadata);
		
		Broadcast<Object2ObjectMap<String, Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>>>> dependRectsB = sc.broadcast(dependRects);
		Broadcast<ProductNodeGroup<Placemark>> masterGcps = sc.broadcast(myGCPSelection.getMasterGcpGroup());
		Broadcast<GCPMetadata> GCPMetadataBroad = sc.broadcast(GCPMetadata);
		Broadcast<Map<String, String>> bandsListGCPB = sc.broadcast(bandsListGCP);

		Broadcast<WarpMetadata> warpMetadataB = sc.broadcast(warpMetadata);
		Broadcast<ChangeDetectionMetadata> changeDMetadataB = sc.broadcast(changeDMetadata);
		
		Broadcast<Integer> rows = sc.broadcast(limits.getNumXTiles());

		LoopLimits limits2 = new LoopLimits(myWarp.getTargetProduct());
		Broadcast<LoopLimits> limitsBroad = sc.broadcast(limits2);
		JavaPairRDD<String, MyTile> masterRastersRdd = sc.parallelizePairs(masterRasters).partitionBy(new HashPartitioner(10));
		JavaPairRDD<String, MyTile> slaveRastersRdd = sc.parallelizePairs(slaveRasters).partitionBy(new HashPartitioner(10));
		
		//calibration of the master image 
		JavaPairRDD<Tuple2<Point, String>, MyTile> masterRastersCal = masterRastersRdd
				.mapToPair((Tuple2<String, MyTile> tuple) -> {
					return CalibrationMappers.calibrationMaster( tuple,imgMetadataB.getValue(),
							calMetadataB.getValue());
				});
		//.repartition(10);
	
		//calibration of the slave image 
		JavaPairRDD<String, MyTile> slaveRastersCal = slaveRastersRdd.mapToPair((Tuple2<String, MyTile> tuple) -> {
			return CalibrationMappers.calibrationSlave( tuple,imgMetadataB.getValue(),
					calMetadataB.getValue());
		});
		
		//For createstack, to compute each tile of the result image you need the pixels from the source, slave image.
		//Here, for each result tile, the corresponding rectangle in the source image is computed.
		JavaPairRDD<Tuple3<Point, String, Rectangle>, MyTile> dependentPairs = slaveRastersCal
				.flatMapToPair((Tuple2<String, MyTile> pair) -> {
					ImageMetadata srcImgMetadataStack = imgMetadataB.getValue().get(pair._1+"_"+"stack");
					List<Tuple2<Tuple3<Point, String, Rectangle>, MyTile>> pairs = new ArrayList<Tuple2<Tuple3<Point, String, Rectangle>, MyTile>>();
					Object2ObjectMap<String, Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>>> dependRectsMap = dependRectsB.getValue();
					Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>> dependRectangles = dependRectsMap.get(pair._1);

					Point sourcePoint = srcImgMetadataStack.getTileIndices(pair._2.getMinX(), pair._2.getMinY());
					List<Tuple2<Point, Rectangle>> tuples = dependRectangles.get(sourcePoint);
					if(tuples!=null){
					for (Tuple2<Point, Rectangle> tuple : tuples)
						pairs.add(new Tuple2<Tuple3<Point, String, Rectangle>, MyTile>(
								new Tuple3<Point, String, Rectangle>(tuple._1(), pair._1, tuple._2()), pair._2));
					}
					return pairs;
				});
		
		//run create stack
		JavaPairRDD<Tuple2<Point, String>, MyTile> createstackResults = dependentPairs.groupByKey()
				.mapToPair((Tuple2<Tuple3<Point, String, Rectangle>, Iterable<MyTile>> pair) -> {
					return CreateStackMappers.createStack(pair, imgMetadataB.getValue());
				}).cache();
	//	Checks.checkCreatestack(createstackResults.collect());

		// //split the createstack tiles in groups of rows with a unique key to
				// each group
				JavaPairRDD<Tuple2<Integer, String>, MyTile> createstackResultsRows = createstackResults
						.filter((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
							Map<String, String> bandsList = bandsListGCPB.getValue();
							return bandsList.containsKey(pair._1._2);
						}).flatMapToPair((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
							return CreateStackMappers.mapToRows(pair, imgMetadataB.getValue(), rows.getValue());
						});
		//split the master tiles in groups of rows with a unique key to each group
		JavaPairRDD<Tuple2<Integer, String>, MyTile> masterRastersRdd2 = masterRastersCal
				.filter((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
					String name = imgMetadataB.getValue().get(pair._1._2 + "_stack").getBandPairName();
					Map<String, String> bandsList = bandsListGCPB.getValue();
					return bandsList.containsKey(name);
				}).flatMapToPair((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
					return CalibrationMappers.mapToRows(pair, imgMetadataB.getValue(), rows.getValue());
				});
		//gcps computation. group by key the groups of rows and compute the gcps contained to each group
		//Then, collect the gcps to the master node.
		JavaPairRDD<Tuple2<Integer, String>, Iterable<MyTile>> masterRows = masterRastersRdd2.groupByKey();
		JavaPairRDD<Tuple2<Integer, String>, Iterable<MyTile>> stacktilesRows = createstackResultsRows.groupByKey();
		List<Tuple2<String, Tuple2<Integer, Placemark>>> slaveGCPs = masterRows.join(stacktilesRows)
				.flatMap((Tuple2<Tuple2<Integer, String>, Tuple2<Iterable<MyTile>, Iterable<MyTile>>> pair) -> {
					return GCPMappers.GCPSelection(pair, GCPMetadataBroad.getValue(), imgMetadataB.getValue(),
							masterGcps.getValue(), rows.getValue());
				}).collect();
		if(slaveGCPs.isEmpty())
			return;
		//put the gcps into a hashmap to eliminate some duplicates
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
		
		
	//	Checks.checkGCPs2(gcpsMap, myGCPSelection.getMasterGcpGroup());
		//add gcps to the GCPManager
		for (String name : bandsListGCP.keySet()) {
			final ProductNodeGroup<Placemark> targetGCPGroup = GCPManager.instance()
					.getGcpGroup(myGCPSelection.getTargetProduct().getBand(name));
			Map<Integer, Placemark> map = gcpsMap.get(name);
			for (Placemark p : map.values()) {
				targetGCPGroup.add(p);
			}
			System.out.println("gcps"+targetGCPGroup.getNodeCount());
		}
		//compute the warp function and broadcast it to all workers.
		long startWarpTime = System.currentTimeMillis();
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
		//SerialCoregistration.testGCPs(gcpsMap);
		//Checks.checkGCPs2(gcpsMap, masterGcps.getValue());
		Broadcast<Map<String, WarpData>> warpDataMapB = sc.broadcast(warpdataMap);

		Object2ObjectMap<String,Object2ObjectMap<Point,ObjectSet<Rectangle>>> dependRectsWarp2 = new Object2ObjectOpenHashMap<String,Object2ObjectMap<Point,ObjectSet<Rectangle>>> ();
		Object2ObjectMap<String,Object2ObjectMap<Rectangle, ObjectList<Point>>> dependPointsWarp = new Object2ObjectOpenHashMap<String,Object2ObjectMap<Rectangle, ObjectList<Point>>>();
		
		getSourceDependWarp(myWarp,warpdataMap,imageMetadata,dependRectsWarp2,dependPointsWarp);
		Broadcast<Object2ObjectMap<String,Object2ObjectMap<Point,ObjectSet<Rectangle>>>> dependRectsWarpB2 = sc.broadcast(dependRectsWarp2);
		Broadcast<Object2ObjectMap<String,Object2ObjectMap<Rectangle, ObjectList<Point>>>> dependPointsWarpB = sc.broadcast(dependPointsWarp);
		
		JavaPairRDD<Tuple2<String, Rectangle>, MyTile> dependentPairsWarp = createstackResults
				.flatMapToPair((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
					List<Tuple2<Tuple2<String, Rectangle>, MyTile>> pairs = new ArrayList<Tuple2<Tuple2<String, Rectangle>, MyTile>>();
					Object2ObjectMap<String,Object2ObjectMap<Point,ObjectSet<Rectangle>>> dependRectsMap = dependRectsWarpB2.getValue();
					Object2ObjectMap<Point,ObjectSet<Rectangle>> bandRects=dependRectsMap.get(pair._1._2);
					ImageMetadata srcImgMetadataWarp = imgMetadataB.getValue().get(pair._1._2 +"_warp"+ "_source");
					Point sourcePoint = srcImgMetadataWarp.getTileIndices(pair._2.getMinX(), pair._2.getMinY());
					Set<Rectangle> tuples = bandRects.get(sourcePoint);
					if(tuples!=null){
					for (Rectangle rect:tuples)
						pairs.add(new Tuple2<Tuple2<String, Rectangle>, MyTile>(
								new Tuple2<String, Rectangle>(pair._1._2, rect), pair._2));
					}
					return pairs;
				});

		long endWarpTime = System.currentTimeMillis();
		long totalWarpTime = endWarpTime - startWarpTime;
		System.out.println("warp time "+totalWarpTime);
		
		//run warp operator
		JavaPairRDD<Tuple2<String, Rectangle>, Iterable<MyTile>> warpResults1 = dependentPairsWarp.groupByKey();			
		JavaPairRDD<Tuple2<Point, String>, MyTile> warpResults = warpResults1
				.flatMapToPair((Tuple2<Tuple2<String, Rectangle>, Iterable<MyTile>> pair) -> {
					List<Tuple2<Tuple2<Point, String>, MyTile>> trgtiles = new ArrayList<Tuple2<Tuple2<Point, String>, MyTile>>();
					Object2ObjectMap<String,Object2ObjectMap<Rectangle, ObjectList<Point>>> dependRectsMap = dependPointsWarpB.getValue();
					Object2ObjectMap<Rectangle, ObjectList<Point>> pointsRect=dependRectsMap.get(pair._1._1);
					ImageMetadata srcImgMetadataWarp = imgMetadataB.getValue().get(pair._1._1() +"_warp"+ "_source");

					//NullOpImage img=new NullOpImage();
					int bufferType = ImageManager.getDataBufferType(srcImgMetadataWarp.getDataType());
//					final SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(bufferType,
//							srcImgMetadataWarp.getImageWidth(), srcImgMetadataWarp.getImageHeight());
					final SampleModel sampleModel1 = ImageUtils.createSingleBandedSampleModel(bufferType,
							srcImgMetadataWarp.getTileSize().width, srcImgMetadataWarp.getTileSize().height);
					final ColorModel cm = PlanarImage.createColorModel(sampleModel1);
			//		WritableRaster raster = RasterFactory.createWritableRaster(sampleModel, new Point(0, 0));
		//			BufferedImage img1 = new BufferedImage(cm, raster, false, new java.util.Hashtable());
					
//					RenderedOp rop=Tools.createTiledImage(img1, srcImgMetadataWarp.getTileSize().width, srcImgMetadataWarp.getTileSize().height);
//					RenderedImage img=rop.getRendering();
//					TiledImage img=new TiledImage(img1,
//							srcImgMetadataWarp.getTileSize().width,srcImgMetadataWarp.getTileSize().height);
					TiledImage img = new TiledImage(0,0, srcImgMetadataWarp.getImageWidth(),srcImgMetadataWarp.getImageHeight(),0,0,
							sampleModel1,cm);
					List<MyTile> tiles = Lists.newArrayList(pair._2.iterator());
					List<Point> targetPoints = pointsRect.get(pair._1._2);
					for(MyTile myTile: tiles) {
						//img2.setData(myTile.getWritableRaster());
						Point ind=srcImgMetadataWarp.getTileIndices(myTile.getMinX(), myTile.getMinY());
						img.getWritableTile(ind.x,ind.y).setDataElements(myTile.getMinX(), myTile.getMinY(), myTile.getWidth(),
								myTile.getHeight(), myTile.getRawSamples().getElems());
//						img.getRaster().setDataElements(myTile.getMinX(), myTile.getMinY(), myTile.getWidth(),
//								myTile.getHeight(), myTile.getRawSamples().getElems());

					}
//					for(int i=0;i<tiles.size();i++){
//						Point point=srcImgMetadataWarp.getTileIndices(tiles.get(i).getMinX(), tiles.get(i).getMinY());
//						DataBuffer bf=img.getTile(point.x, point.y).getDataBuffer();
//						boolean eq=Arrays.equals((float[])tiles.get(i).getDataBuffer().getElems(),((DataBufferFloat) bf).getData());
//						System.out.println(eq);
//					}
					WarpMetadata w1 = warpMetadataB.getValue();
					Map<String, WarpData> map = warpDataMapB.getValue();
					WarpData w = map.get(pair._1._1());
					Warp warp = new Warp(w1.getInterp(), w, w1.getInterpTable());
					// get warped image
					ImageMetadata trgImgMetadataWarp = imgMetadataB.getValue().get(pair._1._1() +"_warp"+ "_target");
					Map<String,ImageMetadata> map2=imgMetadataB.getValue();
					ImageMetadata stackImgMetadata = map2.get(pair._1._1()+"_"+"stack");
					RenderedOp warpedImage = warp.createWarpImage(warp.getWarpData().jaiWarp, img);
					for (Point p : targetPoints) {
						//System.out.println(" starts warp for point "+p);
						Point point=p;
						long startWarpPoint=System.currentTimeMillis();
						MyTile targetTile = new MyTile(trgImgMetadataWarp.getWritableRaster(p.x, p.y),
								trgImgMetadataWarp.getRectangle(p.x, p.y), trgImgMetadataWarp.getDataType());
						warp.computeTile(targetTile, warpedImage);

						//trgtiles.add(new Tuple2<Tuple2<Point, String>, MyTile>(
								//new Tuple2<Point, String>(p, stackImgMetadata.getBandPairName()), targetTile));
						trgtiles.add(new Tuple2<Tuple2<Point, String>, MyTile>(
								new Tuple2<Point, String>(p, trgImgMetadataWarp.getBandName()), targetTile));
						//System.out.println(System.currentTimeMillis()-startWarpPoint+" ends warp for point "+p);
					}
					return trgtiles;
				});
		//.repartition(10);
		
		//run chage detection
		JavaPairRDD<Tuple2<Point, String>, MyTile> changeDResults=masterRastersCal.join(warpResults).mapToPair((Tuple2<Tuple2<Point,String>, Tuple2<MyTile, MyTile>> pair) -> {
			ChangeDetectionMetadata metadata=changeDMetadataB.getValue();
						ChangeDetection changeDetection = new ChangeDetection(
								metadata.isOutputLogRatio(),
								metadata.getNoDataValueN(),
								metadata.getNoDataValueD());
						ImageMetadata trgImgMetadata=imgMetadataB.getValue().get("ratio_changeD");
						MyTile targetTile = new MyTile(trgImgMetadata.getWritableRaster(pair._1._1.x, pair._1._1.y),
								trgImgMetadata.getRectangle(pair._1._1.x, pair._1._1.y),
								trgImgMetadata.getDataType());
						try {
							changeDetection.computeTile(pair._2._1, pair._2._2, targetTile, targetTile.getRectangle());
						} catch (ArrayIndexOutOfBoundsException e) {
			
							System.out.println("key: " + pair._1);
							System.out.println("");
						}
			
						return new Tuple2<Tuple2<Point, String>, MyTile>(new Tuple2<Point,String>(pair._1._1,"ratio"), targetTile);
					});
		List<Tuple2<Tuple2<Point, String>, MyTile>> warpRes = warpResults.collect();
		Write write = new Write(myWarp.getTargetProduct(), targetFile, "BEAM-DIMAP");
		for(int i=0;i<warpRes.size();i++){
			Band targetBand=writeOp.getTargetProduct().getBand(warpRes.get(i)._1._2);
			write.storeTile(targetBand, warpRes.get(i)._2);
		}	
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println(totalTime);
//		//Checks.checkChangeDetection(changeDRes);
		//Checks.checkWarp(warpRes);
		
	}

	private void getDependRects(Object2ObjectMap<String, ImageMetadata> imageMetadata, MyCreateStack myCreateStack2,Object2ObjectMap<String, Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>>> dependRects) {
		Map<Product, int[]> slaveOffsetMap = myCreateStack2.getSlaveOffsettMap();
		Map<Band, Band> sourceRasterMap = myCreateStack2.getSourceRasterMap();
		Product targetProductStack = myCreateStack2.getTargetProduct();
		String[] masterBandNames = StackUtils.getMasterBandNames(targetProductStack);
		Set<String> masterBands = new HashSet(Arrays.asList(masterBandNames));

		LoopLimits limits = new LoopLimits(targetProductStack);

		for (int i = 0; i < targetProductStack.getNumBands(); i++) {
			Band trgBand = targetProductStack.getBandAt(i);
			if (masterBands.contains(trgBand.getName())) {
				continue;
			}
			ImageMetadata srcMetadata = imageMetadata.get(sourceRasterMap.get(trgBand).getName()+"_"+"stack");
			ImageMetadata trgMetadata = imageMetadata.get(trgBand.getName()+"_"+"stack");
			for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
				for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
					Rectangle dependRect = OperatorDependencies.createStack(trgMetadata.getRectangle(tileX, tileY),
							slaveOffsetMap.get(sourceRasterMap.get(trgBand).getProduct()), srcMetadata.getImageWidth(),
							srcMetadata.getImageHeight());
					Point[] points = sourceRasterMap.get(trgBand).getSourceImage().getTileIndices(dependRect);
					for (Point p : points) {
						String bandName = sourceRasterMap.get(trgBand).getName();
						if (dependRects.containsKey(bandName)) {
							Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>> rects = dependRects.get(bandName);
							if (rects.containsKey(p)) {
								rects.get(p).add(new Tuple2<Point, Rectangle>(new Point(tileX, tileY), dependRect));
							} else {
								ObjectList<Tuple2<Point, Rectangle>> list = new ObjectArrayList<Tuple2<Point, Rectangle>>();
								list.add(new Tuple2<Point, Rectangle>(new Point(tileX, tileY), dependRect));
								rects.put(p, list);
							}
						} else {
							Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>> rectsMap = new Object2ObjectOpenHashMap<Point, ObjectList<Tuple2<Point, Rectangle>>>();
							ObjectList<Tuple2<Point, Rectangle>> list = new ObjectArrayList<Tuple2<Point, Rectangle>>();
							list.add(new Tuple2<Point, Rectangle>(new Point(tileX, tileY), dependRect));
							rectsMap.put(p, list);
							dependRects.put(bandName, rectsMap);
						}
					}
				}
			}

		}
		
	}
	public void getSourceDependWarp(MyWarp myWarp,Map<String, WarpData> warpdataMap,Map<String,ImageMetadata> imageMetadata,Object2ObjectMap<String,Object2ObjectMap<Point,ObjectSet<Rectangle>>> dependRectsWarp2,Object2ObjectMap<String,Object2ObjectMap<Rectangle, ObjectList<Point>>> dependPointsWarp ){
		Product targetProductWarp = myWarp.getTargetProduct();
		String[] masterBandNamesWarp = StackUtils.getMasterBandNames(targetProductWarp);
		Set<String> masterBandsWarp = new HashSet(Arrays.asList(masterBandNamesWarp));
		LoopLimits limits2 = new LoopLimits(targetProductWarp);
		LoopLimits limits1 = new LoopLimits(myWarp.getSourceProduct());
		//System.out.println("target tiles "+limits2.getNumXTiles()*limits2.getNumYTiles());
		//System.out.println("source tiles "+limits1.getNumXTiles()*limits1.getNumYTiles());
		Map<Band,Band> sourceRasterMap =myWarp.getSourceRasterMap();
		for (int i = 0; i < targetProductWarp.getNumBands(); i++) {
			if (masterBandsWarp.contains(targetProductWarp.getBandAt(i).getName()))
				continue;
			//ImageLayout imageLayout=ImageManager.createSingleBandedImageLayout(targetProductWarp.getBandAt(i));
			TiledImage img=new TiledImage((RenderedImage)Utils.createSourceImages(targetProductWarp.getBandAt(i)),
					(int)targetProductWarp.getPreferredTileSize().getWidth(),(int)targetProductWarp.getPreferredTileSize().getHeight());
			targetProductWarp.getBandAt(i).setSourceImage(img);
			final RenderedOp warpedImage = myWarp.createWarpImage(
					warpdataMap.get(targetProductWarp.getBandAt(i).getName()).jaiWarp,
					(RenderedImage) targetProductWarp.getBandAt(i).getSourceImage());

			ImageMetadata trgImgMetadataWarp = imageMetadata
					.get(targetProductWarp.getBandAt(i).getName()+"_warp" + "_target");
			WarpOpImage wimg = (WarpOpImage) warpedImage.getRendering();
			for (int tileY = 0; tileY < limits2.getNumYTiles(); tileY++) {
				for (int tileX = 0; tileX < limits2.getNumXTiles(); tileX++) {
					Point[] points = warpedImage.getTileIndices(trgImgMetadataWarp.getRectangle(tileX, tileY));
					Rectangle finalRect = null;
					//System.out.println("target tile "+"("+tileX+","+tileY+")");
					for (int j = 0; j < points.length; j++) {
						
						//System.out.println(j+" tile index at warpedimg "+points[j]);
						Rectangle rect = wimg.getTileRect(points[j].x, points[j].y);
						Rectangle rect2 = wimg.mapDestRect(rect, 0);
						//System.out.println(j+" source rect "+rect2);
						if (j == 0)
							finalRect = rect2;
						else
							finalRect = finalRect.union(rect2);

					}
					//System.out.println("final source depend rect of target tile "+"("+tileX+","+tileY+") :"+finalRect);
					if (finalRect != null) {
						
						if (dependPointsWarp.containsKey(targetProductWarp.getBandAt(i).getName())) {
						Object2ObjectMap<Rectangle, ObjectList<Point>> map=dependPointsWarp.get(targetProductWarp.getBandAt(i).getName());
						if (map.containsKey(finalRect)) {
							List<Point> rects=map.get(finalRect);
							rects.add(new Point(tileX,tileY));
						}else{
							ObjectList<Point>  list = new ObjectArrayList<Point>();
							list.add(new Point(tileX,tileY));
							map.put(finalRect,list );
						
						}
						}else{
							Object2ObjectMap<Rectangle, ObjectList<Point>> map=new Object2ObjectOpenHashMap<Rectangle, ObjectList<Point>>();
							ObjectList<Point>  list = new ObjectArrayList<Point>();
							list.add(new Point(tileX,tileY));
							map.put(finalRect,list );
							dependPointsWarp.put(targetProductWarp.getBandAt(i).getName(), map);
						}
						Point[] ps = sourceRasterMap.get(targetProductWarp.getBandAt(i)).getSourceImage().getTileIndices(finalRect);
						//System.out.println("final source depend rect of target tile "+"("+tileX+","+tileY+") intersects source tiles :"+ps.length);
						for (Point p : ps) {
						if (dependRectsWarp2.containsKey(targetProductWarp.getBandAt(i).getName())) {
							Object2ObjectMap<Point,ObjectSet<Rectangle>> pointsRects = dependRectsWarp2.get(targetProductWarp.getBandAt(i).getName());
							if (pointsRects.containsKey(p)) {
								Set<Rectangle> rects=pointsRects.get(p);
								rects.add(finalRect);
							} else {
								ObjectSet<Rectangle>  rects = new ObjectOpenHashSet<Rectangle>();
								rects.add(finalRect);
								pointsRects.put(p, rects);
							}
						} else {
							Object2ObjectMap<Point,ObjectSet<Rectangle>> pointsRects =  new Object2ObjectOpenHashMap<Point,ObjectSet<Rectangle>>();
							ObjectSet<Rectangle>  rects = new ObjectOpenHashSet<Rectangle>();
							rects.add(finalRect);
							pointsRects.put(p, rects);
							dependRectsWarp2.put(targetProductWarp.getBandAt(i).getName(), pointsRects);
							
						}
						}
					}
				}
			}
		}
	}
	private static RenderedOp createTiledAndCachedImage(BufferedImage image,int tileHeight,int tileWidth) {
        final ImageLayout imageLayout = new ImageLayout();
        imageLayout.setTileWidth(tileWidth);
        imageLayout.setTileHeight(tileHeight);
        final RenderingHints hints = new RenderingHints(JAI.KEY_IMAGE_LAYOUT, imageLayout);
        hints.add(new RenderingHints(JAI.KEY_TILE_CACHE, JAI.getDefaultInstance().getTileCache()));
        return FormatDescriptor.create(image, image.getSampleModel().getDataType(), hints);
    }
	
}
