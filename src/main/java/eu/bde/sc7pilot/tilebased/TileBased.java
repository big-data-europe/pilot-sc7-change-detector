package eu.bde.sc7pilot.tilebased;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
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
import javax.media.jai.RasterFactory;
import javax.media.jai.RenderedOp;
import javax.media.jai.WarpOpImage;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.esa.s1tbx.insar.gpf.coregistration.GCPManager;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Placemark;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductNodeGroup;
import org.esa.snap.core.datamodel.TiePointGeoCoding;
import org.esa.snap.core.image.ImageManager;
import org.esa.snap.core.util.ImageUtils;
import org.esa.snap.engine_utilities.gpf.StackUtils;

import com.google.common.collect.Lists;

import eu.bde.sc7pilot.hdfsreader.BandInfo;
import eu.bde.sc7pilot.hdfsreader.ReadHDFSTile;
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
import eu.bde.sc7pilot.taskbased.Utils;
import eu.bde.sc7pilot.taskbased.calibration.MyCalibration;
import eu.bde.sc7pilot.tilebased.metadata.CalibrationMetadata;
import eu.bde.sc7pilot.tilebased.metadata.ChangeDetectionMetadata;
import eu.bde.sc7pilot.tilebased.metadata.GCPMetadata;
import eu.bde.sc7pilot.tilebased.metadata.ImageMetadata;
import eu.bde.sc7pilot.tilebased.metadata.WarpMetadata;
import eu.bde.sc7pilot.tilebased.model.MyTile;
import eu.bde.sc7pilot.tilebased.operator.ChangeDetection;
import eu.bde.sc7pilot.tilebased.operator.CreateStack;
import eu.bde.sc7pilot.tilebased.operator.GCPSelection;
import eu.bde.sc7pilot.tilebased.operator.Sentinel1Calibrator;
import eu.bde.sc7pilot.tilebased.operator.Warp;
import eu.bde.sc7pilot.tilebased.operator.Write;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import scala.Tuple2;
import scala.Tuple3;

public class TileBased {

	public static void main(String[] args) throws Exception {
		TileBased parallelTiles = new TileBased();
		parallelTiles.processTiles(args[0],args[1],args[2],args[3]);
	}

	public void processTiles(String hdfsPath,String masterZipFilePath,String slaveZipFilePath,String targetPath) throws Exception {
       
        ZipHandler2 zipHandler = new ZipHandler2();
        //String masterTiffInHDFS = "/home/hadoop/s1a-iw-grd-vv-20141225t142407-20141225t142436-003877-004a54-001.tiff";
       // String slaveTiffInHDFS = "/home/hadoop/s1a-iw-grd-vv-20150518t142409-20150518t142438-005977-007b49-001.tiff";
        String masterTiffInHDFS = "";
        String slaveTiffInHDFS = "";
        try {
        	masterTiffInHDFS = zipHandler.tiffToHDFS(masterZipFilePath, hdfsPath);
        	slaveTiffInHDFS = zipHandler.tiffToHDFS(slaveZipFilePath, hdfsPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
		long startTime = System.currentTimeMillis();
		List<Tuple2<String, Point>> slaveIndices = null;
		List<Tuple2<String, Point>> masterIndices = null;
		Map<String,BandInfo> bandInfos=new HashMap<>(2);
		Object2ObjectMap<String, Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>>> dependRects = new Object2ObjectOpenHashMap<String, Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>>>();

		// read slave and master files as usual
		SerialProcessor sp = new SerialProcessor();
		String[] selectedPolarisations=null;
		Product masterTargetProduct =null;
		Product slaveTargetProduct=null;
		try {
			masterTargetProduct = zipHandler.findTargetProduct(masterZipFilePath);
			for(int i=0;i<masterTargetProduct.getNumBands();i++){
				Band band=masterTargetProduct.getBandAt(i);
				if(band.getClass()==Band.class){
					BandInfo bandInfo=new BandInfo(masterTiffInHDFS,i,0);
					bandInfos.put(band.getName()+"_read1", bandInfo);
				}
			}
			masterIndices=sp.readTilesIndices(masterTargetProduct,selectedPolarisations);
		} catch (Exception e) {
			throw e;
		}

		try {
			slaveTargetProduct = zipHandler.findTargetProduct(slaveZipFilePath);
			for(int i=0;i<slaveTargetProduct.getNumBands();i++){
				Band band=slaveTargetProduct.getBandAt(i);
				if(band.getClass()==Band.class){
					BandInfo bandInfo=new BandInfo(slaveTiffInHDFS,i,0);
					bandInfos.put(band.getName()+"_read2", bandInfo);
				}
			}
			slaveIndices=sp.readTilesIndices(slaveTargetProduct,selectedPolarisations);
		} catch (Exception e) {
			throw e;
		}
		//initialize all operators as usual to acquire the necessary metadata
		MyBandSelect bandselect1=new MyBandSelect(selectedPolarisations,null);
		bandselect1.setSourceProduct(masterTargetProduct);
		sp.initOperator(bandselect1);
		MyBandSelect bandselect2=new MyBandSelect(selectedPolarisations,null);
		bandselect2.setSourceProduct(slaveTargetProduct);
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
		
		File targetFile = new File(targetPath, "changeDetection-output");
		MyWrite writeOp = new MyWrite(myChangeDetection.getTargetProduct(), targetFile, "BEAM-DIMAP");
		writeOp.setId("write");
		sp.initOperator(writeOp);
		//initialize the imageMetadata for all bands and operators.
		//The imageMetadata class has replaced the band class and contains only the absolutely essential metadata for tile computations
		Object2ObjectMap<String, ImageMetadata> imageMetadata = new Object2ObjectOpenHashMap<String, ImageMetadata>(
				myCalibration1.getTargetProduct().getNumBands() * 3);
		OpMetadataCreator opMetadataCreator = new OpMetadataCreator();
		Object2ObjectMap<String, CalibrationMetadata> calMetadata = new Object2ObjectOpenHashMap<String, CalibrationMetadata>(
				myCalibration1.getTargetProduct().getNumBands() * 4);
		// read metadata.
		opMetadataCreator.createProdImgMetadata(imageMetadata, masterTargetProduct,
				"read1");
		opMetadataCreator.createProdImgMetadata(imageMetadata, slaveTargetProduct,
				"read2");
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
		SparkConf conf = new SparkConf().set("spark.driver.maxResultSize", "5g")
				.setAppName("First test on tile parallel processing");	
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
		
		Broadcast<Object2ObjectMap<String, Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>>>> dependRectsB = sc.broadcast(dependRects);
		Broadcast<ProductNodeGroup<Placemark>> masterGcps = sc.broadcast(myGCPSelection.getMasterGcpGroup());
		Broadcast<GCPMetadata> GCPMetadataBroad = sc.broadcast(GCPMetadata);
		Broadcast<Map<String, String>> bandsListGCPB = sc.broadcast(bandsListGCP);

		Broadcast<WarpMetadata> warpMetadataB = sc.broadcast(warpMetadata);
		Broadcast<ChangeDetectionMetadata> changeDMetadataB = sc.broadcast(changeDMetadata);
		
		Broadcast<Integer> rows = sc.broadcast(limits.getNumXTiles());
		Broadcast<Map<String,BandInfo>> bandInfosB = sc.broadcast(bandInfos);

		LoopLimits limits2 = new LoopLimits(myWarp.getTargetProduct());
		JavaPairRDD<String, Point> masterRastersRdd = sc.parallelizePairs(masterIndices,10);
		JavaPairRDD<String, Point> slaveRastersRdd = sc.parallelizePairs(slaveIndices,10);
		long startWithGCPTime = System.currentTimeMillis();
		//master image calibration
		JavaPairRDD<Tuple2<Point, String>, MyTile> masterRastersCal = masterRastersRdd
				.mapPartitionsToPair((Iterator<Tuple2<String,Point>> iterator)-> {
					List<Tuple2<Tuple2<Point, String>, MyTile>> tiles=new ArrayList<Tuple2<Tuple2<Point, String>, MyTile>>();
					Map<String,BandInfo> infos=bandInfosB.getValue();
					List<Tuple2<String,Point>> points = Lists.newArrayList(iterator);
					Map<String,ReadHDFSTile> bands=new HashMap<String,ReadHDFSTile>();
					
					for(int i=0;i<points.size();i++){
						ReadHDFSTile readHDFSTile=null;
						Tuple2<String,Point> tuple=points.get(i);
						if(!bands.containsKey(tuple._1+"_read1")){
							BandInfo info=infos.get(tuple._1+"_read1");
							readHDFSTile=new ReadHDFSTile(info.getHdfsPath());
							bands.put(tuple._1+"_read1",readHDFSTile);
						}else
							readHDFSTile=bands.get(tuple._1+"_read1");
						Map<String, ImageMetadata> map = imgMetadataB.getValue();
						ImageMetadata srcImgMetadataCal1 = imgMetadataB.getValue().get(tuple._1+"_"+"cal1");
						ImageMetadata trgImgMetadataCal1 = map.get(srcImgMetadataCal1.getBandPairName()+"_"+"cal1");
						
						ImageMetadata imgMetadataRead = map.get(tuple._1+"_"+"read1");
						MyTile readTile=new MyTile(imgMetadataRead.getWritableRaster(tuple._2.x, tuple._2.y),
								imgMetadataRead.getRectangle(tuple._2.x, tuple._2.y),
								imgMetadataRead.getDataType());
						readHDFSTile.readTile( readTile,infos.get(tuple._1+"_"+"read1"));
						
						Point targetPoint = tuple._2;
						CalibrationMetadata calMeatadata = calMetadataB.getValue().get(tuple._1+"_"+"cal1");
						Sentinel1Calibrator sentinel1Calibrator = new Sentinel1Calibrator(calMeatadata);
						MyTile targetTile = new MyTile(trgImgMetadataCal1.getWritableRaster(targetPoint.x, targetPoint.y),
								trgImgMetadataCal1.getRectangle(targetPoint.x, targetPoint.y),
								trgImgMetadataCal1.getDataType());
						sentinel1Calibrator.computeTile(readTile, null, targetTile, srcImgMetadataCal1.getNoDataValue(),
								trgImgMetadataCal1.getBandName());
						tiles.add(new Tuple2<Tuple2<Point, String>, MyTile>(
								new Tuple2<Point, String>(targetPoint, trgImgMetadataCal1.getBandName()), targetTile));
					}
					
				return tiles;
				}).cache();
		JavaPairRDD<String, MyTile> slaveRastersCal = slaveRastersRdd
				.mapPartitionsToPair((Iterator<Tuple2<String,Point>> iterator)-> {
					List<Tuple2<String, MyTile>> tiles=new ArrayList<Tuple2<String, MyTile>>();
					Map<String,BandInfo> infos=bandInfosB.getValue();
					List<Tuple2<String,Point>> points = Lists.newArrayList(iterator);
					Map<String,ReadHDFSTile> bands=new HashMap<String,ReadHDFSTile>();
					
					for(int i=0;i<points.size();i++){
						ReadHDFSTile readHDFSTile=null;
						Tuple2<String,Point> tuple=points.get(i);
						if(!bands.containsKey(tuple._1+"_"+"read2")){
							BandInfo info=infos.get(tuple._1+"_"+"read2");
							readHDFSTile=new ReadHDFSTile(info.getHdfsPath());
							bands.put(tuple._1+"_"+"read2",readHDFSTile);
						}else
							readHDFSTile=bands.get(tuple._1+"_read2");
						Map<String, ImageMetadata> map = imgMetadataB.getValue();
						ImageMetadata srcImgMetadataCal2 = imgMetadataB.getValue().get(tuple._1+"_"+"cal2");
						ImageMetadata trgImgMetadataCal2 = map.get(srcImgMetadataCal2.getBandPairName()+"_"+"cal2");
						
						ImageMetadata imgMetadataRead = map.get(tuple._1+"_"+"read2");
						MyTile readTile=new MyTile(imgMetadataRead.getWritableRaster(tuple._2.x, tuple._2.y),
								imgMetadataRead.getRectangle(tuple._2.x, tuple._2.y),
								imgMetadataRead.getDataType());
						readHDFSTile.readTile( readTile,infos.get(tuple._1+"_"+"read2"));
						
						Point targetPoint = tuple._2;
						CalibrationMetadata calMeatadata = calMetadataB.getValue().get(tuple._1+"_"+"cal2");
						Sentinel1Calibrator sentinel1Calibrator = new Sentinel1Calibrator(calMeatadata);
						MyTile targetTile = new MyTile(trgImgMetadataCal2.getWritableRaster(targetPoint.x, targetPoint.y),
								trgImgMetadataCal2.getRectangle(targetPoint.x, targetPoint.y),
								trgImgMetadataCal2.getDataType());
						sentinel1Calibrator.computeTile(readTile, null, targetTile, srcImgMetadataCal2.getNoDataValue(),
								trgImgMetadataCal2.getBandName());
						tiles.add( new Tuple2<String, MyTile>(trgImgMetadataCal2.getBandName(), targetTile));
					}
					
				return tiles;
				});
		JavaPairRDD<Tuple3<Point, String, Rectangle>, MyTile> dependentPairs = slaveRastersCal
				.flatMapToPair((Tuple2<String, MyTile> pair) -> {
					ImageMetadata srcImgMetadataStack = imgMetadataB.getValue().get(pair._1+"_"+"stack");
					List<Tuple2<Tuple3<Point, String, Rectangle>, MyTile>> pairs = new ArrayList<Tuple2<Tuple3<Point, String, Rectangle>, MyTile>>();
					Object2ObjectMap<String, Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>>> dependRectsMap = dependRectsB.getValue();
					Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>> dependRectangles = dependRectsMap.get(pair._1);

					Point sourcePoint = srcImgMetadataStack.getTileIndices(pair._2.getMinX(), pair._2.getMinY());
					List<Tuple2<Point, Rectangle>> tuples = dependRectangles.get(sourcePoint);
					for (Tuple2<Point, Rectangle> tuple : tuples)
						pairs.add(new Tuple2<Tuple3<Point, String, Rectangle>, MyTile>(
								new Tuple3<Point, String, Rectangle>(tuple._1(), pair._1(), tuple._2()), pair._2));
					return pairs;
				});

		JavaPairRDD<Tuple2<Point, String>, MyTile> createstackResults = dependentPairs.groupByKey()
				.mapToPair((Tuple2<Tuple3<Point, String, Rectangle>, Iterable<MyTile>> pair) -> {
					Rectangle dependentRect = pair._1._3();
					Iterator<MyTile> iterator = pair._2.iterator();
					boolean first = true;
					MyTile sourceTile = null;
					Rectangle currRect = null;
					WritableRaster destinationRaster = null;
					int tileType = -1;
					while (iterator.hasNext()) {
						MyTile myTile = (MyTile) iterator.next();
						tileType = myTile.getType();
						currRect = dependentRect.intersection(myTile.getRectangle());
						if (first && !iterator.hasNext()) {
							WritableRaster currRaster = myTile.getWritableRaster();
							Raster sourceRaster = currRaster.getBounds().equals(currRect) ? currRaster
									: currRaster.createChild(currRect.x, currRect.y, currRect.width, currRect.height,
											currRect.x, currRect.y, null);
							destinationRaster = Raster.createWritableRaster(sourceRaster.getSampleModel(),
									sourceRaster.getDataBuffer(), currRaster.getBounds().getLocation());
							break;
						} else {
							if (first) {

								destinationRaster = Utils.createWritableRaster(dependentRect, myTile.getType());
								first = false;
							}
							Object dataBuffer = ImageUtils.createDataBufferArray(
									myTile.getWritableRaster().getTransferType(),
									(int) currRect.getWidth() * (int) currRect.getHeight());
							myTile.getWritableRaster().getDataElements((int) currRect.getMinX(),
									(int) currRect.getMinY(), (int) currRect.getWidth(), (int) currRect.getHeight(),
									dataBuffer);
							destinationRaster.setDataElements((int) currRect.getMinX(), (int) currRect.getMinY(),
									(int) currRect.getWidth(), (int) currRect.getHeight(), dataBuffer);
						}
					}
					sourceTile = new MyTile(destinationRaster, dependentRect, tileType);

					// compute tile, createstack
					CreateStack createStack = new CreateStack();
					ImageMetadata srcImgMetadataStack = imgMetadataB.getValue().get(pair._1._2()+"_"+"stack");
					ImageMetadata trgImgMetadataStack = imgMetadataB.getValue()
							.get(srcImgMetadataStack.getBandPairName()+"_"+"stack");
					MyTile targetTile = new MyTile(
							trgImgMetadataStack.getWritableRaster(pair._1._1().x, pair._1._1().y),
							trgImgMetadataStack.getRectangle(pair._1._1().x, pair._1._1().y),
							trgImgMetadataStack.getDataType());
					createStack.computeTile(targetTile, sourceTile, srcImgMetadataStack.getOffsetMap(),
							srcImgMetadataStack.getImageWidth(), srcImgMetadataStack.getImageHeight(),
							trgImgMetadataStack.getNoDataValue());
					return new Tuple2<Tuple2<Point, String>, MyTile>(
							new Tuple2<Point, String>(pair._1._1(), trgImgMetadataStack.getBandName()), targetTile);
				});
		
		//split the createstack tiles in groups of rows with a unique key to each group
		JavaPairRDD<Tuple2<Integer, String>, MyTile> createstackResultsRows = createstackResults
				.filter((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
					Map<String, String> bandsList = bandsListGCPB.getValue();
					return bandsList.containsKey(pair._1._2);
				}).flatMapToPair((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
					List<Tuple2<Tuple2<Integer, String>, MyTile>> pairs = new ArrayList<Tuple2<Tuple2<Integer, String>, MyTile>>();
					int rowsCount = rows.getValue();
					int nOfKeys = (int) Math.ceil((float) rowsCount / (float) 4);
					int key = 0;
					ImageMetadata trgImgMetadatastack = imgMetadataB.getValue().get(pair._1._2+"_stack");
					int y = trgImgMetadatastack.getTileIndices(pair._2.getMinX(), pair._2.getMinY()).y;
					for (int i = 1; i <= nOfKeys; i++) {
						if (y < 8 * i && y >= 8 * (i - 1)) {
							key = i;
							pairs.add(new Tuple2<Tuple2<Integer, String>, MyTile>(
									new Tuple2<Integer, String>(key, pair._1._2), pair._2));
							if (i != nOfKeys && (y == 8 * i - 1))
								pairs.add(new Tuple2<Tuple2<Integer, String>, MyTile>(
										new Tuple2<Integer, String>(key + 1, pair._1._2), pair._2));
							if (i != 1 && (y == 8 * (i - 1)))
								pairs.add(new Tuple2<Tuple2<Integer, String>, MyTile>(
										new Tuple2<Integer, String>(key - 1, pair._1._2), pair._2));
							break;
						}
					}

					return pairs;
				});
		//split the master tiles in groups of rows with a unique key to each group
		JavaPairRDD<Tuple2<Integer, String>, MyTile> masterRastersRdd2 = masterRastersCal
				.filter((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
					String name = imgMetadataB.getValue().get(pair._1._2+"_stack").getBandPairName();
					Map<String, String> bandsList = bandsListGCPB.getValue();
					return bandsList.containsKey(name);
				}).flatMapToPair((Tuple2<Tuple2<Point, String>, MyTile> pair) -> {
					List<Tuple2<Tuple2<Integer, String>, MyTile>> pairs = new ArrayList<Tuple2<Tuple2<Integer, String>, MyTile>>();
					ImageMetadata trgImgMetadataCal1 = imgMetadataB.getValue().get(pair._1._2+"_cal1");
					
					String name = imgMetadataB.getValue().get(pair._1._2+"_stack").getBandPairName();
					int rowsCount = rows.getValue();
					int nOfKeys = (int) Math.ceil((float) rowsCount / (float) 4);
					int key = 0;
					int y = trgImgMetadataCal1.getTileIndices(pair._2.getMinX(), pair._2.getMinY()).y;
					for (int i = 1; i <= nOfKeys; i++) {
						if (y < 8 * i && y >= 8 * (i - 1)) {
							key = i;
							pairs.add(new Tuple2<Tuple2<Integer, String>, MyTile>(
									new Tuple2<Integer, String>(key, name),
									pair._2));
							if (i != nOfKeys && (y == 8 * i - 1))
								pairs.add(new Tuple2<Tuple2<Integer, String>, MyTile>(new Tuple2<Integer, String>(
										key + 1, name), pair._2));
							if (i != 1 && (y == 8 * (i - 1)))
								pairs.add(new Tuple2<Tuple2<Integer, String>, MyTile>(new Tuple2<Integer, String>(
										key - 1, name), pair._2));
							break;
						}
					}

					return pairs;

				});
		
		//gcps computation. group by key the groups of rows and and compute the gcps contained to each group
		//Then, collect the gcps to the master node.
		JavaPairRDD<Tuple2<Integer, String>, Iterable<MyTile>> masterRows = masterRastersRdd2.groupByKey();
		JavaPairRDD<Tuple2<Integer, String>, Iterable<MyTile>> stacktilesRows = createstackResultsRows.groupByKey();
		List<Tuple2<String, Tuple2<Integer, Placemark>>> slaveGCPs = masterRows.join(stacktilesRows)
				.flatMap((Tuple2<Tuple2<Integer, String>, Tuple2<Iterable<MyTile>, Iterable<MyTile>>> pair) -> {
					List<MyTile> masterTiles = Lists.newArrayList(pair._2._1.iterator());
					List<MyTile> slaveTiles = Lists.newArrayList(pair._2._2.iterator());
					int x = masterTiles.get(0).getMinX();
					int y = masterTiles.get(0).getMinY();
					int width = 0;
					int height = 0;
					int type = 0;
					for (int i = 1; i < masterTiles.size(); i++) {
						MyTile masterTile = masterTiles.get(i);
						if (masterTile.getMinX() < x)
							x = masterTile.getMinX();
						if (masterTile.getMinY() < y)
							y = masterTile.getMinY();
						type = masterTile.getType();
					}
					for (int i = 0; i < masterTiles.size(); i++) {
						MyTile masterTile = masterTiles.get(i);
						if (masterTile.getMinX() == x)
							height += masterTile.getHeight();
						if (masterTile.getMinY() == y)
							width += masterTile.getWidth();
					}
					WritableRaster masterRaster = Utils.createWritableRaster(new Rectangle(x, y, width, height), type);
					for (int i = 0; i < masterTiles.size(); i++) {
						masterRaster.setDataElements(masterTiles.get(i).getMinX(), masterTiles.get(i).getMinY(),
								masterTiles.get(i).getWidth(), masterTiles.get(i).getHeight(),
								masterTiles.get(i).getRawSamples().getElems());
					}
					x = slaveTiles.get(0).getMinX();
					y = slaveTiles.get(0).getMinY();
					width = 0;
					height = 0;
					for (int i = 1; i < slaveTiles.size(); i++) {
						MyTile slaveTile = slaveTiles.get(i);
						if (slaveTile.getMinX() < x)
							x = slaveTile.getMinX();
						if (slaveTile.getMinY() < y)
							y = slaveTile.getMinY();
						type = slaveTile.getType();
					}
					for (int i = 0; i < slaveTiles.size(); i++) {
						MyTile slaveTile = slaveTiles.get(i);
						if (slaveTile.getMinX() == x)
							height += slaveTile.getHeight();
						if (slaveTile.getMinY() == y)
							width += slaveTile.getWidth();
					}
					WritableRaster slaveRaster = Utils.createWritableRaster(new Rectangle(x, y, width, height), type);
					for (int i = 0; i < slaveTiles.size(); i++) {
						slaveRaster.setDataElements(slaveTiles.get(i).getMinX(), slaveTiles.get(i).getMinY(),
								slaveTiles.get(i).getWidth(), slaveTiles.get(i).getHeight(),
								slaveTiles.get(i).getRawSamples().getElems());
					}
					MyTile masterTile = new MyTile(masterRaster,
							new Rectangle(x, y, masterRaster.getWidth(), masterRaster.getHeight()), type);
					MyTile slaveTile = new MyTile(slaveRaster,
							new Rectangle(x, y, slaveRaster.getWidth(), slaveRaster.getHeight()), type);

					List<Tuple2<String, Tuple2<Integer, Placemark>>> slaveGCPsRes = new ArrayList<Tuple2<String, Tuple2<Integer, Placemark>>>();
					ProductNodeGroup<Placemark> masterGcpGroup = masterGcps.getValue();
					final int numberOfMasterGCPs = masterGcpGroup.getNodeCount();
					GCPMetadata GCPMetadataBroad2 = GCPMetadataBroad.getValue();

					ImageMetadata trgImgMetadataGCP = imgMetadataB.getValue().get(pair._1._2 +"_gcp"+ "_target");
					ImageMetadata srcImgMetadataGCP = imgMetadataB.getValue().get(pair._1._2 +"_gcp"+ "_source");

					int[] iParams2 = { Integer.parseInt(GCPMetadataBroad2.getCoarseRegistrationWindowWidth()),
							Integer.parseInt(GCPMetadataBroad2.getCoarseRegistrationWindowHeight()),
							GCPMetadataBroad2.getMaxIteration(),
							Integer.parseInt(GCPMetadataBroad2.getRowInterpFactor()),
							Integer.parseInt(GCPMetadataBroad2.getColumnInterpFactor()),
							srcImgMetadataGCP.getImageWidth(), srcImgMetadataGCP.getImageHeight() };
					double[] dParams3 = { GCPMetadataBroad2.getGcpTolerance(), trgImgMetadataGCP.getNoDataValue(),
							srcImgMetadataGCP.getNoDataValue() };

					final int[] offset2 = new int[2];
					// long startgcpTime = System.currentTimeMillis();
					int rowsCount = rows.getValue();
					int nOfKeys = (int) Math.ceil((float) rowsCount / (float) 4);
					int bMinY = (int) masterTile.getRectangle().getMinY();
					int bHeight = (int) masterTile.getRectangle().getHeight();
					double tileHeight = masterTiles.get(0).getHeight();
					if (pair._1._1 != 1)
						bMinY = bMinY + (int) tileHeight;
					if (pair._1._1 != nOfKeys && pair._1._1 != 1)
						bHeight = bHeight - (int) (2 * tileHeight);
					else if (pair._1._1 == 1)
						bHeight = bHeight - (int) tileHeight;
					Rectangle bounds = new Rectangle((int) masterTile.getRectangle().getMinX(), bMinY,
							(int) masterTile.getRectangle().getWidth(), bHeight);
					GeoCoding geoCoding=trgImgMetadataGCP.getGeoCoding();
					if(geoCoding==null)
						geoCoding=new TiePointGeoCoding(trgImgMetadataGCP.getLatGrid(),trgImgMetadataGCP.getLonGrid());
					for (int i = 0; i < numberOfMasterGCPs; i++) {
						final Placemark mPin = masterGcpGroup.get(i);

						final PixelPos sGCPPixelPos = new PixelPos(mPin.getPixelPos().x + offset2[0],
								mPin.getPixelPos().y + offset2[1]);

						if (bounds.contains(new Point((int) sGCPPixelPos.x, (int) sGCPPixelPos.y))) {
							GCPSelection GCPSelection = new GCPSelection(iParams2, dParams3,
									geoCoding, masterTile, slaveTile);
							if (GCPSelection.checkMasterGCPValidity(mPin)
									&& GCPSelection.checkSlaveGCPValidity(sGCPPixelPos)) {
								Placemark sPin = GCPSelection.computeSlaveGCP(mPin, sGCPPixelPos);

								if (sPin != null)
									slaveGCPsRes.add(new Tuple2<String, Tuple2<Integer, Placemark>>(pair._1._2, new Tuple2<Integer, Placemark>(i, sPin)));
							}

						}
					}
					return slaveGCPsRes;
				}).collect();
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
//		//add gcps to the GCPManager
		for (String name : bandsListGCP.keySet()) {
			final ProductNodeGroup<Placemark> targetGCPGroup = GCPManager.instance()
					.getGcpGroup(myGCPSelection.getTargetProduct().getBand(name));
			Map<Integer, Placemark> map = gcpsMap.get(name);
			for (Placemark p : map.values()) {
				targetGCPGroup.add(p);
			}
		}
		//compute the warp function
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
					for (Rectangle rect:tuples)
						pairs.add(new Tuple2<Tuple2<String, Rectangle>, MyTile>(
								new Tuple2<String, Rectangle>(pair._1._2, rect), pair._2));
					return pairs;
				});

		JavaPairRDD<Tuple2<Point, String>, MyTile> warpResults = dependentPairsWarp.groupByKey()
				.flatMapToPair((Tuple2<Tuple2<String, Rectangle>, Iterable<MyTile>> pair) -> {
					List<Tuple2<Tuple2<Point, String>, MyTile>> trgtiles = new ArrayList<Tuple2<Tuple2<Point, String>, MyTile>>();
					Object2ObjectMap<String,Object2ObjectMap<Rectangle, ObjectList<Point>>> dependRectsMap = dependPointsWarpB.getValue();
					Object2ObjectMap<Rectangle, ObjectList<Point>> pointsRect=dependRectsMap.get(pair._1._1);
					ImageMetadata srcImgMetadataWarp = imgMetadataB.getValue().get(pair._1._1() +"_warp"+ "_source");

					int bufferType = ImageManager.getDataBufferType(srcImgMetadataWarp.getDataType());
					final SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(bufferType,
							srcImgMetadataWarp.getImageWidth(), srcImgMetadataWarp.getImageHeight());
					final ColorModel cm = PlanarImage.createColorModel(sampleModel);
					WritableRaster raster = RasterFactory.createWritableRaster(sampleModel, new Point(0, 0));
					BufferedImage img = new BufferedImage(cm, raster, false, new java.util.Hashtable());
					List<MyTile> tiles = Lists.newArrayList(pair._2.iterator());
					List<Point> targetPoints = pointsRect.get(pair._1._2);
					for(MyTile myTile: tiles) {
						img.getRaster().setDataElements(myTile.getMinX(), myTile.getMinY(), myTile.getWidth(),
								myTile.getHeight(), myTile.getRawSamples().getElems());

					}
					WarpMetadata w1 = warpMetadataB.getValue();
					Map<String, WarpData> map = warpDataMapB.getValue();
					WarpData w = map.get(pair._1._1());
					Warp warp = new Warp(w1.getInterp(), w, w1.getInterpTable());
					// get warped image
					ImageMetadata trgImgMetadataWarp = imgMetadataB.getValue().get(pair._1._1() +"_warp"+ "_target");

					
					for (Point p : targetPoints) {
						MyTile targetTile = new MyTile(trgImgMetadataWarp.getWritableRaster(p.x, p.y),
								trgImgMetadataWarp.getRectangle(p.x, p.y), trgImgMetadataWarp.getDataType());
						warp.computeTile(targetTile, img);

						trgtiles.add(new Tuple2<Tuple2<Point, String>, MyTile>(
								new Tuple2<Point, String>(p, pair._1._1()), targetTile));
					}
					return trgtiles;
				});
		JavaPairRDD<Tuple2<Point, String>, MyTile> changeDResults=masterRastersCal.join(warpResults).mapToPair((Tuple2<Tuple2<Point,String>, Tuple2<MyTile, MyTile>> pair) -> {
			ChangeDetectionMetadata metadata=changeDMetadataB.getValue();
						ChangeDetection changeDetection = new ChangeDetection(
								metadata.isOutputLogRatio(),
								metadata.getNoDataValueN(),
								metadata.getNoDataValueD());
						ImageMetadata trgImgMetadata=imgMetadataB.getValue().get(pair._1._2+"_changeD");
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
		List<Tuple2<Tuple2<Point, String>, MyTile>> changeDRes =changeDResults.collect();
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		
		Write write = new Write(myChangeDetection.getTargetProduct(), targetFile, "BEAM-DIMAP");
		for(int i=0;i<changeDRes.size();i++){
			Band targetBand=writeOp.getTargetProduct().getBand(changeDRes.get(i)._1._2);
			write.storeTile(targetBand, changeDRes.get(i)._2);
		}
		System.out.println("total time "+totalTime);
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
		Map<Band,Band> sourceRasterMap =myWarp.getSourceRasterMap();
		for (int i = 0; i < targetProductWarp.getNumBands(); i++) {
			if (masterBandsWarp.contains(targetProductWarp.getBandAt(i).getName()))
				continue;
			targetProductWarp.getBandAt(i).setSourceImage(Utils.createSourceImages(targetProductWarp.getBandAt(i)));
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
					for (int j = 0; j < points.length; j++) {
						Rectangle rect = wimg.getTileRect(points[j].x, points[j].y);
						Rectangle rect2 = wimg.mapDestRect(rect, 0);
						if (j == 0)
							finalRect = rect2;
						else
							finalRect = finalRect.union(rect2);

					}
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
}
