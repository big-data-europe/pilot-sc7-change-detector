package tileBased;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Placemark;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.ProductNodeGroup;
import org.esa.snap.core.util.ImageUtils;
import org.esa.snap.engine_utilities.datamodel.Unit;

import scala.Tuple2;
import serialProcessingNew.LoopLimits;
import serialProcessingNew.MyChangeDetection;
import serialProcessingNew.MyCreateStack;
import serialProcessingNew.MyGCPSelection;
import serialProcessingNew.MyRead;
import serialProcessingNew.MyWarp;
import serialProcessingNew.MyWrite;
import serialProcessingNew.SerialProcessor;
import serialProcessingNew.Utils;
import serialProcessingNew.calibration.MyCalibration;
import serialProcessingNew.calibration.MySentinel1Calibrator;
import tileBased.metadata.CalibrationMetadata;
import tileBased.metadata.ChangeDetectionMetadata;
import tileBased.metadata.GCPMetadata;
import tileBased.metadata.ImageMetadata;
import tileBased.metadata.WarpMetadata;
import tileBased.model.MyTile;
import tileBased.operator.CreateStack;
import tileBased.operator.Sentinel1Calibrator;

public class SimpleCreateStack {
	public static void main(String[] args) throws Exception {
		List<MyTile> slaveRasters = null;
		List<MyTile> masterRasters = null;
		List<Tuple2<Point, Rectangle>> dependRects = new ArrayList<Tuple2<Point, Rectangle>>();
		List<Tuple2<Point, Rectangle>> sourceRects = new ArrayList<Tuple2<Point, Rectangle>>();
		// read the metadata

		String filesPath = "/home/efi/SNAP/sentinel-images/";
		File masterFile = new File(filesPath,
				"subset3_of_S1A_IW_GRDH_1SSV_20141225T142407_20141225T142436_003877_004A54_040F.dim");
		File slaveFile = new File(filesPath,
				"subset3_of_S1A_IW_GRDH_1SSV_20150518T142409_20150518T142438_005977_007B49_AF76.dim");
		File targetFile = new File(filesPath, "parallel-change");
		// read slave and master files as usual
		SerialProcessor sp = new SerialProcessor();
		File[] files = new File[4];
		files[0] = masterFile;
		files[1] = slaveFile;
		MyRead myRead1 = null;
		MyRead myRead2 = null;
		try {
			myRead1 = new MyRead(masterFile, "read1");
			masterRasters = sp.readTiles2(myRead1);
		} catch (Exception e) {
			throw e;
		}

		try {
			myRead2 = new MyRead(slaveFile, "read2");
			slaveRasters = sp.readTiles2(myRead2);
		} catch (Exception e) {
			throw e;
		}
		Boolean[] bParams1 = { false, false, false, false, true, false, false, false };
		MyCalibration myCalibration1 = new MyCalibration(null, bParams1, null);
		myCalibration1.setSourceProduct(myRead1.getTargetProduct());
		myCalibration1.setId("myCalibration1");
		sp.initOperator(myCalibration1);

		MyCalibration myCalibration2 = new MyCalibration(null, bParams1, null);
		myCalibration2.setSourceProduct(myRead2.getTargetProduct());
		myCalibration2.setId("myCalibration2");
		sp.initOperator(myCalibration2);
		
		// myRead2.getTargetProduct().getBandAt(1).setSourceImage(sp.createSourceImages(myRead2.getTargetProduct().getBandAt(1)));
		Product[] sourcesForCreateStack = new Product[2];
		sourcesForCreateStack[0] = myCalibration1.getTargetProduct();
		sourcesForCreateStack[1] = myCalibration2.getTargetProduct();

		// init all operators as usual
		String[] parameters = { "NONE", "Master", "Orbit" };
		MyCreateStack myCreateStack2 = new MyCreateStack(parameters);
		myCreateStack2.setSourceProduct(sourcesForCreateStack);
		myCreateStack2.setId("myCreateStack");
		sp.initOperator(myCreateStack2);

		boolean[] bParams = { false, false, false, false };
		int[] iParams = { 200, 10, 3 };
		double[] dParams = { 0.25, 0.6 };
		String[] sParams = { "128", "128", "4", "4", "32", "32" };
		GCPMetadata GCPMetadata = new GCPMetadata(bParams, dParams, iParams, sParams);
		MyGCPSelection gcpSelectionOp = new MyGCPSelection(bParams, dParams, iParams, sParams);
		gcpSelectionOp.setSourceProduct(myCreateStack2.getTargetProduct());
		gcpSelectionOp.setId("gcpSelection");
		sp.initOperator(gcpSelectionOp);

		boolean[] bParams2 = { false, false };
		MyWarp warpOp = new MyWarp(bParams2, 0.05f, 1, "Bilinear interpolation");
		warpOp.setSourceProduct(gcpSelectionOp.getTargetProduct());
		warpOp.setId("warp");
		sp.initOperator(warpOp);

		boolean[] bParams3 = { false, false };
		float[] fParams = { 2.0f, -2.0f };
		MyChangeDetection myChangeDetection = new MyChangeDetection(bParams3, fParams,null);
		myChangeDetection.setSourceProduct(warpOp.getTargetProduct());
		sp.initOperator(myChangeDetection);

		MyWrite writeOp = new MyWrite(myCreateStack2.getTargetProduct(), targetFile, "BEAM-DIMAP");
		writeOp.setId("write");
		sp.initOperator(writeOp);

		// calibration1 metadata.
		Product targetProductCal1 = myCalibration1.getTargetProduct();
		Product sourceProductCal1 = myCalibration1.getSourceProduct();
		Band targetBandCal1 = targetProductCal1.getBandAt(0);
		Band sourceBandCal1 = sourceProductCal1.getBandAt(0);
		ImageMetadata trgImageMetadataCal1 = new ImageMetadata(targetBandCal1.getDataType(),
				targetProductCal1.getSceneRasterWidth(), targetProductCal1.getSceneRasterHeight(),
				(float) targetBandCal1.getGeophysicalNoDataValue(), 0, 0, targetBandCal1.getGeoCoding(),targetBandCal1.getName());
		ImageMetadata srcImageMetadataCal1 = new ImageMetadata(sourceBandCal1.getDataType(),
				sourceProductCal1.getSceneRasterWidth(),
				sourceProductCal1.getSceneRasterHeight(),
				(float) sourceBandCal1.getGeophysicalNoDataValue(), 0, 0, sourceBandCal1.getGeoCoding());
		
		CalibrationMetadata calibrationMetadata1=new CalibrationMetadata(bParams1,Unit.getUnitType(targetBandCal1),Unit.getUnitType(sourceBandCal1)
				,((MySentinel1Calibrator)myCalibration1.getCalibrator()).getTargetBandToCalInfo());
		
		// calibration2 metadata.
		Product targetProductCal2 = myCalibration2.getTargetProduct();
		Product sourceProductCal2 = myCalibration2.getSourceProduct();
		Band targetBandCal2 = targetProductCal2.getBandAt(0);
		Band sourceBandCal2 = sourceProductCal2.getBandAt(0);
		ImageMetadata trgImageMetadataCal2 = new ImageMetadata(targetBandCal2.getDataType(),
				targetProductCal2.getSceneRasterWidth(), targetProductCal2.getSceneRasterHeight(),
				(float) targetBandCal2.getGeophysicalNoDataValue(), 0, 0, targetBandCal2.getGeoCoding(),targetBandCal2.getName());
		ImageMetadata srcImageMetadataCal2 = new ImageMetadata(sourceBandCal2.getDataType(),
				sourceProductCal2.getSceneRasterWidth(),
				sourceProductCal2.getSceneRasterHeight(),
				(float) sourceBandCal2.getGeophysicalNoDataValue(), 0, 0, sourceBandCal2.getGeoCoding());

		CalibrationMetadata calibrationMetadata2=new CalibrationMetadata(bParams1,Unit.getUnitType(targetBandCal2),
				Unit.getUnitType(sourceBandCal2),((MySentinel1Calibrator)myCalibration2.getCalibrator()).getTargetBandToCalInfo());
		
		// createstack metadata.
		Product targetProductStack = myCreateStack2.getTargetProduct();
		Band targetBandStack = targetProductStack.getBandAt(1);
		Band sourceBandStack = myCreateStack2.getSourceProduct()[1].getBandAt(0);
		ImageMetadata trgImageMetadataStack = new ImageMetadata(targetBandStack.getDataType(),
				targetProductStack.getSceneRasterWidth(), targetProductStack.getSceneRasterHeight(),
				(float) targetBandStack.getGeophysicalNoDataValue(), 0, 0, targetBandStack.getGeoCoding());
		ImageMetadata srcImageMetadatastack = new ImageMetadata(sourceBandStack.getDataType(),
				myCreateStack2.getSourceProduct()[1].getSceneRasterWidth(),
				myCreateStack2.getSourceProduct()[1].getSceneRasterHeight(),
				(float) sourceBandStack.getGeophysicalNoDataValue(), 0, 0, sourceBandStack.getGeoCoding());
		Map<Product, int[]> slaveOffsettMap = myCreateStack2.getSlaveOffsettMap();

		// GCP image and other metadata
		Product targetProductGCP = gcpSelectionOp.getTargetProduct();
		ImageMetadata trgImageMetadataGCP = new ImageMetadata(targetProductGCP.getBandAt(1).getDataType(),
				targetProductGCP.getSceneRasterWidth(), targetProductGCP.getSceneRasterHeight(),
				(float) targetProductGCP.getBandAt(1).getGeophysicalNoDataValue(), 0, 0,
				targetProductGCP.getBandAt(1).getGeoCoding());
		ImageMetadata srcImageMetadataGCP = new ImageMetadata(sourceBandStack.getDataType(),
				gcpSelectionOp.getSourceProduct().getSceneRasterWidth(),
				gcpSelectionOp.getSourceProduct().getSceneRasterHeight(),
				(float) gcpSelectionOp.getSourceProduct().getBandAt(1).getGeophysicalNoDataValue(), 0, 0,
				gcpSelectionOp.getSourceProduct().getBandAt(1).getGeoCoding());

		// Warp image and other metadata

		Product warpTargetProduct = warpOp.getTargetProduct();
		//warpTargetProduct.getBandAt(1).setSourceImage(sp.createSourceImages(warpOp.getTargetProduct().getBandAt(1)));

		Product warpSourceProduct = warpOp.getSourceProduct();
//		warpSourceProduct.getBandAt(1).setSourceImage(sp.createSourceImages(warpSourceProduct.getBandAt(1)));
//		warpSourceProduct.getBandAt(1).setSourceImage(sp.createSourceImages(warpOp.getSourceProduct().getBandAt(1)));
		ImageMetadata trgImageMetadataWarp = new ImageMetadata(warpTargetProduct.getBandAt(1).getDataType(),
				warpTargetProduct.getSceneRasterWidth(), warpTargetProduct.getSceneRasterHeight(),
				(float) warpTargetProduct.getBandAt(1).getGeophysicalNoDataValue(), 0, 0,
				warpTargetProduct.getBandAt(1).getGeoCoding());
		ImageMetadata srcImageMetadataWarp = new ImageMetadata(warpSourceProduct.getBandAt(1).getDataType(),
				warpSourceProduct.getSceneRasterWidth(), warpSourceProduct.getSceneRasterHeight(),
				(float) warpSourceProduct.getBandAt(1).getGeophysicalNoDataValue(), 0, 0,
				warpSourceProduct.getBandAt(1).getGeoCoding());
		WarpMetadata warpMetadata = new WarpMetadata(warpOp.getInterp(), warpOp.getInterpTable());

		// Change detection image and other metadata

		Product changeDTargetProduct = myChangeDetection.getTargetProduct();

		Product changeDSourceProduct = myChangeDetection.getSourceProduct();
		ImageMetadata trgImageMetadatachangeD = new ImageMetadata(changeDTargetProduct.getBandAt(0).getDataType(),
				changeDTargetProduct.getSceneRasterWidth(), changeDTargetProduct.getSceneRasterHeight(),
				(float) changeDTargetProduct.getBandAt(0).getGeophysicalNoDataValue(), 0, 0,
				changeDTargetProduct.getBandAt(0).getGeoCoding());
		ImageMetadata srcImageMetadatachangeD = new ImageMetadata(changeDSourceProduct.getBandAt(1).getDataType(),
				changeDSourceProduct.getSceneRasterWidth(), changeDSourceProduct.getSceneRasterHeight(),
				(float) warpSourceProduct.getBandAt(1).getGeophysicalNoDataValue(), 0, 0,
				changeDSourceProduct.getBandAt(1).getGeoCoding());
		double[] dParams2 = { changeDSourceProduct.getBandAt(0).getGeophysicalNoDataValue(),
				changeDSourceProduct.getBandAt(1).getGeophysicalNoDataValue() };
		ChangeDetectionMetadata changeDetectionMetadata = new ChangeDetectionMetadata(bParams3, dParams2);

		LoopLimits limits = new LoopLimits(targetProductStack);

		for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
			for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
				Rectangle dependRect = OperatorDependencies.createStack(trgImageMetadataStack.getRectangle(tileX, tileY), myCreateStack2.getSlaveOffsettMap().get(myCreateStack2.getSourceProduct()[1]),
						srcImageMetadatastack.getImageWidth(),
						srcImageMetadatastack.getImageHeight());
				dependRects.add(new Tuple2<Point, Rectangle>(new Point(tileX, tileY),
						dependRect));
			}
		}
		
		// init sparkConf
		SparkConf conf = new SparkConf().setMaster("local[4]").set("spark.driver.maxResultSize", "3g")
				.setAppName("First test on tile parallel processing");

		// configure spark to use Kryo serializer instead of the java
		// serializer.
		// All classes that should be serialized by kryo, are registered in
		// MyRegitration class .
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryo.registrator", "tilesParallel.MyRegistrator").set("spark.kryoserializer.buffer.max", "550");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// broadcast metadata
		Broadcast<List<Tuple2<Point, Rectangle>>> dependRectsB = sc.broadcast(dependRects);
		Broadcast<ImageMetadata> srcImgMetadataCal1Broad = sc.broadcast(srcImageMetadataCal1);
		Broadcast<ImageMetadata> trgImgMetadataCal1Broad = sc.broadcast(trgImageMetadataCal1);
		Broadcast<ImageMetadata> srcImgMetadataCal2Broad = sc.broadcast(srcImageMetadataCal2);
		Broadcast<ImageMetadata> trgImgMetadataCal2Broad = sc.broadcast(trgImageMetadataCal2);
		Broadcast<CalibrationMetadata> calMetadataBroad1 = sc.broadcast(calibrationMetadata1);
		Broadcast<CalibrationMetadata> calMetadataBroad2 = sc.broadcast(calibrationMetadata2);
		
		Broadcast<ImageMetadata> srcImgMetadataStackBroad = sc.broadcast(srcImageMetadatastack);
		Broadcast<ImageMetadata> trgImgMetadatastackBroad = sc.broadcast(trgImageMetadataStack);
		Broadcast<ImageMetadata> srcImgMetadataGCPBroad = sc.broadcast(srcImageMetadataGCP);
		Broadcast<ImageMetadata> trgImgMetadataGCPBroad = sc.broadcast(trgImageMetadataGCP);
		Broadcast<ImageMetadata> srcImgMetadataWarpBroad = sc.broadcast(srcImageMetadataWarp);
		Broadcast<ImageMetadata> trgImgMetadataWarpkBroad = sc.broadcast(trgImageMetadataWarp);
		Broadcast<ImageMetadata> trgImgMetadatachangeDBroad = sc.broadcast(trgImageMetadatachangeD);
		Broadcast<WarpMetadata> warpMetadataBroad = sc.broadcast(warpMetadata);
				
		Broadcast<ChangeDetectionMetadata> changeDetectionMetadataBroad = sc.broadcast(changeDetectionMetadata);
		Broadcast<LoopLimits> limitsBroad = sc.broadcast(limits);

		Broadcast<int[]> offset = sc
				.broadcast(myCreateStack2.getSlaveOffsettMap().get(myCreateStack2.getSourceProduct()[1]));
		Broadcast<ProductNodeGroup<Placemark>> masterGcps = sc.broadcast(gcpSelectionOp.getMasterGcpGroup());
		Broadcast<GCPMetadata> GCPMetadataBroad = sc.broadcast(GCPMetadata);
		Broadcast<Integer> rows = sc.broadcast(limits.getNumXTiles());
		
		JavaRDD<MyTile> masterRastersRdd = sc.parallelize(masterRasters);
		JavaRDD<MyTile> slaveRastersRdd = sc.parallelize(slaveRasters);
		JavaPairRDD<Point, Rectangle> masterRectRdd = sc.parallelizePairs(dependRects);

		JavaPairRDD<Point, MyTile> masterRastersCal = masterRastersRdd.mapToPair((MyTile myTile) -> {
			Point targetPoint=trgImgMetadataCal1Broad.getValue().getTileIndices(myTile.getMinX(), myTile.getMinY());
			CalibrationMetadata calMeatadata=calMetadataBroad1.getValue();
			Sentinel1Calibrator sentinel1Calibrator=new Sentinel1Calibrator(calMeatadata);
			MyTile targetTile = new MyTile(trgImgMetadataCal1Broad.getValue().getWritableRaster(targetPoint.x, targetPoint.y),
					trgImgMetadataCal1Broad.getValue().getRectangle(targetPoint.x, targetPoint.y),
					trgImgMetadataCal1Broad.getValue().getDataType());
			sentinel1Calibrator.computeTile(myTile, null, targetTile, srcImgMetadataCal1Broad.getValue().getNoDataValue(), trgImgMetadataCal1Broad.getValue().getBandName());
			return new Tuple2<Point, MyTile>(
					targetPoint, targetTile);

		});
		
		JavaPairRDD<Point, MyTile> slaveRastersCal = slaveRastersRdd.mapToPair((MyTile myTile) -> {
			Point targetPoint=trgImgMetadataCal2Broad.getValue().getTileIndices(myTile.getMinX(), myTile.getMinY());
			CalibrationMetadata calMeatadata=calMetadataBroad2.getValue();
			Sentinel1Calibrator sentinel1Calibrator=new Sentinel1Calibrator(calMeatadata);
			MyTile targetTile = new MyTile(trgImgMetadataCal2Broad.getValue().getWritableRaster(targetPoint.x, targetPoint.y),
					trgImgMetadataCal2Broad.getValue().getRectangle(targetPoint.x, targetPoint.y),
					trgImgMetadataCal2Broad.getValue().getDataType());
			sentinel1Calibrator.computeTile(myTile, null, targetTile, srcImgMetadataCal2Broad.getValue().getNoDataValue(), trgImgMetadataCal2Broad.getValue().getBandName());
			return new Tuple2<Point, MyTile>(
					targetPoint, targetTile);

		});
		
		JavaPairRDD<Tuple2<Point, Rectangle>, MyTile> dependentPairs = slaveRastersRdd
				.flatMapToPair((MyTile myTile) -> {
					List<Tuple2<Tuple2<Point, Rectangle>, MyTile>> pairs=new ArrayList<Tuple2<Tuple2<Point, Rectangle>, MyTile>>();
					List<Tuple2<Point,Rectangle>> dependRectangles=dependRectsB.getValue();
					
						for(int i=0;i<dependRectangles.size();i++)
							if(dependRectangles.get(i)._2.intersects(myTile.getRectangle())){
								pairs.add(new Tuple2<Tuple2<Point, Rectangle>, MyTile>(new Tuple2<Point, Rectangle>(dependRectangles.get(i)._1,dependRectangles.get(i)._2),myTile));
							}
					return pairs;
				});
		JavaPairRDD<Point, MyTile> createstackDepTiles = dependentPairs.groupByKey()
				.mapToPair((Tuple2<Tuple2<Point, Rectangle>, Iterable<MyTile>> pair) -> {
					Rectangle dependentRect = pair._1._2;
					Iterator<MyTile> iterator = pair._2.iterator();
					boolean first = true;
					MyTile sourceTile = null;
					Rectangle currRect = null;
					WritableRaster destinationRaster = null;
					ProductData productData = null;
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
					return new Tuple2<Point, MyTile>(pair._1._1, sourceTile);
				});

		JavaPairRDD<Point, MyTile> createstackResults = createstackDepTiles.mapToPair((Tuple2<Point, MyTile> pair) -> {

			CreateStack createStack = new CreateStack();
			// WritableRaster raster =
			// Utils.createWritableRaster(trgImgMetadatastackBroad.getValue().getRectangle(pair._1.x,
			// pair._1.y), pair._2.getType());
			MyTile targetTile = new MyTile(trgImgMetadatastackBroad.getValue().getWritableRaster(pair._1.x, pair._1.y),
					trgImgMetadatastackBroad.getValue().getRectangle(pair._1.x, pair._1.y),
					trgImgMetadatastackBroad.getValue().getDataType());
			// ProductData productData =
			// ProductData.createInstance(trgImgMetadatastackBroad.getValue().getDataType(),
			// ImageUtils.getPrimitiveArray(raster.getDataBuffer()));
			// targetTile.setRawSamples(productData);
			// trgImgMetadatastackBroad.getValue().getRectangle(pair._1.x,
			// pair._1.y);
			try {
				createStack.computeTile(targetTile, pair._2, offset.getValue(),
						srcImgMetadataStackBroad.getValue().getImageWidth(),
						srcImgMetadataStackBroad.getValue().getImageHeight(),
						trgImgMetadatastackBroad.getValue().getNoDataValue());
			} catch (ArrayIndexOutOfBoundsException e) {

				System.out.println("key: " + pair._1);
				System.out.println("");
			}
			// createStack.computeTile(targetTile, pair._2, offset.getValue(),
			// srcImgMetadataStackBroad.getValue().getImageWidth(),
			// srcImgMetadataStackBroad.getValue().getImageHeight(),trgImgMetadatastackBroad.getValue().getNoDataValue());
			// if(pair._1.x==0&&pair._1.y==0)
			// System.out.println(targetTile.getRawSamples().getElemString());

			return new Tuple2<Point, MyTile>(pair._1, targetTile);
		});
		JavaPairRDD<Integer, MyTile> createstackResultsRows = createstackResults
				.flatMapToPair((Tuple2<Point, MyTile> pair) -> {
					List<Tuple2<Integer, MyTile>> pairs = new ArrayList<Tuple2<Integer, MyTile>>();
					int rowsCount = rows.getValue();
					int nOfKeys = (int) Math.ceil((float) rowsCount / (float) 4);
					int key = 0;
					for (int i = 1; i <= nOfKeys; i++) {
						if (pair._1.y < 3 * i && pair._1.y >= 3 * (i - 1)) {
							key = i;
							pairs.add(new Tuple2<Integer, MyTile>(key, pair._2));
							if ((pair._1.y == 3 * i - 1) || (pair._1.y == 3 * i - 2))
								pairs.add(new Tuple2<Integer, MyTile>(key + 1, pair._2));
						}
					}

					return pairs;
				});

//		masterRectRdd.flatMapToPair((Tuple2<Point, Rectangle> pair) -> {
//			List<Tuple2<Integer, Point>> parts = new ArrayList<Tuple2<Integer, Point>>();
//			ProductNodeGroup<Placemark> masterGcpGroup = masterGcps.getValue();
//			final int numberOfMasterGCPs = masterGcpGroup.getNodeCount();
//
//			for (int i = 0; i < numberOfMasterGCPs; ++i) {
//				Placemark mPin = masterGcpGroup.get(i);
//				final PixelPos mGCPPixelPos = mPin.getPixelPos();
//				int x = Math.max(0, (int) mGCPPixelPos.x - 200);
//				int y = Math.max(0, (int) mGCPPixelPos.y - 200);
//				Rectangle gcpBounds = new Rectangle(x, y, 400, 400);
//				if (gcpBounds.intersects(pair._2)) {
//					parts.add(new Tuple2<Integer, Point>(i, pair._1));
//				}
//
//			}
//			return parts;
//		});
//		JavaPairRDD<Integer, MyTile> masterRastersRdd2 = masterRastersCal.flatMapToPair((Tuple2<Point, MyTile> pair) -> {
//			List<Tuple2<Integer, MyTile>> pairs = new ArrayList<Tuple2<Integer, MyTile>>();
//			int rowsCount = rows.getValue();
//			int nOfKeys = (int) Math.ceil((float) rowsCount / (float) 4);
//			int key = 0;
//			int y = trgImgMetadatastackBroad.getValue().getTileIndices(pair._2.getMinX(), pair._2.getMinY()).y;
//			for (int i = 1; i <= nOfKeys; i++) {
//				if (y < 4 * i && y >= 4 * (i - 1)) {
//					key = i;
//					pairs.add(new Tuple2<Integer, MyTile>(key, pair._2));
//					if ((y == 4 * i - 1) || (y == 4 * i - 2))
//						pairs.add(new Tuple2<Integer, MyTile>(key + 1, pair._2));
//				}
//			}
//
//			return pairs;
//
//		});
		// ClassTag<Integer> ITEGER_TAG =
		// ClassManifestFactory$.MODULE$.fromClass(Integer.class);
		// Ordering<Integer> ordering =
		// IntOrdering$.MODULE$.comparatorToOrdering(Comparator.<Integer>naturalOrder());
		// JavaPairRDD<Integer,MyTile>
		// masterRastersRddParts=masterRastersRdd2.partitionBy(new
		// RangePartitioner<Integer,MyTile>(10, masterRastersRdd2, true, null,
		// ITEGER_TAG));
//		JavaPairRDD<Integer, Iterable<MyTile>> masterRows = masterRastersRdd2.groupByKey();
//		JavaPairRDD<Integer, Iterable<MyTile>> stacktilesRows = createstackResultsRows.groupByKey();
//		List<Tuple2<Integer,Placemark>> slaveGCPs = masterRows.join(stacktilesRows)
//				.flatMap((Tuple2<Integer, Tuple2<Iterable<MyTile>, Iterable<MyTile>>> pair) -> {
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
//					List<Tuple2<Integer,Placemark>> slaveGCPsRes = new ArrayList<Tuple2<Integer,Placemark>>();
//					ProductNodeGroup<Placemark> masterGcpGroup = masterGcps.getValue();
//					final int numberOfMasterGCPs = masterGcpGroup.getNodeCount();
//					GCPMetadata GCPMetadataBroad2 = GCPMetadataBroad.getValue();
//					int[] iParams2 = { Integer.parseInt(GCPMetadataBroad2.getCoarseRegistrationWindowWidth()),
//							Integer.parseInt(GCPMetadataBroad2.getCoarseRegistrationWindowHeight()),
//							GCPMetadataBroad2.getMaxIteration(),
//							Integer.parseInt(GCPMetadataBroad2.getRowInterpFactor()),
//							Integer.parseInt(GCPMetadataBroad2.getColumnInterpFactor()),
//							srcImgMetadataGCPBroad.getValue().getImageWidth(),
//							srcImgMetadataGCPBroad.getValue().getImageHeight() };
//					double[] dParams3 = { GCPMetadataBroad2.getGcpTolerance(),
//							trgImgMetadatastackBroad.getValue().getNoDataValue(),
//							srcImgMetadataStackBroad.getValue().getNoDataValue() };
//					
//					final int[] offset2 = new int[2];
//					for (int i = 0; i < numberOfMasterGCPs; i++) {
//						final Placemark mPin = masterGcpGroup.get(i);
//
//						final PixelPos sGCPPixelPos = new PixelPos(mPin.getPixelPos().x + offset2[0],
//								mPin.getPixelPos().y + offset2[1]);
//						if (masterTile.getRectangle().contains(new Point((int) sGCPPixelPos.x, (int) sGCPPixelPos.y)))
//						{
//							GCPSelection GCPSelection = new GCPSelection(iParams2, dParams3,
//									trgImgMetadatastackBroad.getValue().getGeoCoding(), masterTile, slaveTile);
//							if (GCPSelection.checkMasterGCPValidity(mPin)
//									&& GCPSelection.checkSlaveGCPValidity(sGCPPixelPos)) {
//								Placemark sPin = GCPSelection.computeSlaveGCP(mPin, sGCPPixelPos);
//
//								if (sPin != null)
//									slaveGCPsRes.add(new Tuple2<Integer,Placemark>(i,sPin));
//							}
//
//						}
//					}
//					return slaveGCPsRes;
//				}).collect();
//
//		final ProductNodeGroup<Placemark> targetGCPGroup = GCPManager.instance()
//				.getGcpGroup(warpTargetProduct.getBandAt(1));
////
//		Map<Integer, Placemark> gcpsSet = new HashMap<Integer, Placemark>();
//		System.out.println(slaveGCPs.size());
//		for (int i = 0; i < slaveGCPs.size(); i++) {
//			gcpsSet.put(slaveGCPs.get(i)._1, slaveGCPs.get(i)._2);
//		}
//		System.out.println(gcpsSet.size());
//		for (Placemark p : gcpsSet.values()) {
//			targetGCPGroup.add(p);
//		}
//		warpOp.getWarpData();
//		Band srcBandWarp = warpOp.getSourceRasterMap().get(warpOp.getTargetProduct().getBandAt(1));
//		Band realSrcBandWarp = warpOp.getComplexSrcMap().get(srcBandWarp);
//		if (realSrcBandWarp == null) {
//			realSrcBandWarp = srcBandWarp;
//		}
//		WarpData warpData = warpOp.getWarpDataMap().get(realSrcBandWarp);
//		Broadcast<WarpData> warpDataBroad = sc.broadcast(warpData);
//		 Broadcast<RenderedImage>
//		 renderedImgBroad=sc.broadcast(warpOp.getSourceProduct().getBandAt(1).getSourceImage().getImage(0));

//		JavaPairRDD<Integer, Iterable<MyTile>> createstackResultsForWarp = createstackResults
//				.mapToPair((Tuple2<Point, MyTile> pair) -> {
//					return new Tuple2<Integer, MyTile>(1, pair._2);
//				}).groupByKey();
//
//		JavaPairRDD<Point, MyTile> warpResults = createstackResultsForWarp
//				.flatMapToPair((Tuple2<Integer, Iterable<MyTile>> pair) -> {
//					List<Tuple2<Point, MyTile>> tiles = new ArrayList<Tuple2<Point, MyTile>>();
//					int numXTiles=MathUtils.ceilInt(srcImgMetadataWarpBroad.getValue().getImageWidth() / (double) srcImgMetadataWarpBroad.getValue().getTileSize().width);
//					int numYTiles=MathUtils.ceilInt(srcImgMetadataWarpBroad.getValue().getImageHeight() / (double) srcImgMetadataWarpBroad.getValue().getTileSize().height);
////					int width = srcImgMetadataWarpBroad.getValue().getTileSize().width*numXTiles;
////					int height = srcImgMetadataWarpBroad.getValue().getTileSize().height*numYTiles;
//					int width = srcImgMetadataWarpBroad.getValue().getImageWidth();
//					int height = srcImgMetadataWarpBroad.getValue().getImageHeight();
//					int bufferType = ImageManager.getDataBufferType(srcImgMetadataWarpBroad.getValue().getDataType());
//					final SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(bufferType, width, height);
//					final ColorModel cm = PlanarImage.createColorModel(sampleModel);
//					WritableRaster raster = RasterFactory.createWritableRaster(sampleModel, new Point(0, 0));
//					BufferedImage img = new BufferedImage(cm, raster, false, new java.util.Hashtable());
//					Iterator<MyTile> it = pair._2.iterator();
//					while (it.hasNext()) {
//						MyTile myTile = it.next();
//						img.getRaster().setDataElements(myTile.getMinX(), myTile.getMinY(), myTile.getWidth(),
//								myTile.getHeight(), myTile.getDataBuffer().getElems());
//
//					}
//
//					WarpMetadata w1 = warpMetadataBroad.getValue();
//					WarpData w = warpDataBroad.getValue();
//					Warp warp = new Warp(w1.getInterp(), w, w1.getInterpTable());
//					// get warped image
//					LoopLimits lims = limitsBroad.getValue();
//					for (int tileY = 0; tileY < lims.getNumYTiles(); tileY++) {
//						for (int tileX = 0; tileX < lims.getNumXTiles(); tileX++) {
//							MyTile targetTile = new MyTile(
//									trgImgMetadataWarpkBroad.getValue().getWritableRaster(tileX, tileY),
//									trgImgMetadataWarpkBroad.getValue().getRectangle(tileX, tileY),
//									trgImgMetadataWarpkBroad.getValue().getDataType());
//							warp.computeTile(targetTile, img);
//							tiles.add(new Tuple2<Point, MyTile>(new Point(tileX, tileY), targetTile));
//						}
//					}
//
//					return tiles;
//				});
//		
//		JavaPairRDD<Point, MyTile> changeDResults=masterRastersCal.join(warpResults).mapToPair((Tuple2<Point, Tuple2<MyTile, MyTile>> pair) -> {
//
//			ChangeDetection changeDetection = new ChangeDetection(
//					changeDetectionMetadataBroad.getValue().isOutputLogRatio(),
//					changeDetectionMetadataBroad.getValue().getNoDataValueN(),
//					changeDetectionMetadataBroad.getValue().getNoDataValueD());
//			// WritableRaster raster =
//			// Utils.createWritableRaster(trgImgMetadatastackBroad.getValue().getRectangle(pair._1.x,
//			// pair._1.y), pair._2.getType());
//			MyTile targetTile = new MyTile(trgImgMetadatachangeDBroad.getValue().getWritableRaster(pair._1.x, pair._1.y),
//					trgImgMetadatachangeDBroad.getValue().getRectangle(pair._1.x, pair._1.y),
//					trgImgMetadatachangeDBroad.getValue().getDataType());
//			//MyTile denominatorTile=Utils.getSubTile(pair._2._2,targetTile.getRectangle());
//			//MyTile nominatorTile=Utils.getSubTile(pair._2._1,targetTile.getRectangle());
//			try {
//				changeDetection.computeTile(pair._2._1, pair._2._2, targetTile, targetTile.getRectangle());
//			} catch (ArrayIndexOutOfBoundsException e) {
//
//				System.out.println("key: " + pair._1);
//				System.out.println("");
//			}
//			// createStack.computeTile(targetTile, pair._2, offset.getValue(),
//			// srcImgMetadataStackBroad.getValue().getImageWidth(),
//			// srcImgMetadataStackBroad.getValue().getImageHeight(),trgImgMetadatastackBroad.getValue().getNoDataValue());
//			// if(pair._1.x==0&&pair._1.y==0)
//			// System.out.println(targetTile.getRawSamples().getElemString());
//
//			return new Tuple2<Point, MyTile>(pair._1, targetTile);
//		});
        //File file2 = new File(filesPath, "serial-original-chd.dim");
//		File file2 = new File(filesPath, "serial-original-warp.dim");
//        Map<Point, ProductData> tilesMap = new HashMap<Point, ProductData>();
//        MyRead myRead = null;
//        try {
//            myRead = new MyRead(file2, "read");
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//        Product targetProduct2 = myRead.getTargetProduct();
//        int noOfProducts = targetProduct2.getNumBands();
//        for (int i = 0; i < noOfProducts; i++) {
//        	System.out.println("band "+i);
//            Band band1 = targetProduct2.getBandAt(i);
//            if (band1.getClass() == Band.class) {
//                for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
//                    for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
//                        GetTile getTile1 = new GetTile(band1, myRead);
//                        Tile tile1 = getTile1.computeTile(tileX, tileY);
//                        tilesMap.put(new Point(tileX,tileY), tile1.getDataBuffer());
//                    }
//                }
//            }
//        }
		try {
			myRead1 = new MyRead(masterFile, "read1");
			sp.getBufferedImage(myRead1);
		} catch (Exception e) {
			throw e;
		}
		try {
			myRead2 = new MyRead(slaveFile, "read2");
			sp.getBufferedImage(myRead2);
		} catch (Exception e) {
			throw e;
		}
		
		MyCalibration myCalibration11 = new MyCalibration(null, bParams1, null);
		myCalibration11.setSourceProduct(myRead1.getTargetProduct());
		myCalibration11.setId("myCalibration11");
		sp.initOperatorForMultipleBands(myCalibration11);

		MyCalibration myCalibration22 = new MyCalibration(null, bParams1, null);
		myCalibration22.setSourceProduct(myRead2.getTargetProduct());
		myCalibration22.setId("myCalibration2");
		sp.initOperatorForMultipleBands(myCalibration22);
		sp.processTiles(myCalibration11);
		sp.processTiles(myCalibration22);
		
		Product[] sourcesForCreateStack2 = new Product[2];
		sourcesForCreateStack2[0] = myCalibration11.getTargetProduct();
		sourcesForCreateStack2[1] = myCalibration22.getTargetProduct();
		
		String[] parameters2 = { "NONE", "Master", "Orbit" };
		MyCreateStack myCreateStack22 = new MyCreateStack(parameters2);
		myCreateStack22.setSourceProduct(sourcesForCreateStack2);
		myCreateStack22.setId("myCreateStack");
		//sp.initOperatorForMultipleBands(myCreateStack22);
//
//		MyGCPSelection gcpSelectionOp2 = new MyGCPSelection(bParams, dParams, iParams, sParams);
//		gcpSelectionOp2.setSourceProduct(myCreateStack22.getTargetProduct());
//		gcpSelectionOp2.setId("gcpSelection");
//		sp.initOperator(gcpSelectionOp2, myCreateStack22);
//
//		boolean[] bParams4 = { false, false };
//		MyWarp warpOp2 = new MyWarp(bParams4, 0.05f, 1, "Bilinear interpolation");
//		warpOp2.setSourceProduct(gcpSelectionOp2.getTargetProduct());
//		warpOp2.setId("warp");
//		sp.initOperator(warpOp2, gcpSelectionOp2);
//		
//		MyChangeDetection myChangeDetection2 = new MyChangeDetection(bParams3, fParams);
//		myChangeDetection2.setSourceProduct(warpOp2.getTargetProduct());
//		sp.initOperator(myChangeDetection2, warpOp2);
//
//		sp.processTiles(myCreateStack22, false);
//		sp.processTiles(gcpSelectionOp2, false);
//		sp.processTiles(warpOp2, false);

		// Product targetProduct = writeOp.getTargetProduct();
		// Tuple2<ProductNodeGroup<Placemark>,ProductNodeGroup<Placemark>> GCPGroups =SerialCoregistration.testGCPs();
////		 GCPManager.instance().getGcpGroup(warpOp2.getSourceProduct().getBandAt(1));
////		
////		
//		System.out.println("correct gcps"+GCPGroups._1.getNodeCount());
//		 for(int i=0;i<GCPGroups._1.getNodeCount();i++){
//		 if(gcpsSet.containsKey(GCPGroups._1.get(i).getPixelPos()))
//		 gcpsSet.remove(GCPGroups._1.get(i).getPixelPos());
//		 else
//		 System.out.println(GCPGroups._1.get(i).getPixelPos());
//		 }
//		 System.out.println("after del "+gcpsSet.size());
		 
//		 Map<PixelPos, Placemark> gcpsMasterSet = new HashMap<PixelPos, Placemark>();
//		 for (int i = 0; i < masterGcps.getValue().getNodeCount(); i++) {
//			 gcpsMasterSet.put( masterGcps.getValue().get(i).getPixelPos(),masterGcps.getValue().get(i));
//			}
//		 System.out.println("correct gcps"+GCPGroups._2.getNodeCount());
//		 for(int i=0;i<GCPGroups._2.getNodeCount();i++){
//		 if(gcpsMasterSet.containsKey(GCPGroups._2.get(i).getPixelPos()))
//			 gcpsMasterSet.remove(GCPGroups._2.get(i).getPixelPos());
//		 else
//		 System.out.println(GCPGroups._2.get(i).getPixelPos());
//		 }
//		 System.out.println("after del "+gcpsMasterSet.size());

//		List<Tuple2<Point, Tile>> serialwarpRes = sp.processTiles(myChangeDetection2);
////		List<Tuple2<Point, Tile>> serialcal1Res = sp.processTiles(myCalibration11);
////		List<Tuple2<Point, Tile>> serialcal2Res = sp.processTiles(myCalibration22);
////		List<Tuple2<Point, Tile>> serialstackRes = sp.processTiles(myCreateStack22);
        List<Tuple2<Point, MyTile>> res = createstackResults.collect();
//		
		Map<Point, ProductData> tilesMap = new HashMap<Point, ProductData>();
		
		for (int i = 0; i < res.size(); i++) {
			tilesMap.put(res.get(i)._1, res.get(i)._2.getDataBuffer());
		}
		System.out.println(tilesMap.size());
		
		for (Tuple2<Point, MyTile> tuple : res) {
			ProductData obj = tilesMap.get(tuple._1);
			if (obj.equalElems(tuple._2.getDataBuffer())) {
				tilesMap.remove(tuple._1);
			} else {
				System.out.println("point " + tuple._1 + " rect " + tuple._2.getRectangle());
//				float[] arr1=(float[])obj.getElems();
//				float[] arr2=tuple._2.getDataBufferFloat();
//				for(int i=0;i<arr1.length;i++){
//					
//					if(arr1[i]!=arr2[i])
//						System.out.println("index "+i);
//					
//				}
//				break;
				}
		}
		
		System.out.println(tilesMap.size());
	}

}
