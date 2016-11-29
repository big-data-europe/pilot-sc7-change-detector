package tileBased;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.esa.s1tbx.insar.gpf.coregistration.GCPManager;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Placemark;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.ProductNodeGroup;
import org.esa.snap.core.gpf.Tile;
import org.esa.snap.engine_utilities.gpf.StackUtils;

import scala.Tuple2;
import scala.Tuple3;
import serialProcessingNew.GetTile;
import serialProcessingNew.LoopLimits;
import serialProcessingNew.MyBandSelect;
import serialProcessingNew.MyChangeDetection;
import serialProcessingNew.MyCreateStack;
import serialProcessingNew.MyGCPSelection;
import serialProcessingNew.MyRead;
import serialProcessingNew.MyWarp;
import serialProcessingNew.SerialProcessor;
import serialProcessingNew.calibration.MyCalibration;
import tileBased.model.MyTile;

//this class is temporal and it is used only to compare the computed tiles with the tiles from the serial processing.
public class Checks {

	public static boolean checkCalibration(List<Tuple2<Tuple2<Point, String>, MyTile>> masterCalres,
			List<Tuple2<Tuple2<Point, String>, MyTile>> slaveCalres) throws Exception {

		Map<String, Map<Point, MyTile>> bandsMap1 = new HashMap<String, Map<Point, MyTile>>();
		Map<String, Map<Point, MyTile>> bandsMap2 = new HashMap<String, Map<Point, MyTile>>();
		for (int i = 0; i < masterCalres.size(); i++) {
			String bandName = masterCalres.get(i)._1._2;
			if (bandsMap1.containsKey(bandName)) {
				bandsMap1.get(bandName).put(masterCalres.get(i)._1._1, masterCalres.get(i)._2);
			} else {
				Map<Point, MyTile> tilesMap = new HashMap<Point, MyTile>();
				tilesMap.put(masterCalres.get(i)._1._1, masterCalres.get(i)._2);
				bandsMap1.put(bandName, tilesMap);
			}
		}
		for (int i = 0; i < slaveCalres.size(); i++) {
			String bandName = slaveCalres.get(i)._1._2;
			if (bandsMap2.containsKey(bandName)) {
				bandsMap2.get(bandName).put(slaveCalres.get(i)._1._1, slaveCalres.get(i)._2);
			} else {
				Map<Point, MyTile> tilesMap = new HashMap<Point, MyTile>();
				tilesMap.put(slaveCalres.get(i)._1._1, slaveCalres.get(i)._2);
				bandsMap2.put(bandName, tilesMap);
			}
		}

		String filesPath = "/home/efi/SNAP/sentinel-images/";
		File masterFile = new File(filesPath,
				"subset_0_of_S1A_IW_GRDH_1SDV_20151110T145915_20151110T145940_008544_00C1A6_F175.dim");
		File slaveFile = new File(filesPath,
				"subset_1_of_S1A_IW_GRDH_1SDV_20151029T145915_20151029T145940_008369_00BD0B_334C.dim");
		SerialProcessor sp = new SerialProcessor();
		String[] selectedPolarisations = { "VH" };
		MyRead myRead1 = new MyRead(masterFile, "read1");
		sp.getBufferedImage(myRead1, selectedPolarisations);

		MyBandSelect bandselect1 = new MyBandSelect(selectedPolarisations, null);
		bandselect1.setSourceProduct(myRead1.getTargetProduct());
		sp.initOperatorForMultipleBands(bandselect1);

		MyRead myRead2 = new MyRead(slaveFile, "read2");
		sp.getBufferedImage(myRead2, selectedPolarisations);

		MyBandSelect bandselect2 = new MyBandSelect(selectedPolarisations, null);
		bandselect2.setSourceProduct(myRead2.getTargetProduct());
		sp.initOperatorForMultipleBands(bandselect2);

		Boolean[] bParams1 = { false, false, false, false, true, false, false, false };
		MyCalibration myCalibration1 = new MyCalibration(null, bParams1, null);
		myCalibration1.setSourceProduct(bandselect1.getTargetProduct());
		myCalibration1.setId("myCalibration1");
		sp.initOperatorForMultipleBands(myCalibration1);

		MyCalibration myCalibration2 = new MyCalibration(null, bParams1, null);
		myCalibration2.setSourceProduct(bandselect2.getTargetProduct());
		myCalibration2.setId("myCalibration2");
		sp.initOperatorForMultipleBands(myCalibration2);

		Map<String, List<Tuple2<Point, Tile>>> cal1Res = sp.processTiles2(myCalibration1);
		Map<String, List<Tuple2<Point, Tile>>> cal2Res = sp.processTiles2(myCalibration2);

		boolean resultsOk = true;
		;
		String[] names1 = myCalibration1.getTargetProduct().getBandNames();
		for (int j = 0; j < names1.length; j++) {
			Map<Point, MyTile> tilesMap = bandsMap1.get(names1[j]);
			List<Tuple2<Point, Tile>> calTiles = cal1Res.get(names1[j]);

			System.out.println("init size for band " + names1[j] + " " + tilesMap.size());
			for (int i = 0; i < calTiles.size(); i++) {
				ProductData obj = tilesMap.get(calTiles.get(i)._1).getDataBuffer();
				if (obj.equalElems(calTiles.get(i)._2.getDataBuffer())) {
					tilesMap.remove(calTiles.get(i)._1);
				} else {
					// System.out.println("point " + calTiles.get(i)._1 + " rect
					// " + calTiles.get(i)._2.getRectangle());
				}
			}
			if (tilesMap.size() != 0)
				resultsOk = false;
			System.out.println("final size for band " + names1[j] + " " + tilesMap.size());

		}

		String[] names2 = myCalibration2.getTargetProduct().getBandNames();
		for (int j = 0; j < names2.length; j++) {
			Map<Point, MyTile> tilesMap = bandsMap2.get(names2[j]);
			List<Tuple2<Point, Tile>> calTiles = cal2Res.get(names2[j]);

			System.out.println("init size for band " + names2[j] + " " + tilesMap.size());
			for (int i = 0; i < calTiles.size(); i++) {
				ProductData obj = tilesMap.get(calTiles.get(i)._1).getDataBuffer();
				if (obj.equalElems(calTiles.get(i)._2.getDataBuffer())) {
					tilesMap.remove(calTiles.get(i)._1);
				} else {
					// System.out.println("point " + calTiles.get(i)._1 + " rect
					// " + calTiles.get(i)._2.getRectangle());
				}
			}

			System.out.println("final size for band " + names2[j] + " " + tilesMap.size());
			if (tilesMap.size() != 0)
				resultsOk = false;
		}
		myCalibration1 = null;
		myCalibration2 = null;
		return resultsOk;
	}

	public static boolean checkDepends(List<Tuple2<Tuple2<Point, String>, MyTile>> createStackRes)
			throws Exception {
		Map<String, Map<Point, MyTile>> bandsMap = new HashMap<String, Map<Point, MyTile>>();
		for (int i = 0; i < createStackRes.size(); i++) {
			String bandName = createStackRes.get(i)._1._2();
			if (bandsMap.containsKey(bandName)) {
				bandsMap.get(bandName).put(createStackRes.get(i)._1._1(), createStackRes.get(i)._2);
			} else {
				Map<Point, MyTile> tilesMap = new HashMap<Point, MyTile>();
				tilesMap.put(createStackRes.get(i)._1._1(), createStackRes.get(i)._2);
				bandsMap.put(bandName, tilesMap);
			}
		}
		String filesPath = "/home/efi/SNAP/sentinel-images/";
		File slaveFile = new File(filesPath,
				 "subset_1_of_S1A_IW_GRDH_1SSV_20150518T142409_20150518T142438_005977_007B49_AF76.dim");
File masterFile = new File(filesPath,
						 "subset_0_of_S1A_IW_GRDH_1SSV_20141225T142407_20141225T142436_003877_004A54_040F.dim");	 

//		File masterFile = new File(filesPath,
//				"subset_0_of_S1A_IW_GRDH_1SDV_20151110T145915_20151110T145940_008544_00C1A6_F175.dim");
//		File slaveFile = new File(filesPath,
//				"subset_1_of_S1A_IW_GRDH_1SDV_20151029T145915_20151029T145940_008369_00BD0B_334C.dim");
		SerialProcessor sp = new SerialProcessor();
		//String[] selectedPolarisations = { "VH" };
		String[] selectedPolarisations = null;
		MyRead myRead1 = new MyRead(masterFile, "read1");
		sp.getBufferedImage(myRead1, selectedPolarisations);

		MyBandSelect bandselect1 = new MyBandSelect(selectedPolarisations, null);
		bandselect1.setSourceProduct(myRead1.getTargetProduct());
		sp.initOperatorForMultipleBands(bandselect1);

		MyRead myRead2 = new MyRead(slaveFile, "read2");
		sp.getBufferedImage(myRead2, selectedPolarisations);

		MyBandSelect bandselect2 = new MyBandSelect(selectedPolarisations, null);
		bandselect2.setSourceProduct(myRead2.getTargetProduct());
		sp.initOperatorForMultipleBands(bandselect2);

		Boolean[] bParams1 = { false, false, false, false, true, false, false, false };
		MyCalibration myCalibration1 = new MyCalibration(null, bParams1, null);
		myCalibration1.setSourceProduct(bandselect1.getTargetProduct());
		myCalibration1.setId("myCalibration1");
		sp.initOperatorForMultipleBands(myCalibration1);
		myRead1 = null;
		sp.processTiles(myCalibration1);

		MyCalibration myCalibration2 = new MyCalibration(null, bParams1, null);
		myCalibration2.setSourceProduct(bandselect2.getTargetProduct());
		myCalibration2.setId("myCalibration2");
		sp.initOperatorForMultipleBands(myCalibration2);
		myRead2 = null;
		sp.processTiles(myCalibration2);

		Product[] sourcesForCreateStack = new Product[2];
		sourcesForCreateStack[0] = myCalibration1.getTargetProduct();
		sourcesForCreateStack[1] = myCalibration2.getTargetProduct();

		String[] parameters = { "NONE", "Master", "Orbit" };
		MyCreateStack myCreateStack = new MyCreateStack(parameters);
		myCreateStack.setSourceProduct(sourcesForCreateStack);
		myCreateStack.setId("myCreateStack");
		sp.initOperatorForMultipleBands(myCreateStack);
		myRead1 = null;
		myRead2 = null;

		Map<String, List<Tuple2<Point, Raster>>> stackRes = new HashMap<String, List<Tuple2<Point, Raster>>>();
		Product targetProductStack = myCreateStack.getTargetProduct();
		LoopLimits limits = new LoopLimits(targetProductStack);
		String[] masterBandNames = StackUtils.getMasterBandNames(targetProductStack);
		Set<String> masterBands = new HashSet(Arrays.asList(masterBandNames));

		for (int i = 0; i < targetProductStack.getNumBands(); i++) {
			if (masterBands.contains(targetProductStack.getBandAt(i).getName()))
				continue;
			for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
				for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {

					GetTile getTile = new GetTile(targetProductStack.getBandAt(i), myCreateStack);
					Rectangle rect = OperatorDependencies.createStack(getTile.getRectangle(tileX, tileY),
							myCreateStack.getSlaveOffsettMap()
									.get(myCreateStack.getSourceRasterMap().get(targetProductStack.getBandAt(i))
											.getProduct()),
							myCreateStack.getSourceRasterMap().get(targetProductStack.getBandAt(i)).getProduct()
									.getSceneRasterWidth(),
							myCreateStack.getSourceRasterMap().get(targetProductStack.getBandAt(i)).getProduct()
									.getSceneRasterHeight());
					Raster raster = targetProductStack.getBandAt(i).getSourceImage().getImage(0).getData(rect);
					if (stackRes.containsKey(targetProductStack.getBandAt(i).getName())) {
						stackRes.get(targetProductStack.getBandAt(i).getName())
								.add(new Tuple2<Point, Raster>(new Point(tileX, tileY), raster));
					} else {
						List<Tuple2<Point, Raster>> tilesMap = new ArrayList<Tuple2<Point, Raster>>();
						tilesMap.add(new Tuple2<Point, Raster>(new Point(tileX, tileY), raster));
						stackRes.put(targetProductStack.getBandAt(i).getName(), tilesMap);
					}
				}
			}
		}

		String[] names1 = myCreateStack.getTargetProduct().getBandNames();

		boolean resultsOk = true;
		for (int j = 0; j < names1.length; j++) {
			if (masterBands.contains(names1[j]))
				continue;
			Map<Point, MyTile> tilesMap = bandsMap.get(names1[j]);
			List<Tuple2<Point, Raster>> calTiles = stackRes.get(names1[j]);

			System.out.println("init size for band " + names1[j] + " " + tilesMap.size());
			for (int i = 0; i < calTiles.size(); i++) {
				ProductData obj = tilesMap.get(calTiles.get(i)._1).getDataBuffer();
				Object obj2 = obj.getElems();
				DataBuffer d = calTiles.get(i)._2.getDataBuffer();
				boolean eq = true;
				for (int k = 0; k < ((float[]) obj2).length; k++) {
					if (d.getElemFloat(k) != ((float[]) obj2)[k]) {
						eq = false;
						break;
					}
				}
				if (eq) {
					tilesMap.remove(calTiles.get(i)._1);
				} else {
					System.out.println("point " + calTiles.get(i)._1);
				}
			}
			System.out.println("final size for band " + names1[j] + " " + tilesMap.size());
			if (tilesMap.size() != 0)
				resultsOk = false;
		}

		myCalibration1 = null;
		myCalibration2 = null;
		return resultsOk;
	}

	public static void checkCreatestack(List<Tuple2<Tuple2<Point, String>, MyTile>> createStackRes) throws Exception {
		Map<String, Map<Point, MyTile>> bandsMap = new HashMap<String, Map<Point, MyTile>>();
		for (int i = 0; i < createStackRes.size(); i++) {
			String bandName = createStackRes.get(i)._1._2;
			if (bandsMap.containsKey(bandName)) {
				bandsMap.get(bandName).put(createStackRes.get(i)._1._1, createStackRes.get(i)._2);
			} else {
				Map<Point, MyTile> tilesMap = new HashMap<Point, MyTile>();
				tilesMap.put(createStackRes.get(i)._1._1, createStackRes.get(i)._2);
				bandsMap.put(bandName, tilesMap);
			}
		}
		// String
		// masterFile="/home/hadoop/S1A_IW_GRDH_1SSV_20150518T142409_20150518T142438_005977_007B49_AF76.zip";
		// String slaveFile =
		// "/home/hadoop/S1A_IW_GRDH_1SSV_20150518T142409_20150518T142438_005977_007B49_AF76.zip";
		String filesPath = "/home/efi/SNAP/sentinel-images/";
//		File masterFile = new File(filesPath,
//				"subset_0_of_S1A_IW_GRDH_1SDV_20151110T145915_20151110T145940_008544_00C1A6_F175.dim");
//		File slaveFile = new File(filesPath,
//				"subset_1_of_S1A_IW_GRDH_1SDV_20151029T145915_20151029T145940_008369_00BD0B_334C.dim");
		File slaveFile = new File(filesPath,
				 "subset_1_of_S1A_IW_GRDH_1SSV_20150518T142409_20150518T142438_005977_007B49_AF76.dim");
		File masterFile = new File(filesPath,
						 "subset_0_of_S1A_IW_GRDH_1SSV_20141225T142407_20141225T142436_003877_004A54_040F.dim");	 
		SerialProcessor sp = new SerialProcessor();
//		String[] selectedPolarisations = { "VH" };
		String[] selectedPolarisations = null;
		MyRead myRead1 = new MyRead(masterFile, "read1");
		sp.getBufferedImage(myRead1, selectedPolarisations);

		MyBandSelect bandselect1 = new MyBandSelect(selectedPolarisations, null);
		bandselect1.setSourceProduct(myRead1.getTargetProduct());
		sp.initOperatorForMultipleBands(bandselect1);

		MyRead myRead2 = new MyRead(slaveFile, "read2");
		sp.getBufferedImage(myRead2, selectedPolarisations);

		MyBandSelect bandselect2 = new MyBandSelect(selectedPolarisations, null);
		bandselect2.setSourceProduct(myRead2.getTargetProduct());
		sp.initOperatorForMultipleBands(bandselect2);

		Boolean[] bParams1 = { false, false, false, false, true, false, false, false };
		MyCalibration myCalibration1 = new MyCalibration(null, bParams1, null);
		myCalibration1.setSourceProduct(bandselect1.getTargetProduct());
		myCalibration1.setId("myCalibration1");
		sp.initOperatorForMultipleBands(myCalibration1);
		myRead1 = null;
		sp.processTiles(myCalibration1);

		MyCalibration myCalibration2 = new MyCalibration(null, bParams1, null);
		myCalibration2.setSourceProduct(bandselect2.getTargetProduct());
		myCalibration2.setId("myCalibration2");
		sp.initOperatorForMultipleBands(myCalibration2);
		myRead2 = null;
		sp.processTiles(myCalibration2);

		Product[] sourcesForCreateStack = new Product[2];
		sourcesForCreateStack[0] = myCalibration1.getTargetProduct();
		sourcesForCreateStack[1] = myCalibration2.getTargetProduct();

		String[] parameters = { "NONE", "Master", "Orbit" };
		MyCreateStack myCreateStack = new MyCreateStack(parameters);
		myCreateStack.setSourceProduct(sourcesForCreateStack);
		myCreateStack.setId("myCreateStack");
		sp.initOperatorForMultipleBands(myCreateStack);
		myRead1 = null;
		myRead2 = null;

		Map<String, List<Tuple2<Point, Tile>>> stackRes = sp.processTiles2(myCreateStack);

		String[] names1 = myCreateStack.getTargetProduct().getBandNames();

		String[] masterBandNames = StackUtils.getMasterBandNames(myCreateStack.getTargetProduct());
		Set<String> masterBands = new HashSet(Arrays.asList(masterBandNames));
		for (int j = 0; j < names1.length; j++) {
			if (masterBands.contains(names1[j]))
				continue;
			Map<Point, MyTile> tilesMap = bandsMap.get(names1[j]);
			List<Tuple2<Point, Tile>> calTiles = stackRes.get(names1[j]);

			System.out.println("init size for band " + names1[j] + " " + tilesMap.size());
			for (int i = 0; i < calTiles.size(); i++) {
				ProductData obj = tilesMap.get(calTiles.get(i)._1).getDataBuffer();
				if (obj.equalElems(calTiles.get(i)._2.getDataBuffer())) {
					tilesMap.remove(calTiles.get(i)._1);
				} else {
					System.out.println("point " + calTiles.get(i)._1 + " rect " + calTiles.get(i)._2.getRectangle());
					// float[] arr1=(float[])obj.getElems();
					// float[] arr2=calTiles.get(i)._2.getDataBufferFloat();
					// for(int k=0;k<arr1.length;k++){
					//
					// if(arr1[k]!=arr2[k])
					// System.out.println("index "+k);
					//
					// }
					// break;
				}
			}

			System.out.println("final size for band " + names1[j] + " " + tilesMap.size());
		}
		myCalibration1 = null;
		myCalibration2 = null;
		myCreateStack = null;
	}

	public static void checkGCPs(Map<String, List<Placemark>> gcpsMap, ProductNodeGroup<Placemark> masterGCPs)
			throws IOException {
		System.out.println("check starts");
		Map<PixelPos, Placemark> gcpsMasterSet = new HashMap<PixelPos, Placemark>();
		for (int i = 0; i < masterGCPs.getNodeCount(); i++) {
			gcpsMasterSet.put(masterGCPs.get(i).getPixelPos(), masterGCPs.get(i));
		}

		Map<String, Map<PixelPos, Placemark>> gcpPosMap = new HashMap<String, Map<PixelPos, Placemark>>();
		for (String name : gcpsMap.keySet()) {
			List<Placemark> map = gcpsMap.get(name);
			Map<PixelPos, Placemark> pmap = new HashMap<PixelPos, Placemark>();
			for (Placemark entry : map)
				pmap.put(entry.getPixelPos(), entry);
			gcpPosMap.put(name, pmap);
		}

		String filesPath = "/home/efi/SNAP/sentinel-images/";
		// File masterFile = new File(filesPath,
		// "subset_0_of_S1A_IW_GRDH_1SDV_20151110T145915_20151110T145940_008544_00C1A6_F175.dim");
		// File slaveFile = new File(filesPath,
		// "subset_1_of_S1A_IW_GRDH_1SDV_20151029T145915_20151029T145940_008369_00BD0B_334C.dim");
		File masterFile = new File(filesPath,
				"subset3_of_S1A_IW_GRDH_1SSV_20141225T142407_20141225T142436_003877_004A54_040F.dim");
		File slaveFile = new File(filesPath,
				"subset3_of_S1A_IW_GRDH_1SSV_20150518T142409_20150518T142438_005977_007B49_AF76.dim");
		SerialProcessor sp = new SerialProcessor();
		String[] selectedPolarisations = { "VH" };
		MyRead myRead1 = new MyRead(masterFile, "read1");
		sp.getBufferedImage(myRead1, selectedPolarisations);

		MyBandSelect bandselect1 = new MyBandSelect(selectedPolarisations, null);
		bandselect1.setSourceProduct(myRead1.getTargetProduct());
		sp.initOperatorForMultipleBands(bandselect1);

		MyRead myRead2 = new MyRead(slaveFile, "read2");
		sp.getBufferedImage(myRead2, selectedPolarisations);

		MyBandSelect bandselect2 = new MyBandSelect(selectedPolarisations, null);
		bandselect2.setSourceProduct(myRead2.getTargetProduct());
		sp.initOperatorForMultipleBands(bandselect2);

		Boolean[] bParams1 = { false, false, false, false, true, false, false, false };
		MyCalibration myCalibration1 = new MyCalibration(null, bParams1, null);
		myCalibration1.setSourceProduct(bandselect1.getTargetProduct());
		myCalibration1.setId("myCalibration1");
		sp.initOperatorForMultipleBands(myCalibration1);
		myRead1 = null;
		sp.processTiles(myCalibration1);

		MyCalibration myCalibration2 = new MyCalibration(null, bParams1, null);
		myCalibration2.setSourceProduct(bandselect2.getTargetProduct());
		myCalibration2.setId("myCalibration2");
		sp.initOperatorForMultipleBands(myCalibration2);
		myRead2 = null;
		sp.processTiles(myCalibration2);

		Product[] sourcesForCreateStack = new Product[2];
		sourcesForCreateStack[0] = myCalibration1.getTargetProduct();
		sourcesForCreateStack[1] = myCalibration2.getTargetProduct();

		String[] parameters = { "NONE", "Master", "Orbit" };
		MyCreateStack myCreateStack = new MyCreateStack(parameters);
		myCreateStack.setSourceProduct(sourcesForCreateStack);
		myCreateStack.setId("myCreateStack");
		sp.initOperatorForMultipleBands(myCreateStack);
		sp.processTiles(myCreateStack);
		myRead1 = null;
		myRead2 = null;

		boolean[] bParams = { false, false, false, false };
		int[] iParams = { 1000, 10, 3 };
		double[] dParams = { 0.25, 0.6 };
		String[] sParams = { "128", "128", "4", "4", "32", "32" };
		MyGCPSelection gcpSelectionOp = new MyGCPSelection(bParams, dParams, iParams, sParams);
		gcpSelectionOp.setSourceProduct(myCreateStack.getTargetProduct());
		gcpSelectionOp.setId("gcpSelection");
		sp.initOperatorForMultipleBands(gcpSelectionOp);
		myCreateStack = null;
		sp.processTiles(gcpSelectionOp);

		ProductNodeGroup<Placemark> correctMasterGCPs = gcpSelectionOp.getMasterGcpGroup();
		System.out.println("init size for gcps of master " + gcpsMasterSet.size());
		for (int i = 0; i < correctMasterGCPs.getNodeCount(); i++) {
			if (gcpsMasterSet.containsKey(correctMasterGCPs.get(i).getPixelPos())) {
				// System.out.println(correctMasterGCPs.get(i).getPixelPos());
				gcpsMasterSet.remove(correctMasterGCPs.get(i).getPixelPos());
			} else
				System.out.println(gcpsMasterSet.get(i).getPixelPos());
		}
		System.out.println("final size for gcps of master " + gcpsMasterSet.size());
		Product targetProductGCP = gcpSelectionOp.getTargetProduct();
		ProductNodeGroup<Placemark> correctGCPs = null;
		for (int i = 0; i < targetProductGCP.getNumBands(); i++) {

			correctGCPs = GCPManager.instance().getGcpGroup(targetProductGCP.getBandAt(i));
			Map<PixelPos, Placemark> pmap = gcpPosMap.get(targetProductGCP.getBandAt(i).getName());

			if (pmap == null)
				continue;
			System.out.println(
					"init size for gcps of band " + targetProductGCP.getBandAt(i).getName() + " " + pmap.size());
			System.out.println("size for correct gcps of band " + targetProductGCP.getBandAt(i).getName() + " "
					+ correctGCPs.getNodeCount());
			for (int j = 0; j < correctGCPs.getNodeCount(); j++) {
				if (pmap.containsKey(correctGCPs.get(j).getPixelPos()))
					pmap.remove(correctGCPs.get(j).getPixelPos());
				else
					System.out.println(correctGCPs.get(j).getPixelPos());

			}
			// System.out.println("");
			// System.out.println("");
			// for(PixelPos pos:pmap.keySet()){
			// System.out.println(pos);
			//
			// }
			System.out.println(
					"final size for gcps of band " + targetProductGCP.getBandAt(i).getName() + " " + pmap.size());
		}
	}

	public static void checkGCPs2(Map<String, Map<Integer, Placemark>> gcpsMap, ProductNodeGroup<Placemark> masterGCPs)
			throws IOException {
		System.out.println("check starts");
		Map<PixelPos, Placemark> gcpsMasterSet = new HashMap<PixelPos, Placemark>();
		for (int i = 0; i < masterGCPs.getNodeCount(); i++) {
			gcpsMasterSet.put(masterGCPs.get(i).getPixelPos(), masterGCPs.get(i));
		}

		Map<String, Map<PixelPos, Placemark>> gcpPosMap = new HashMap<String, Map<PixelPos, Placemark>>();
		for (String name : gcpsMap.keySet()) {
			Map<Integer, Placemark> map = gcpsMap.get(name);
			Map<PixelPos, Placemark> pmap = new HashMap<PixelPos, Placemark>();
			for (Placemark entry : map.values())
				pmap.put(entry.getPixelPos(), entry);
			gcpPosMap.put(name, pmap);
		}

		String filesPath = "/home/efi/SNAP/sentinel-images/";
		File masterFile = new File(filesPath,
				"subset_0_of_S1A_IW_GRDH_1SDV_20151110T145915_20151110T145940_008544_00C1A6_F175.dim");
		File slaveFile = new File(filesPath,
				"subset_1_of_S1A_IW_GRDH_1SDV_20151029T145915_20151029T145940_008369_00BD0B_334C.dim");
		// File masterFile = new File(filesPath,
		// "subset3_of_S1A_IW_GRDH_1SSV_20141225T142407_20141225T142436_003877_004A54_040F.dim");
		// File slaveFile = new File(filesPath,
		// "subset3_of_S1A_IW_GRDH_1SSV_20150518T142409_20150518T142438_005977_007B49_AF76.dim");
		SerialProcessor sp = new SerialProcessor();
		// String[] selectedPolarisations={"VH"};
		String[] selectedPolarisations = null;
		MyRead myRead1 = new MyRead(masterFile, "read1");
		sp.getBufferedImage(myRead1, selectedPolarisations);

		MyBandSelect bandselect1 = new MyBandSelect(selectedPolarisations, null);
		bandselect1.setSourceProduct(myRead1.getTargetProduct());
		sp.initOperatorForMultipleBands(bandselect1);

		MyRead myRead2 = new MyRead(slaveFile, "read2");
		sp.getBufferedImage(myRead2, selectedPolarisations);

		MyBandSelect bandselect2 = new MyBandSelect(selectedPolarisations, null);
		bandselect2.setSourceProduct(myRead2.getTargetProduct());
		sp.initOperatorForMultipleBands(bandselect2);

		Boolean[] bParams1 = { false, false, false, false, true, false, false, false };
		MyCalibration myCalibration1 = new MyCalibration(null, bParams1, null);
		myCalibration1.setSourceProduct(bandselect1.getTargetProduct());
		myCalibration1.setId("myCalibration1");
		sp.initOperatorForMultipleBands(myCalibration1);
		myRead1 = null;
		sp.processTiles(myCalibration1);

		MyCalibration myCalibration2 = new MyCalibration(null, bParams1, null);
		myCalibration2.setSourceProduct(bandselect2.getTargetProduct());
		myCalibration2.setId("myCalibration2");
		sp.initOperatorForMultipleBands(myCalibration2);
		myRead2 = null;
		sp.processTiles(myCalibration2);

		Product[] sourcesForCreateStack = new Product[2];
		sourcesForCreateStack[0] = myCalibration1.getTargetProduct();
		sourcesForCreateStack[1] = myCalibration2.getTargetProduct();

		String[] parameters = { "NONE", "Master", "Orbit" };
		MyCreateStack myCreateStack = new MyCreateStack(parameters);
		myCreateStack.setSourceProduct(sourcesForCreateStack);
		myCreateStack.setId("myCreateStack");
		sp.initOperatorForMultipleBands(myCreateStack);
		sp.processTiles(myCreateStack);
		myRead1 = null;
		myRead2 = null;

		boolean[] bParams = { false, false, false, false };
		int[] iParams = { 2000, 10, 3 };
		double[] dParams = { 0.25, 0.6 };
		String[] sParams = { "128", "128", "4", "4", "32", "32" };
		MyGCPSelection gcpSelectionOp = new MyGCPSelection(bParams, dParams, iParams, sParams);
		gcpSelectionOp.setSourceProduct(myCreateStack.getTargetProduct());
		gcpSelectionOp.setId("gcpSelection");
		sp.initOperatorForMultipleBands(gcpSelectionOp);
		myCreateStack = null;
		sp.processTiles(gcpSelectionOp);

		ProductNodeGroup<Placemark> correctMasterGCPs = gcpSelectionOp.getMasterGcpGroup();
		System.out.println("init size for gcps of master " + gcpsMasterSet.size());
		for (int i = 0; i < correctMasterGCPs.getNodeCount(); i++) {
			if (gcpsMasterSet.containsKey(correctMasterGCPs.get(i).getPixelPos())) {
				// System.out.println(correctMasterGCPs.get(i).getPixelPos());
				gcpsMasterSet.remove(correctMasterGCPs.get(i).getPixelPos());
			} else
				System.out.println(gcpsMasterSet.get(i).getPixelPos());
		}
		System.out.println("final size for gcps of master " + gcpsMasterSet.size());
		Product targetProductGCP = gcpSelectionOp.getTargetProduct();
		ProductNodeGroup<Placemark> correctGCPs = null;
		for (int i = 0; i < targetProductGCP.getNumBands(); i++) {

			correctGCPs = GCPManager.instance().getGcpGroup(targetProductGCP.getBandAt(i));
			Map<PixelPos, Placemark> pmap = gcpPosMap.get(targetProductGCP.getBandAt(i).getName());

			if (pmap == null)
				continue;
			System.out.println(
					"init size for gcps of band " + targetProductGCP.getBandAt(i).getName() + " " + pmap.size());
			System.out.println("size for correct gcps of band " + targetProductGCP.getBandAt(i).getName() + " "
					+ correctGCPs.getNodeCount());
			for (int j = 0; j < correctGCPs.getNodeCount(); j++) {
				Placemark slaveGCP = pmap.get(correctGCPs.get(j).getPixelPos());
				if (slaveGCP != null) {

					if (slaveGCP.getName() .equals(correctGCPs.get(j).getName()))
						pmap.remove(correctGCPs.get(j).getPixelPos());
					else
						System.out.println(correctGCPs.get(j).getPixelPos());
				} else
					System.out.println(correctGCPs.get(j).getPixelPos());

			}
			// System.out.println("");
			// System.out.println("");
			// for(PixelPos pos:pmap.keySet()){
			// System.out.println(pos);
			//
			// }
			System.out.println(
					"final size for gcps of band " + targetProductGCP.getBandAt(i).getName() + " " + pmap.size());
		}
	}

	public static void checkWarp(List<Tuple2<Tuple2<Point, String>, MyTile>> warpkRes) throws Exception {
		System.out.println("check starts");
		Map<String, Map<Point, MyTile>> bandsMap = new HashMap<String, Map<Point, MyTile>>();
		for (int i = 0; i < warpkRes.size(); i++) {
			String bandName = warpkRes.get(i)._1._2;
			if (bandsMap.containsKey(bandName)) {
				bandsMap.get(bandName).put(warpkRes.get(i)._1._1, warpkRes.get(i)._2);
			} else {
				Map<Point, MyTile> tilesMap = new HashMap<Point, MyTile>();
				tilesMap.put(warpkRes.get(i)._1._1, warpkRes.get(i)._2);
				bandsMap.put(bandName, tilesMap);
			}
		}
		// String filesPath = "D:\\work-di\\SNAP\\sentinel-images\\";
		String filesPath = "/home/efi/SNAP/sentinel-images/";
//		File masterFile = new File(filesPath,
//				"subset_0_of_S1A_IW_GRDH_1SDV_20151110T145915_20151110T145940_008544_00C1A6_F175.dim");
//		File slaveFile = new File(filesPath,
//				"subset_1_of_S1A_IW_GRDH_1SDV_20151029T145915_20151029T145940_008369_00BD0B_334C.dim");
		 File masterFile = new File(filesPath,
		 "subset3_of_S1A_IW_GRDH_1SSV_20141225T142407_20141225T142436_003877_004A54_040F.dim");
		 File slaveFile = new File(filesPath,
		 "subset3_of_S1A_IW_GRDH_1SSV_20150518T142409_20150518T142438_005977_007B49_AF76.dim");
		SerialProcessor sp = new SerialProcessor();
		// String[] selectedPolarisations={"VH"};
		String[] selectedPolarisations = null;
		MyRead myRead1 = new MyRead(masterFile, "read1");
		sp.getBufferedImage(myRead1, selectedPolarisations);

		MyBandSelect bandselect1 = new MyBandSelect(selectedPolarisations, null);
		bandselect1.setSourceProduct(myRead1.getTargetProduct());
		sp.initOperatorForMultipleBands(bandselect1);

		MyRead myRead2 = new MyRead(slaveFile, "read2");
		sp.getBufferedImage(myRead2, selectedPolarisations);

		MyBandSelect bandselect2 = new MyBandSelect(selectedPolarisations, null);
		bandselect2.setSourceProduct(myRead2.getTargetProduct());
		sp.initOperatorForMultipleBands(bandselect2);

		Boolean[] bParams1 = { false, false, false, false, true, false, false, false };
		MyCalibration myCalibration1 = new MyCalibration(null, bParams1, null);
		myCalibration1.setSourceProduct(bandselect1.getTargetProduct());
		myCalibration1.setId("myCalibration1");
		sp.initOperatorForMultipleBands(myCalibration1);
		myRead1 = null;
		sp.processTiles(myCalibration1);

		MyCalibration myCalibration2 = new MyCalibration(null, bParams1, null);
		myCalibration2.setSourceProduct(bandselect2.getTargetProduct());
		myCalibration2.setId("myCalibration2");
		sp.initOperatorForMultipleBands(myCalibration2);
		myRead2 = null;
		sp.processTiles(myCalibration2);

		Product[] sourcesForCreateStack = new Product[2];
		sourcesForCreateStack[0] = myCalibration1.getTargetProduct();
		sourcesForCreateStack[1] = myCalibration2.getTargetProduct();

		String[] parameters = { "NONE", "Master", "Orbit" };
		MyCreateStack myCreateStack = new MyCreateStack(parameters);
		myCreateStack.setSourceProduct(sourcesForCreateStack);
		myCreateStack.setId("myCreateStack");
		sp.initOperatorForMultipleBands(myCreateStack);
		sp.processTiles(myCreateStack);
		myRead1 = null;
		myRead2 = null;

		boolean[] bParams = { false, false, false, false };
		int[] iParams = { 1000, 10, 3 };
		double[] dParams = { 0.25, 0.6 };
		String[] sParams = { "128", "128", "4", "4", "32", "32" };
		MyGCPSelection gcpSelectionOp = new MyGCPSelection(bParams, dParams, iParams, sParams);
		gcpSelectionOp.setSourceProduct(myCreateStack.getTargetProduct());
		gcpSelectionOp.setId("gcpSelection");
		sp.initOperatorForMultipleBands(gcpSelectionOp);
		myCreateStack = null;
		sp.processTiles(gcpSelectionOp);

		boolean[] bParams2 = { false, false };
		MyWarp warpOp = new MyWarp(bParams2, 0.05f, 1, "Bilinear interpolation");
		warpOp.setSourceProduct(gcpSelectionOp.getTargetProduct());
		warpOp.setId("warp");
		sp.initOperatorForMultipleBands(warpOp);
		// gcpSelectionOp = null;

		Map<String, List<Tuple2<Point, Tile>>> warpRes = sp.processTiles2(warpOp);

		String[] names1 = warpOp.getTargetProduct().getBandNames();

		String[] masterBandNames = StackUtils.getMasterBandNames(warpOp.getTargetProduct());
		Set<String> masterBands = new HashSet(Arrays.asList(masterBandNames));
		for (int j = 0; j < names1.length; j++) {
			if (masterBands.contains(names1[j]))
				continue;
			Map<Point, MyTile> tilesMap = bandsMap.get(names1[j]);
			List<Tuple2<Point, Tile>> calTiles = warpRes.get(names1[j]);

			System.out.println("init size for band " + names1[j] + " " + tilesMap.size());
			for (int i = 0; i < calTiles.size(); i++) {
				ProductData obj = tilesMap.get(calTiles.get(i)._1).getDataBuffer();
				if (obj.equalElems(calTiles.get(i)._2.getDataBuffer())) {
					tilesMap.remove(calTiles.get(i)._1);
				} else {
					System.out.println("point " + calTiles.get(i)._1 + " rect " + calTiles.get(i)._2.getRectangle());
					// float[] arr1=(float[])obj.getElems();
					// float[] arr2=calTiles.get(i)._2.getDataBufferFloat();
					// for(int k=0;k<arr1.length;k++){
					//
					// if(arr1[k]!=arr2[k])
					// System.out.println("index "+k);
					//
					// }
					// break;
				}
			}

			System.out.println("final size for band " + names1[j] + " " + tilesMap.size());
		}
		myCalibration1 = null;
		myCalibration2 = null;
		myCreateStack = null;
		warpOp = null;
	}

	public static void checkChangeDetection(List<Tuple2<Tuple2<Point, String>, MyTile>> changeDRes) throws Exception {
		System.out.println("check starts");
		Map<String, Map<Point, MyTile>> bandsMap = new HashMap<String, Map<Point, MyTile>>();
		for (int i = 0; i < changeDRes.size(); i++) {
			String bandName = changeDRes.get(i)._1._2;
			if (bandsMap.containsKey(bandName)) {
				bandsMap.get(bandName).put(changeDRes.get(i)._1._1, changeDRes.get(i)._2);
			} else {
				Map<Point, MyTile> tilesMap = new HashMap<Point, MyTile>();
				tilesMap.put(changeDRes.get(i)._1._1, changeDRes.get(i)._2);
				bandsMap.put(bandName, tilesMap);
			}
		}
		String filesPath = "D:\\work-di\\SNAP\\sentinel-images\\";
		// String filesPath = "/home/efi/SNAP/sentinel-images/";
		File masterFile = new File(filesPath,
				"subset_0_of_S1A_IW_GRDH_1SDV_20151110T145915_20151110T145940_008544_00C1A6_F175.dim");
		File slaveFile = new File(filesPath,
				"subset_1_of_S1A_IW_GRDH_1SDV_20151029T145915_20151029T145940_008369_00BD0B_334C.dim");
		// File masterFile = new File(filesPath,
		// "subset3_of_S1A_IW_GRDH_1SSV_20141225T142407_20141225T142436_003877_004A54_040F.dim");
		// File slaveFile = new File(filesPath,
		// "subset3_of_S1A_IW_GRDH_1SSV_20150518T142409_20150518T142438_005977_007B49_AF76.dim");
		SerialProcessor sp = new SerialProcessor();
		String[] selectedPolarisations = { "VH" };
		MyRead myRead1 = new MyRead(masterFile, "read1");
		sp.getBufferedImage(myRead1, selectedPolarisations);

		MyBandSelect bandselect1 = new MyBandSelect(selectedPolarisations, null);
		bandselect1.setSourceProduct(myRead1.getTargetProduct());
		sp.initOperatorForMultipleBands(bandselect1);

		MyRead myRead2 = new MyRead(slaveFile, "read2");
		sp.getBufferedImage(myRead2, selectedPolarisations);

		MyBandSelect bandselect2 = new MyBandSelect(selectedPolarisations, null);
		bandselect2.setSourceProduct(myRead2.getTargetProduct());
		sp.initOperatorForMultipleBands(bandselect2);

		Boolean[] bParams1 = { false, false, false, false, true, false, false, false };
		MyCalibration myCalibration1 = new MyCalibration(null, bParams1, null);
		myCalibration1.setSourceProduct(bandselect1.getTargetProduct());
		myCalibration1.setId("myCalibration1");
		sp.initOperatorForMultipleBands(myCalibration1);
		myRead1 = null;
		sp.processTiles(myCalibration1);

		MyCalibration myCalibration2 = new MyCalibration(null, bParams1, null);
		myCalibration2.setSourceProduct(bandselect2.getTargetProduct());
		myCalibration2.setId("myCalibration2");
		sp.initOperatorForMultipleBands(myCalibration2);
		myRead2 = null;
		sp.processTiles(myCalibration2);

		Product[] sourcesForCreateStack = new Product[2];
		sourcesForCreateStack[0] = myCalibration1.getTargetProduct();
		sourcesForCreateStack[1] = myCalibration2.getTargetProduct();

		String[] parameters = { "NONE", "Master", "Orbit" };
		MyCreateStack myCreateStack = new MyCreateStack(parameters);
		myCreateStack.setSourceProduct(sourcesForCreateStack);
		myCreateStack.setId("myCreateStack");
		sp.initOperatorForMultipleBands(myCreateStack);
		sp.processTiles(myCreateStack);
		myRead1 = null;
		myRead2 = null;

		boolean[] bParams = { false, false, false, false };
		int[] iParams = { 1000, 10, 3 };
		double[] dParams = { 0.25, 0.6 };
		String[] sParams = { "128", "128", "4", "4", "32", "32" };
		MyGCPSelection gcpSelectionOp = new MyGCPSelection(bParams, dParams, iParams, sParams);
		gcpSelectionOp.setSourceProduct(myCreateStack.getTargetProduct());
		gcpSelectionOp.setId("gcpSelection");
		sp.initOperatorForMultipleBands(gcpSelectionOp);
		myCreateStack = null;
		sp.processTiles(gcpSelectionOp);

		boolean[] bParams2 = { false, false };
		MyWarp warpOp = new MyWarp(bParams2, 0.05f, 1, "Bilinear interpolation");
		warpOp.setSourceProduct(gcpSelectionOp.getTargetProduct());
		warpOp.setId("warp");
		sp.initOperatorForMultipleBands(warpOp);
		sp.processTiles(warpOp);

		boolean[] bParams3 = { false, false };
		float[] fParams = { 2.0f, -2.0f };
		MyChangeDetection myChangeDetection = new MyChangeDetection(bParams3, fParams, null);
		myChangeDetection.setSourceProduct(warpOp.getTargetProduct());
		sp.initOperatorForMultipleBands(myChangeDetection);
		// gcpSelectionOp = null;

		Map<String, List<Tuple2<Point, Tile>>> changeDetectionRes = sp.processTiles2(myChangeDetection);

		String[] names1 = myChangeDetection.getTargetProduct().getBandNames();

		String[] masterBandNames = StackUtils.getMasterBandNames(myChangeDetection.getTargetProduct());
		Set<String> masterBands = new HashSet(Arrays.asList(masterBandNames));
		for (int j = 0; j < names1.length; j++) {
			if (masterBands.contains(names1[j]))
				continue;
			Map<Point, MyTile> tilesMap = bandsMap.get(names1[j]);
			List<Tuple2<Point, Tile>> changeDTiles = changeDetectionRes.get(names1[j]);

			System.out.println("init size for band " + names1[j] + " " + tilesMap.size());
			for (int i = 0; i < changeDTiles.size(); i++) {
				ProductData obj = tilesMap.get(changeDTiles.get(i)._1).getDataBuffer();
				if (obj.equalElems(changeDTiles.get(i)._2.getDataBuffer())) {
					tilesMap.remove(changeDTiles.get(i)._1);
				} else {
					System.out.println(
							"point " + changeDTiles.get(i)._1 + " rect " + changeDTiles.get(i)._2.getRectangle());
					// float[] arr1=(float[])obj.getElems();
					// float[] arr2=calTiles.get(i)._2.getDataBufferFloat();
					// for(int k=0;k<arr1.length;k++){
					//
					// if(arr1[k]!=arr2[k])
					// System.out.println("index "+k);
					//
					// }
					// break;
				}
			}

			System.out.println("final size for band " + names1[j] + " " + tilesMap.size());
		}
		myCalibration1 = null;
		myCalibration2 = null;
		myCreateStack = null;
		warpOp = null;
	}
}
