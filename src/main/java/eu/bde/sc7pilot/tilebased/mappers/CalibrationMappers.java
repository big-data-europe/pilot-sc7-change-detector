package eu.bde.sc7pilot.tilebased.mappers;

import java.awt.Point;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import eu.bde.sc7pilot.hdfsreader.BandInfo;
import eu.bde.sc7pilot.hdfsreader.ReadHDFSTile;
import eu.bde.sc7pilot.tilebased.metadata.CalibrationMetadata;
import eu.bde.sc7pilot.tilebased.metadata.ImageMetadata;
import eu.bde.sc7pilot.tilebased.model.MyTile;
import eu.bde.sc7pilot.tilebased.operator.Sentinel1Calibrator;
import scala.Tuple2;

public class CalibrationMappers {

	public static List<Tuple2<Tuple2<Point, String>, MyTile>> calibrationMaster(Iterator<Tuple2<String, Point>> iterator,
																				Map<String, BandInfo> infos,
																				Map<String, ImageMetadata> map,
																				Map<String, CalibrationMetadata> calMetadataMap) {
		List<Tuple2<Tuple2<Point, String>, MyTile>> tiles = new ArrayList<Tuple2<Tuple2<Point, String>, MyTile>>();
		List<Tuple2<String, Point>> points = Lists.newArrayList(iterator);
		Map<String, ReadHDFSTile> bands = new HashMap<String, ReadHDFSTile>();

		for (int i = 0; i < points.size(); i++) {
			ReadHDFSTile readHDFSTile = null;
			Tuple2<String, Point> tuple = points.get(i);
			if (!bands.containsKey(tuple._1 + "_read1")) {
				BandInfo info = infos.get(tuple._1 + "_read1");
				readHDFSTile = new ReadHDFSTile(info.getHdfsPath());
				bands.put(tuple._1 + "_read1", readHDFSTile);
			} else
				readHDFSTile = bands.get(tuple._1 + "_read1");

			ImageMetadata srcImgMetadataCal1 = map.get(tuple._1 + "_" + "cal1");
			ImageMetadata trgImgMetadataCal1 = map.get(srcImgMetadataCal1.getBandPairName() + "_" + "cal1");

			ImageMetadata imgMetadataRead = map.get(tuple._1 + "_" + "read1");
			MyTile readTile = new MyTile(imgMetadataRead.getWritableRaster(tuple._2.x, tuple._2.y),
												imgMetadataRead.getRectangle(tuple._2.x, tuple._2.y),
												imgMetadataRead.getDataType());
			readHDFSTile.readTile(readTile, infos.get(tuple._1 + "_" + "read1"));

			Point targetPoint = tuple._2;
			CalibrationMetadata calMeatadata = calMetadataMap.get(tuple._1 + "_" + "cal1");
			Sentinel1Calibrator sentinel1Calibrator = new Sentinel1Calibrator(calMeatadata);
			MyTile targetTile = new MyTile(trgImgMetadataCal1.getWritableRaster(targetPoint.x, targetPoint.y),
													trgImgMetadataCal1.getRectangle(targetPoint.x, targetPoint.y),
													trgImgMetadataCal1.getDataType());
			
			//*** Monitoring targetTile before computetile
			int m = 0;
			float[] targetTileBufferBefore = targetTile.getDataBufferFloat();
			for(int j = 0; j < targetTileBufferBefore.length; j++) {
				if (targetTileBufferBefore[j] == 0.0) {
					m++;
				}
			}
			System.out.println("\n\n\t\t************************");
			System.out.println(targetTileBufferBefore.length + "\t\tPixels in targetTileBefore No.:\t" + i);
			System.out.println("\t" + m + " of them are ZEROW\n\n");
			//~~~ Monitoring targetTile before computetile
			
			sentinel1Calibrator.computeTile(readTile, null, targetTile, srcImgMetadataCal1.getNoDataValue(), trgImgMetadataCal1.getBandName());
			
			//*** Monitoring targetTile after computetile
			int l = 0;
			float[] targetTileBuffer = targetTile.getDataBufferFloat();
			for(int j = 0; j < targetTileBuffer.length; j++) {
				if (targetTileBuffer[j] == 0.0) {
					l++;
				}
			}
			System.out.println(targetTileBuffer.length + "\t\tPixels in targetTile No.:\t" + i);
			System.out.println("\t" + l + " of them are ZEROW");
			System.out.println("***************************************************************\n\n\n");
			//~~~ Monitoring targetTile after computetile
			
			tiles.add(new Tuple2<Tuple2<Point, String>, MyTile>(new Tuple2<Point, String>(targetPoint, trgImgMetadataCal1.getBandName()), targetTile));
			//int k = 0;
			
//			float[] readTileBuffer = readTile.getDataBufferFloat();
//			if(readTileBuffer == null) {
//				System.out.println(readTileBuffer + "\tIs the readTileBuffer");
//			}
//			else {
//				for(int m = 0; m < 100; m++) {
//					System.out.println(readTileBuffer[m]);
//					System.out.println("are the values of th first 100 pixels");
//				}
//			}

		}
		return tiles;
	}
	public static List<Tuple2<String,  MyTile>> calibrationSlave(Iterator<Tuple2<String, Point>> iterator,
			Map<String, BandInfo> infos, Map<String, ImageMetadata> map,
			Map<String, CalibrationMetadata> calMetadataMap) {
		List<Tuple2<String,  MyTile>> tiles = new ArrayList<Tuple2<String,  MyTile>>();
		List<Tuple2<String, Point>> points = Lists.newArrayList(iterator);
		Map<String, ReadHDFSTile> bands = new HashMap<String, ReadHDFSTile>();

		for (int i = 0; i < points.size(); i++) {
			ReadHDFSTile readHDFSTile = null;
			Tuple2<String, Point> tuple = points.get(i);
			if (!bands.containsKey(tuple._1 + "_read2")) {
				BandInfo info = infos.get(tuple._1 + "_read2");
				readHDFSTile = new ReadHDFSTile(info.getHdfsPath());
				bands.put(tuple._1 + "_read2", readHDFSTile);
			} else
				readHDFSTile = bands.get(tuple._1 + "_read2");

			ImageMetadata srcImgMetadataCal2 = map.get(tuple._1 + "_" + "cal2");
			ImageMetadata trgImgMetadataCal2 = map.get(srcImgMetadataCal2.getBandPairName() + "_" + "cal2");

			ImageMetadata imgMetadataRead = map.get(tuple._1 + "_" + "read2");
			MyTile readTile = new MyTile(imgMetadataRead.getWritableRaster(tuple._2.x, tuple._2.y),
					imgMetadataRead.getRectangle(tuple._2.x, tuple._2.y), imgMetadataRead.getDataType());
			readHDFSTile.readTile(readTile, infos.get(tuple._1 + "_" + "read2"));

			Point targetPoint = tuple._2;
			CalibrationMetadata calMeatadata = calMetadataMap.get(tuple._1 + "_" + "cal2");
			Sentinel1Calibrator sentinel1Calibrator = new Sentinel1Calibrator(calMeatadata);
			MyTile targetTile = new MyTile(trgImgMetadataCal2.getWritableRaster(targetPoint.x, targetPoint.y),
					trgImgMetadataCal2.getRectangle(targetPoint.x, targetPoint.y), trgImgMetadataCal2.getDataType());
			sentinel1Calibrator.computeTile(readTile, null, targetTile, srcImgMetadataCal2.getNoDataValue(),
					trgImgMetadataCal2.getBandName());
			tiles.add(new Tuple2<String,  MyTile>(trgImgMetadataCal2.getBandName(), targetTile));
		}
		return tiles;
	}
	public static List<Tuple2<Integer, MyTile>> mapToRows(Tuple2<Tuple2<Point, String>, MyTile> pair,
			ImageMetadata[] imgmetadata, int rowsCount) {
		List<Tuple2<Integer, MyTile>> pairs = new ArrayList<Tuple2<Integer, MyTile>>();
		ImageMetadata trgImgMetadataCal1 = imgmetadata[0];
		
		int nOfKeys = (int) Math.ceil((float) rowsCount / (float) 4);
		int key = 0;
		int y = trgImgMetadataCal1.getTileIndices(pair._2.getMinX(), pair._2.getMinY()).y;
		for (int i = 1; i <= nOfKeys; i++) {
			if (y < 4 * i && y >= 4* (i - 1)) {
				key = i;
				pairs.add(new Tuple2<Integer, MyTile>(key,pair._2));
				if (i != nOfKeys && ((y == 4 * i - 1)))
					pairs.add(new Tuple2<Integer, MyTile>(key + 1, pair._2));
				if (i != 1 && ((y == 4 * (i - 1))))
					pairs.add(new Tuple2<Integer, MyTile>(key - 1, pair._2));
				break;
			}
		}

		return pairs;
	}
	public static List<Tuple2<Tuple2<Integer, String>, MyTile>> mapToRows(Tuple2<Tuple2<Point, String>, MyTile> pair,
			Map<String, ImageMetadata> map, int rowsCount) {
		List<Tuple2<Tuple2<Integer, String>, MyTile>> pairs = new ArrayList<Tuple2<Tuple2<Integer, String>, MyTile>>();
	
		ImageMetadata trgImgMetadataCal1 = map.get(pair._1._2 + "_cal1");
	
		String name = map.get(pair._1._2 + "_stack").getBandPairName();
		int nOfKeys = (int) Math.ceil((float) rowsCount / (float) 8);
		int key = 0;
		int y = trgImgMetadataCal1.getTileIndices(pair._2.getMinX(), pair._2.getMinY()).y;
		for (int i = 1; i <= nOfKeys; i++) {
			if (y < 8 * i && y >= 8 * (i - 1)) {
				key = i;
				pairs.add(new Tuple2<Tuple2<Integer, String>, MyTile>(new Tuple2<Integer, String>(key, name), pair._2));
				if (i != nOfKeys && (y == 8 * i - 1))
					pairs.add(new Tuple2<Tuple2<Integer, String>, MyTile>(new Tuple2<Integer, String>(key + 1, name),
							pair._2));
				if (i != 1 && (y == 8 * (i - 1)))
					pairs.add(new Tuple2<Tuple2<Integer, String>, MyTile>(new Tuple2<Integer, String>(key - 1, name),
							pair._2));
				break;
			}
		}
		return pairs;
	}
}
