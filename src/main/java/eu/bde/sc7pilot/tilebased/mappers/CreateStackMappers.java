package eu.bde.sc7pilot.tilebased.mappers;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.esa.snap.core.util.ImageUtils;

import eu.bde.sc7pilot.taskbased.Utils;
import eu.bde.sc7pilot.tilebased.metadata.ImageMetadata;
import eu.bde.sc7pilot.tilebased.model.MyTile;
import eu.bde.sc7pilot.tilebased.operator.CreateStack;
import scala.Tuple2;
import scala.Tuple3;

public class CreateStackMappers {
	public static Tuple2<Tuple2<Point, String>, MyTile> createStack(
			Tuple2<Tuple3<Point, String, Rectangle>, Iterable<MyTile>> pair, Map<String, ImageMetadata> map) {
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
						: currRaster.createChild(currRect.x, currRect.y, currRect.width, currRect.height, currRect.x,
								currRect.y, null);
				destinationRaster = Raster.createWritableRaster(sourceRaster.getSampleModel(),
						sourceRaster.getDataBuffer(), currRaster.getBounds().getLocation());
				break;
			} else {
				if (first) {

					destinationRaster = Utils.createWritableRaster(dependentRect, myTile.getType());
					first = false;
				}
				Object dataBuffer = ImageUtils.createDataBufferArray(myTile.getWritableRaster().getTransferType(),
						(int) currRect.getWidth() * (int) currRect.getHeight());
				myTile.getWritableRaster().getDataElements((int) currRect.getMinX(), (int) currRect.getMinY(),
						(int) currRect.getWidth(), (int) currRect.getHeight(), dataBuffer);
				destinationRaster.setDataElements((int) currRect.getMinX(), (int) currRect.getMinY(),
						(int) currRect.getWidth(), (int) currRect.getHeight(), dataBuffer);
			}
		}
		sourceTile = new MyTile(destinationRaster, dependentRect, tileType);

		// compute tile, createstack
		CreateStack createStack = new CreateStack();
		ImageMetadata srcImgMetadataStack = map.get(pair._1._2() + "_" + "stack");
		ImageMetadata trgImgMetadataStack = map.get(srcImgMetadataStack.getBandPairName() + "_" + "stack");
		MyTile targetTile = new MyTile(trgImgMetadataStack.getWritableRaster(pair._1._1().x, pair._1._1().y),
				trgImgMetadataStack.getRectangle(pair._1._1().x, pair._1._1().y), trgImgMetadataStack.getDataType());
		try {
			createStack.computeTile(targetTile, sourceTile, srcImgMetadataStack.getOffsetMap(),
					srcImgMetadataStack.getImageWidth(), srcImgMetadataStack.getImageHeight(),
					trgImgMetadataStack.getNoDataValue());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return new Tuple2<Tuple2<Point, String>, MyTile>(
				new Tuple2<Point, String>(pair._1._1(), trgImgMetadataStack.getBandName()), targetTile);
	}
	public static List<Tuple2<Tuple2<Integer, String>, MyTile>> mapToRows(Tuple2<Tuple2<Point, String>, MyTile> pair,Map<String, ImageMetadata> map,int rowsCount) {
		List<Tuple2<Tuple2<Integer, String>, MyTile>> pairs = new ArrayList<Tuple2<Tuple2<Integer, String>, MyTile>>();
		int nOfKeys = (int) Math.ceil((float) rowsCount / (float) 8);
		int key = 0;
		ImageMetadata trgImgMetadatastack = map.get(pair._1._2+"_stack");
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
	}
	
}
