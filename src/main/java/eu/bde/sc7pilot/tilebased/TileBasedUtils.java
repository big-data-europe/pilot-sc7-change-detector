package eu.bde.sc7pilot.tilebased;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.RenderedImage;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.media.jai.RenderedOp;
import javax.media.jai.TiledImage;
import javax.media.jai.WarpOpImage;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.engine_utilities.gpf.StackUtils;

import eu.bde.sc7pilot.taskbased.LoopLimits;
import eu.bde.sc7pilot.taskbased.MyCreateStack;
import eu.bde.sc7pilot.taskbased.MyWarp;
import eu.bde.sc7pilot.taskbased.MyWarp.WarpData;
import eu.bde.sc7pilot.taskbased.Utils;
import eu.bde.sc7pilot.tilebased.metadata.ImageMetadata;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import scala.Tuple2;

public class TileBasedUtils {
	public static void getDependRects(Object2ObjectMap<String, ImageMetadata> imageMetadata, MyCreateStack myCreateStack2,Object2ObjectMap<String, Object2ObjectMap<Point, ObjectList<Tuple2<Point, Rectangle>>>> dependRects) {
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
	public static void getSourceDependWarp(MyWarp myWarp,Map<String, WarpData> warpdataMap,Map<String,ImageMetadata> imageMetadata,Object2ObjectMap<String,Object2ObjectMap<Point,ObjectSet<Rectangle>>> dependRectsWarp2,Object2ObjectMap<String,Object2ObjectMap<Rectangle, ObjectList<Point>>> dependPointsWarp ){
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
}
