package eu.bde.sc7pilot.tilebased.mappers;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.WritableRaster;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Placemark;
import org.esa.snap.core.datamodel.ProductNodeGroup;
import org.esa.snap.core.datamodel.TiePointGeoCoding;

import com.google.common.collect.Lists;

import eu.bde.sc7pilot.taskbased.Utils;
import eu.bde.sc7pilot.tilebased.metadata.GCPMetadata;
import eu.bde.sc7pilot.tilebased.metadata.ImageMetadata;
import eu.bde.sc7pilot.tilebased.model.MyTile;
import eu.bde.sc7pilot.tilebased.operator.GCPSelection;
import scala.Tuple2;

public class GCPMappers {

	public static List<Tuple2<String, Tuple2<Integer, Placemark>>> GCPSelection(Tuple2<Tuple2<Integer, String>, Tuple2<Iterable<MyTile>, Iterable<MyTile>>> pair,
																				GCPMetadata GCPMetadataBroad2,
																				Map<String, ImageMetadata> map,
																				ProductNodeGroup<Placemark> masterGcpGroup,
																				int rowsCount) {
		String devsMSG = "[Dev's MSG]\t";
		List<MyTile> masterTiles = Lists.newArrayList(pair._2._1.iterator());
		List<MyTile> slaveTiles = Lists.newArrayList(pair._2._2.iterator());
		int x = masterTiles.get(0).getMinX();
		int y = masterTiles.get(0).getMinY();
		int width = 0;
		int height = 0;
		int type = 0;
		for (int i = 0; i < masterTiles.size(); i++) {
			MyTile masterTile = masterTiles.get(i);
			if (masterTile.getMinX() < x)
				x = masterTile.getMinX();
			if (masterTile.getMinY() < y)
				y = masterTile.getMinY();
			type = masterTile.getType();
		}
		for (int i = 0; i < masterTiles.size(); i++) {
			MyTile masterTile = masterTiles.get(i);
			if (masterTile.getMinX() == x) {
				height += masterTile.getHeight();
			}
			if (masterTile.getMinY() == y)
				width += masterTile.getWidth();
		}
		WritableRaster masterRaster = Utils.createWritableRaster(new Rectangle(x, y, width, height), type);
		for (int i = 0; i < masterTiles.size(); i++) {
			masterRaster.setDataElements(masterTiles.get(i).getMinX(),
										masterTiles.get(i).getMinY(),
										masterTiles.get(i).getWidth(),
										masterTiles.get(i).getHeight(),
										masterTiles.get(i).getRawSamples().getElems());
		}
		x = slaveTiles.get(0).getMinX();
		y = slaveTiles.get(0).getMinY();
		width = 0;
		height = 0;
		for (int i = 0; i < slaveTiles.size(); i++) {
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
			slaveRaster.setDataElements(slaveTiles.get(i).getMinX(),
										slaveTiles.get(i).getMinY(),
										slaveTiles.get(i).getWidth(),
										slaveTiles.get(i).getHeight(),
										slaveTiles.get(i).getRawSamples().getElems());
		}
		MyTile masterTile = new MyTile(masterRaster, new Rectangle(x, y, masterRaster.getWidth(), masterRaster.getHeight()), type);
		int j = 0;
		int masterPixels = masterTile.getDataBufferFloat().length;
		for(int i = 0; i < masterPixels; i++) {
			if(masterTile.getDataBufferFloat()[i] == 0.0) {
				j++;
			}
		}
		int nonZerowsMaster = masterPixels -j;
		System.out.println("\n" + devsMSG + j + "\t\tZEROW masterPixels");
		System.out.println(devsMSG + nonZerowsMaster + "\t\tnon-Zerow masterPixels");
		System.out.println(devsMSG + masterPixels + "\t\tAll masterPixels\n");
		MyTile slaveTile = new MyTile(slaveRaster, new Rectangle(x, y, slaveRaster.getWidth(), slaveRaster.getHeight()), type);
		int k = 0;
		int slavePixels = slaveTile.getDataBufferFloat().length;
		for(int i = 0; i < slavePixels; i++) {
			if(slaveTile.getDataBufferFloat()[i] == 0.0) {
				k++;
			}
		}
		int nonZerowsSlave = slavePixels -j;
		System.out.println("\n" + devsMSG + k + "\t\tZEROW slavePixels");
		System.out.println(devsMSG + nonZerowsSlave + "\t\tnon-Zerow slavePixels");
		System.out.println(devsMSG + slavePixels + "\t\tAll slavePixels\n");

		List<Tuple2<String, Tuple2<Integer, Placemark>>> slaveGCPsRes = new ArrayList<Tuple2<String, Tuple2<Integer, Placemark>>>();
		final int numberOfMasterGCPs = masterGcpGroup.getNodeCount();

		ImageMetadata trgImgMetadataGCP = map.get(pair._1._2 +"_gcp"+ "_target");
		ImageMetadata srcImgMetadataGCP = map.get(pair._1._2 +"_gcp"+ "_source");

		int[] iParams2 = {Integer.parseInt(GCPMetadataBroad2.getCoarseRegistrationWindowWidth()),
							Integer.parseInt(GCPMetadataBroad2.getCoarseRegistrationWindowHeight()),
							GCPMetadataBroad2.getMaxIteration(),
							Integer.parseInt(GCPMetadataBroad2.getRowInterpFactor()),
							Integer.parseInt(GCPMetadataBroad2.getColumnInterpFactor()),
							srcImgMetadataGCP.getImageWidth(),
							srcImgMetadataGCP.getImageHeight()
							};
		double[] dParams3 = {GCPMetadataBroad2.getGcpTolerance(), trgImgMetadataGCP.getNoDataValue(), srcImgMetadataGCP.getNoDataValue()};

		final int[] offset2 = new int[2];
		int nOfKeys = (int) Math.ceil((float) rowsCount / (float) 4);
		int bMinY = (int) masterTile.getRectangle().getMinY();
		int bHeight = (int) masterTile.getRectangle().getHeight();
		double tileHeight = masterTiles.get(0).getHeight();
		if (pair._1._1 != 1) 
			bMinY = bMinY + (int) tileHeight;
		if (pair._1._1 != nOfKeys && pair._1._1 != 1) {
			bHeight = bHeight - (int) (2 * tileHeight);
		}
		else if (pair._1._1 == 1) {
			int initialBHeight = bHeight;
			bHeight = bHeight - (int) tileHeight;
			if(bHeight == 0) {
				bHeight = initialBHeight;
			}
		}
		
		Rectangle bounds = new Rectangle((int) masterTile.getRectangle().getMinX(), bMinY, (int) masterTile.getRectangle().getWidth(), bHeight);
		System.out.println(devsMSG + "Final bounds:\t" + bounds.width + " X " + bounds.height + "\n\n");
		
		GeoCoding geoCoding=trgImgMetadataGCP.getGeoCoding();
		if(geoCoding == null)
			geoCoding = new TiePointGeoCoding(trgImgMetadataGCP.getLatGrid(),trgImgMetadataGCP.getLonGrid());

		int added = 0;		
		for (int i = 0; i < numberOfMasterGCPs; i++) {
			final Placemark mPin = masterGcpGroup.get(i);
			final PixelPos sGCPPixelPos = new PixelPos(mPin.getPixelPos().x + offset2[0], mPin.getPixelPos().y + offset2[1]);
			if (bounds.contains(new Point((int) sGCPPixelPos.x, (int) sGCPPixelPos.y))) {
				GCPSelection GCPSelection = new GCPSelection(iParams2, dParams3, geoCoding, masterTile, slaveTile);
				try {
					if (GCPSelection.checkMasterGCPValidity(mPin) && GCPSelection.checkSlaveGCPValidity(sGCPPixelPos)) {
						Placemark sPin = GCPSelection.computeSlaveGCP(mPin, sGCPPixelPos);

						if (sPin != null) {
							slaveGCPsRes.add(new Tuple2<String, Tuple2<Integer, Placemark>>(pair._1._2, new Tuple2<Integer, Placemark>(i, sPin)));
							added++;
						}
					}
				}
				catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}
		System.out.println("\n" + devsMSG + added + " of the sPin(s) weren't null. Tuple was added to slaveGCPsRes. We got the PRECIOUSES!\n");
		slaveRaster = null;
		masterRaster = null;
		masterTile = null;
		slaveTile = null;
		return slaveGCPsRes;
	}
}
