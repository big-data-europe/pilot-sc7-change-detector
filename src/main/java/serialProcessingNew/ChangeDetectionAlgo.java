package serialProcessingNew;

import java.awt.Rectangle;
import java.awt.geom.Point2D;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;

import javax.media.jai.TiledImage;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;

public class ChangeDetectionAlgo {

	public static void main(String[] args) throws IOException {
		String filesPath = "/home/efi/SNAP/sentinel-images/";
		File inputFile=new File(filesPath,"changeD-subs3.dim");
		ChangeDetectionAlgo chalgo=new ChangeDetectionAlgo();
		chalgo.clusterChanges(inputFile);
	}
	public void clusterChanges(File inputFile) throws IOException {
		
		MyRead myRead = new MyRead(inputFile, "read");
		SerialProcessor sp=new SerialProcessor();
		sp.getBufferedImage(myRead,null);
		Band targetBand= myRead.getTargetProduct().getBandAt(0);
		TiledImage inputImg=(TiledImage)targetBand.getSourceImage().getImage(0);
		GeoCoding geoc =targetBand.getGeoCoding();
		Raster raster = inputImg.getData(new Rectangle(0, 0, 3, 3));
	}
	
	
	public void changesToCoords(RenderedImage img, GeoCoding geoc, int window, double changeThres) {
		int width = img.getWidth();
		int height = img.getHeight();
		int x = 0;
		int y = 0;
		while (y < height - window) {
			while (x <= width - window) {
				Raster raster = img.getData(new Rectangle(x, y, window, window));
				if (checkChange(raster, changeThres) > 4)
					getCoordinates(geoc,x,y,window);
					x += window;
			}
			y += window;
		}

		GeoPos pos = geoc.getGeoPos(new PixelPos(0, 0), null);
	}

	private void getCoordinates(GeoCoding geoc,int x,int y,int window) {
		for(int i=0;i<window;i++){
			for(int j=0;j<window;j++){
				GeoPos gpos=geoc.getGeoPos(new PixelPos(x, y), null);
				Point2D p=new Point2D.Double(gpos.getLon(),gpos.getLat());
				System.out.println(p);
			}
		}

	}

	private int checkChange(Raster raster, double changeThres) {
		int width = raster.getWidth();
		int height = raster.getHeight();
		double[] dArray = new double[1];
		int changed = 0;
		for (int i = 0; i < width; i++) {
			for (int j = 0; j < height; j++) {
				raster.getPixel(i, j, dArray);
				if (dArray[0] > changeThres || dArray[0] < -changeThres)
					changed++;
			}
		}
		return changed;

	}
}
