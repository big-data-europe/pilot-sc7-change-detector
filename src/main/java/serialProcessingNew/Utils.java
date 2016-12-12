package serialProcessingNew;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentSampleModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;

import javax.media.jai.PlanarImage;
import javax.media.jai.RasterFactory;
import javax.media.jai.TiledImage;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.image.ImageManager;
import org.esa.snap.core.util.ImageUtils;

import tileBased.model.MyTile;

public class Utils {
	public static WritableRaster createWritableRaster(Rectangle rectangle, int dataType) {
		final int dataBufferType = ImageManager.getDataBufferType(dataType);
		SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(dataBufferType, rectangle.width,
				rectangle.height);
		final Point location = new Point(rectangle.x, rectangle.y);
		return Raster.createWritableRaster(sampleModel, sampleModel.createDataBuffer(), location);
	}

	public static WritableRaster createCompatibleWritableRaster(Rectangle rectangle, SampleModel sampleModel,
			int dataType) {
		SampleModel sm = sampleModel.createCompatibleSampleModel(rectangle.width, rectangle.height);
		final Point location = new Point(rectangle.x, rectangle.y);
		return Raster.createWritableRaster(sm, location);
	}

	public static int[] computeStrideAndOffset(Rectangle rectangle, WritableRaster raster) {
		ComponentSampleModel sm = (ComponentSampleModel) raster.getSampleModel();
		int smX0 = rectangle.x - raster.getSampleModelTranslateX();
		int smY0 = rectangle.y - raster.getSampleModelTranslateY();
		int dbI0 = raster.getDataBuffer().getOffset();
		int scanlineStride = sm.getScanlineStride();
		int scanlineOffset = smY0 * scanlineStride + smX0 + dbI0;
		return new int[] { scanlineStride, scanlineOffset };
	}

	public static MyTile getSubTile(MyTile myTile, Rectangle rect) {
		WritableRaster tileRaster = myTile.getWritableRaster();
		WritableRaster dest = tileRaster.createCompatibleWritableRaster(rect);
		Object primitiveArray = ImageUtils.getPrimitiveArray(tileRaster.getDataBuffer());
		myTile.getWritableRaster().getDataElements(rect.x, rect.y, primitiveArray);
		dest.setDataElements(rect.x, rect.y, rect.width, rect.height, primitiveArray);
		MyTile subTile = new MyTile(dest, rect, myTile.getType());
		return subTile;
	}

	// public static MyProductData createInstance(int type, int numElems) {
	// if (numElems < 1 && type != ProductData.TYPE_UTC) {
	// throw new IllegalArgumentException("numElems is less than one");
	// }
	// switch (type) {
	// case ProductData.TYPE_INT8:
	// return new MyProductData.Byte(numElems);
	// case ProductData.TYPE_INT16:
	// return new MyProductData.Short(numElems);
	// case ProductData.TYPE_INT32:
	// return new MyProductData.Int(numElems);
	// case ProductData.TYPE_UINT8:
	// return new MyProductData.UByte(numElems);
	// case ProductData.TYPE_UINT16:
	// return new MyProductData.UShort(numElems);
	// case ProductData.TYPE_UINT32:
	// return new MyProductData.UInt(numElems);
	// case ProductData.TYPE_FLOAT32:
	// return new MyProductData.Float(numElems);
	// case ProductData.TYPE_FLOAT64:
	// return new MyProductData.Double(numElems);
	// case ProductData.TYPE_ASCII:
	// return new MyProductData.ASCII(numElems);
	// case ProductData.TYPE_UTC:
	// return new MyProductData.UTC();
	// default:
	// return null;
	// }
	// }
	public static BufferedImage createBufferImage(int width, int height, int bufferType) {
		final SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(bufferType, width, height);
		final ColorModel cm = PlanarImage.createColorModel(sampleModel);
		WritableRaster raster = RasterFactory.createWritableRaster(sampleModel, new Point(0, 0));
		return new BufferedImage(cm, raster, false, new java.util.Hashtable());
	}

	public static BufferedImage createSourceImages(Band band) {
		Product product = band.getProduct();
		int width = product.getSceneRasterWidth();
		int height = product.getSceneRasterHeight();
		int bufferType = ImageManager.getDataBufferType(band.getDataType());
		final SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(bufferType, width, height);
		final ColorModel cm = PlanarImage.createColorModel(sampleModel);
		WritableRaster raster = RasterFactory.createWritableRaster(sampleModel, new Point(0, 0));
		return new BufferedImage(cm, raster, false, new java.util.Hashtable());
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
