package eu.bde.sc7pilot.taskbased;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentSampleModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.MultiPixelPackedSampleModel;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;

import javax.media.jai.PlanarImage;
import javax.media.jai.RasterFactory;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.image.ImageManager;
import org.esa.snap.core.util.ImageUtils;

import com.sun.media.jai.util.ImageUtil;

import eu.bde.sc7pilot.tilebased.model.MyTile;

public class Utils {

    public static WritableRaster createWritableRaster(Rectangle rectangle, int dataType) {
        final int dataBufferType = ImageManager.getDataBufferType(dataType);
        SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(dataBufferType, rectangle.width,
                rectangle.height);
        final Point location = new Point(rectangle.x, rectangle.y);
        return Raster.createWritableRaster(sampleModel, sampleModel.createDataBuffer(), location);
    }

    public static WritableRaster createCompatibleWritableRaster(Rectangle rectangle, SampleModel sampleModel, int dataType) {
        SampleModel sm = sampleModel.createCompatibleSampleModel(rectangle.width,
                rectangle.height);
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
        return new int[]{scanlineStride, scanlineOffset};
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

    public static void setRect(WritableRaster dstRaster, Raster srcRaster) {
        int dx = 0;
        int dy = 0;

        // Special case for bilevel Rasters
        SampleModel srcSampleModel = srcRaster.getSampleModel();
        SampleModel dstSampleModel = dstRaster.getSampleModel();
        if (srcSampleModel instanceof MultiPixelPackedSampleModel
                && dstSampleModel instanceof MultiPixelPackedSampleModel) {
            MultiPixelPackedSampleModel srcMPPSM
                    = (MultiPixelPackedSampleModel) srcSampleModel;
            MultiPixelPackedSampleModel dstMPPSM
                    = (MultiPixelPackedSampleModel) dstSampleModel;

            DataBuffer srcDB = srcRaster.getDataBuffer();
            DataBuffer dstDB = srcRaster.getDataBuffer();

            if (srcDB instanceof DataBufferByte
                    && dstDB instanceof DataBufferByte
                    && srcMPPSM.getPixelBitStride() == 1
                    && dstMPPSM.getPixelBitStride() == 1) {
                if (setRectBilevel(dstRaster, srcRaster, dx, dy)) {
                    return;
                }
            }
        }

        // Use the regular JDK routines for everything else except
        // float and double images.
        int dataType = dstRaster.getSampleModel().getDataType();
        if (dataType != DataBuffer.TYPE_FLOAT
                && dataType != DataBuffer.TYPE_DOUBLE) {
            dstRaster.setRect(dx, dy, srcRaster);
            return;
        }

        int width = srcRaster.getWidth();
        int height = srcRaster.getHeight();
        int srcOffX = srcRaster.getMinX();
        int srcOffY = srcRaster.getMinY();
        int dstOffX = dx + srcOffX;
        int dstOffY = dy + srcOffY;

        int dminX = dstRaster.getMinX();
        int dminY = dstRaster.getMinY();
        int dwidth = dstRaster.getWidth();
        int dheight = dstRaster.getHeight();

        // Clip to dstRaster
        if (dstOffX + width > dminX + dwidth) {
            width = dminX + dwidth - dstOffX;
        }
        if (dstOffY + height > dminY + dheight) {
            height = dminY + dheight - dstOffY;
        }

        switch (srcRaster.getSampleModel().getDataType()) {
            case DataBuffer.TYPE_BYTE:
            case DataBuffer.TYPE_SHORT:
            case DataBuffer.TYPE_USHORT:
            case DataBuffer.TYPE_INT:
                int[] iData = null;
                for (int startY = 0; startY < height; startY++) {
                    // Grab one scanline at a time
                    iData = srcRaster.getPixels(srcOffX, srcOffY + startY, width, 1, iData);
                    dstRaster.setPixels(dstOffX, dstOffY + startY, width, 1, iData);
                }
                break;

            case DataBuffer.TYPE_FLOAT:
                float[] fData = null;
                for (int startY = 0; startY < height; startY++) {
                    fData = srcRaster.getPixels(srcOffX, srcOffY + startY, width, 1, fData);
                    dstRaster.setPixels(dstOffX, dstOffY + startY, width, 1, fData);
                }
                break;

            case DataBuffer.TYPE_DOUBLE:
                double[] dData = null;
                for (int startY = 0; startY < height; startY++) {
                    // Grab one scanline at a time
                    dData = srcRaster.getPixels(srcOffX, srcOffY + startY, width, 1, dData);
                    dstRaster.setPixels(dstOffX, dstOffY + startY, width, 1, dData);
                }
                break;
        }
    }

    public static boolean setRectBilevel(WritableRaster dstRaster,
            Raster srcRaster,
            int dx, int dy) {
        int width = srcRaster.getWidth();
        int height = srcRaster.getHeight();
        int srcOffX = srcRaster.getMinX();
        int srcOffY = srcRaster.getMinY();
        int dstOffX = dx + srcOffX;
        int dstOffY = dy + srcOffY;

        int dminX = dstRaster.getMinX();
        int dminY = dstRaster.getMinY();
        int dwidth = dstRaster.getWidth();
        int dheight = dstRaster.getHeight();

        // Clip to dstRaster
        if (dstOffX + width > dminX + dwidth) {
            width = dminX + dwidth - dstOffX;
        }
        if (dstOffY + height > dminY + dheight) {
            height = dminY + dheight - dstOffY;
        }

        //
        // This implementation works but is not as efficient as the one
        // below which is commented out. In terms of performance, cobbling
        // a 1728x2376 bit image with 128x144 tiles took the following
        // amount of time for four cases:
        //
        // WritableRaster.setRect() 19756
        // Aligned optimal case     5645
        // Unaligned optimal case   6644
        // Case using ImageUtil     7500
        //
        // So this case gives intermediate speed performance closer to the
        // optimal case than to the JDK. It will likely use more memory
        // however. On the other hand this approach covers all data types.
        //
        Rectangle rect = new Rectangle(dstOffX, dstOffY, width, height);
        byte[] binaryData = ImageUtil.getPackedBinaryData(srcRaster, rect);
        ImageUtil.setPackedBinaryData(binaryData, dstRaster, rect);

        return true;
    }
}
