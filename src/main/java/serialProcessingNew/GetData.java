package serialProcessingNew;

import static java.awt.image.Raster.createWritableRaster;

import java.awt.Dimension;
import java.awt.Rectangle;
import java.awt.image.ComponentSampleModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.MultiPixelPackedSampleModel;
import java.awt.image.PixelInterleavedSampleModel;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.awt.image.WritableRenderedImage;

import javax.media.jai.ComponentSampleModelJAI;
import javax.media.jai.TiledImage;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.jai.JAIUtils;

import com.sun.media.jai.util.ImageUtil;

/**
 *
 * @author G.A.P. II
 */
public class GetData {

    private final int minX = 0;
    private final int minY = 0;

    /**
     * The X coordinate of tile (0, 0).
     */
    private final int tileGridXOffset = 0;

    /**
     * The Y coordinate of tile (0, 0).
     */
    private final int tileGridYOffset = 0;
    private final int tileHeight;
    private final int tileWidth;

    private final RenderedImage sourceImage;
    private final Rectangle bounds;
    protected SampleModel sampleModel;

    public GetData(Band band) {
        sourceImage = (TiledImage) band.getSourceImage().getImage(0);
        
        Dimension tileSize = getPreferredTileSize(band.getProduct());
        tileHeight = tileSize.height;
        tileWidth = tileSize.width;
        sampleModel = sourceImage.getSampleModel();

        bounds = new Rectangle();
        bounds.setBounds(minX, minY, sourceImage.getWidth(), sourceImage.getHeight());
    }

    private Raster getTile(int x, int y) {
        try {
            Rectangle rect = getTileRect(x, y);
            return sourceImage.getData(rect);
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    // used in GetTile2, too
    protected static Dimension getPreferredTileSize(Product product) {
        Dimension tileSize;
        final Dimension preferredTileSize = product.getPreferredTileSize();
        if (preferredTileSize != null) {
            tileSize = preferredTileSize;
        } else {
            tileSize = JAIUtils.computePreferredTileSize(product.getSceneRasterWidth(), product.getSceneRasterHeight(), 4);
        }
        return tileSize;
    }

    /**
     * Faster implementation of setRect for bilevel Rasters with a
     * SinglePixelPackedSampleModel and DataBufferByte. Based on
     * sun.awt.image.BytePackedRaster.setDataElements (JDK1.3 beta version),
     * with improvements.
     */
    private static boolean setRectBilevel(WritableRaster dstRaster,
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

    private static void setRect(WritableRaster dstRaster, Raster srcRaster) {
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

    public static SampleModel createPixelInterleavedSampleModel(int dataType,
            int width, int height,
            int pixelStride,
            int scanlineStride,
            int bandOffsets[]) {
        if (bandOffsets == null) {
            throw new IllegalArgumentException("RasterFactory4");
        }
        int minBandOff = bandOffsets[0];
        int maxBandOff = bandOffsets[0];
        for (int i = 1; i < bandOffsets.length; i++) {
            minBandOff = Math.min(minBandOff, bandOffsets[i]);
            maxBandOff = Math.max(maxBandOff, bandOffsets[i]);
        }
        maxBandOff -= minBandOff;
        if (maxBandOff > scanlineStride) {
            throw new IllegalArgumentException("RasterFactory5");

        }
        if (pixelStride * width > scanlineStride) {
            throw new IllegalArgumentException("RasterFactory6");
        }
        if (pixelStride < maxBandOff) {
            throw new IllegalArgumentException("RasterFactory7");
        }

        switch (dataType) {
            case DataBuffer.TYPE_BYTE:
            case DataBuffer.TYPE_USHORT:
                return new PixelInterleavedSampleModel(dataType,
                        width, height,
                        pixelStride,
                        scanlineStride,
                        bandOffsets);
            case DataBuffer.TYPE_INT:
            case DataBuffer.TYPE_SHORT:
            case DataBuffer.TYPE_FLOAT:
            case DataBuffer.TYPE_DOUBLE:
                return new ComponentSampleModelJAI(dataType,
                        width, height,
                        pixelStride,
                        scanlineStride,
                        bandOffsets);
            default:
                throw new IllegalArgumentException("RasterFactory3");
        }
    }

    protected Rectangle getBounds() {
        return bounds;
    }

    /**
     * Returns a <code>Rectangle</code> indicating the active area of a given
     * tile. The <code>Rectangle</code> is defined as the intersection of the
     * tile area and the image bounds. No attempt is made to detect out-of-range
     * indices; tile indices lying completely outside of the image will result
     * in returning an empty <code>Rectangle</code> (width and/or height less
     * than or equal to 0).
     *
     * <p>
     * This method is implemented in terms of the primitive layout accessors and
     * so does not need to be implemented by subclasses.
     *
     * @param tileX The X index of the tile.
     * @param tileY The Y index of the tile.
     *
     * @return A <code>Rectangle</code>
     */
    public Rectangle getTileRect(int tileX, int tileY) {
        return getBounds().intersection(new Rectangle(
                tileXToX(tileX), tileYToY(tileY),
                tileWidth, tileHeight));
    }

    /**
     * Converts a horizontal tile index into the X coordinate of its upper left
     * pixel relative to a given tile grid layout specified by its X offset and
     * tile width.
     */
    protected int tileXToX(int tx) {
        return tx * tileWidth + tileGridXOffset;
    }

    /**
     * Converts a vertical tile index into the Y coordinate of its upper left
     * pixel relative to a given tile grid layout specified by its Y offset and
     * tile height.
     */
    protected int tileYToY(int ty) {
        return ty * tileHeight + tileGridYOffset;
    }

    public int XToTileX(int x) {
        x -= tileGridXOffset;
        if (x < 0) {
            x += 1 - tileWidth;		// force round to -infinity (ceiling)
        }
        return x / tileWidth;
    }

    public int YToTileY(int y) {
        y -= tileGridYOffset;
        if (y < 0) {
            y += 1 - tileHeight;	 // force round to -infinity (ceiling)
        }
        return y / tileHeight;
    }

    public Raster getData(Rectangle region) {
        Rectangle b = getBounds();	// image's bounds

        if (!region.intersects(b)) {
            throw new IllegalArgumentException("PlanarImage4");
        }

        // Get the intersection of the region and the image bounds.
        Rectangle xsect = region == b ? region : region.intersection(b);

        // Compute tile indices over the intersection.
        int startTileX = XToTileX(xsect.x);
        int startTileY = YToTileY(xsect.y);
        int endTileX = XToTileX(xsect.x + xsect.width - 1);
        int endTileY = YToTileY(xsect.y + xsect.height - 1);

        if (startTileX == endTileX && startTileY == endTileY
                && getTileRect(startTileX, startTileY).contains(region)) {
            // Requested region is within a single tile.
            Raster tile = getTile(startTileX, startTileY);
            if (tile == null) {
                return null;
            }

            if (this instanceof WritableRenderedImage) {
                // Returned Raster must not change if the corresponding
                // image data are modified so if this image is mutable
                // a copy must be created.
                SampleModel sm = tile.getSampleModel();
                if (sm.getWidth() != region.width
                        || sm.getHeight() != region.height) {
                    sm = sm.createCompatibleSampleModel(region.width,
                            region.height);
                }
                WritableRaster destinationRaster
                        = createWritableRaster(sm, region.getLocation());
                Raster sourceRaster
                        = tile.getBounds().equals(region)
                                ? tile : tile.createChild(region.x, region.y,
                                        region.width, region.height,
                                        region.x, region.y,
                                        null);
                setRect(destinationRaster, sourceRaster);
                return destinationRaster;
            } else {
                // Image is immutable so returning the tile or a child
                // thereof is acceptable.
                return tile.getBounds().equals(region)
                        ? tile : tile.createChild(region.x, region.y,
                                region.width, region.height,
                                region.x, region.y,
                                null);
            }
        } else {
            // Extract a region crossing tiles into a new WritableRaster
            WritableRaster dstRaster;
            SampleModel srcSM = sampleModel;
            int nbands = srcSM.getNumBands();
            boolean isBandChild = false;
            ComponentSampleModel csm = null;
            if (srcSM instanceof ComponentSampleModel) {
                csm = (ComponentSampleModel) srcSM;
                int ps = csm.getPixelStride();
                isBandChild = (ps > 1 && nbands != ps);
            }

            SampleModel sm = sampleModel;
            if (sm.getWidth() != region.width
                    || sm.getHeight() != region.height) {
                sm = sm.createCompatibleSampleModel(region.width,
                        region.height);
            }

            try {
                dstRaster = createWritableRaster(sm, region.getLocation());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("PlanarImage2");
            }

            for (int j = startTileY; j <= endTileY; j++) {
                for (int i = startTileX; i <= endTileX; i++) {
                    Raster tile = getTile(i, j);

                    Rectangle subRegion = region.intersection(
                            tile.getBounds());
                    Raster subRaster
                            = tile.createChild(subRegion.x,
                                    subRegion.y,
                                    subRegion.width,
                                    subRegion.height,
                                    subRegion.x,
                                    subRegion.y,
                                    null);

                    if (sm instanceof ComponentSampleModel
                            && isBandChild) {
                            // Need to handle this case specially, since
                        // setDataElements will not copy band child images
                        switch (sm.getDataType()) {
                            case DataBuffer.TYPE_FLOAT:
                                dstRaster.setPixels(
                                        subRegion.x,
                                        subRegion.y,
                                        subRegion.width,
                                        subRegion.height,
                                        subRaster.getPixels(
                                                subRegion.x,
                                                subRegion.y,
                                                subRegion.width,
                                                subRegion.height,
                                                new float[nbands * subRegion.width * subRegion.height]));
                                break;
                            case DataBuffer.TYPE_DOUBLE:
                                dstRaster.setPixels(
                                        subRegion.x,
                                        subRegion.y,
                                        subRegion.width,
                                        subRegion.height,
                                        subRaster.getPixels(
                                                subRegion.x,
                                                subRegion.y,
                                                subRegion.width,
                                                subRegion.height,
                                                new double[nbands * subRegion.width * subRegion.height]));
                                break;
                            default:
                                dstRaster.setPixels(
                                        subRegion.x,
                                        subRegion.y,
                                        subRegion.width,
                                        subRegion.height,
                                        subRaster.getPixels(
                                                subRegion.x,
                                                subRegion.y,
                                                subRegion.width,
                                                subRegion.height,
                                                new int[nbands * subRegion.width * subRegion.height]));
                                break;
                        }
                    } else {
                        setRect(dstRaster, subRaster);
                    }

                }
            }

            return dstRaster;
        }
    }
}