package serialProcessingOriginal;

import java.awt.Dimension;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;

import javax.media.jai.JAI;
import javax.media.jai.RasterFactory;
import javax.media.jai.TileFactory;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.Tile;
import org.esa.snap.core.gpf.internal.OperatorContext;
import org.esa.snap.core.gpf.internal.OperatorImage;
import org.esa.snap.core.gpf.internal.TileImpl;
import org.esa.snap.core.util.ImageUtils;
import org.esa.snap.core.util.jai.JAIUtils;

import com.bc.ceres.core.ProgressMonitor;

/**
 *
 * @author G.A.P. II
 */
public class GetTile {

    /**
     * The X coordinate of tile (0, 0).
     */
    private final int tileGridXOffset = 0;

    /**
     * The Y coordinate of tile (0, 0).
     */
    private final int tileGridYOffset = 0;

    private final int minX = 0;
    private final int minY = 0;
    private final int tileHeight;
    private final int tileWidth;

    private final Band band;
    private final Dimension tileSize; 
    private final OperatorContext operatorContext;
    private TileFactory tileFactory;

    public GetTile(Band band, OperatorContext operatorContext) {
        this.band = band;
        this.operatorContext = operatorContext;

        tileSize = getPreferredTileSize(band.getProduct());
        tileHeight = tileSize.height;
        tileWidth = tileSize.width;

        tileFactory = null;
        if (operatorContext.getRenderingHints() != null) {
            // Set tileFactory if key present.
            if (operatorContext.getRenderingHints().containsKey(JAI.KEY_TILE_FACTORY)) {
                Object factoryValue = operatorContext.getRenderingHints().get(JAI.KEY_TILE_FACTORY);

                // Check the class type in case 'properties' is not
                // an instance of RenderingHints.
                if (factoryValue instanceof TileFactory) {
                    tileFactory = (TileFactory) factoryValue;
                }
            }
        }
    }

    public void computeTile(int tileX, int tileY) {
        Point org = new Point(tileXToX(tileX), tileYToY(tileY));

        int bufferType = getDataBufferType(band.getDataType());
        final SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(bufferType, tileSize.width, tileSize.height);

        final WritableRaster dest = createWritableRaster(sampleModel, org);

        /* Clip output rectangle to image bounds. */
        Rectangle rect = new Rectangle(org.x, org.y, sampleModel.getWidth(), sampleModel.getHeight());
        Rectangle destRect = rect.intersection(getBounds());
        computeRect(dest, destRect);
    }
                
    //implementation of org.esa.snap.framework.gpf.internal.OperationImage
    protected void computeRect(WritableRaster tile, Rectangle destRect) {
        Tile targetTile = null;
        if (isComputingImageOf(band)) {
            targetTile = createTargetTile(band, tile, destRect);
        } else if (operatorContext.requiresAllBands()) {
            targetTile = operatorContext.getSourceTile(band, destRect);
        }

        // computeTile() may have been deactivated
        if (targetTile != null && operatorContext.isComputeTileMethodUsable()) {
            operatorContext.getOperator().computeTile(band, targetTile, ProgressMonitor.NULL);
        }
    }

    protected static TileImpl createTargetTile(Band band, WritableRaster targetTileRaster, Rectangle targetRectangle) {
        return new TileImpl(band, targetTileRaster, targetRectangle, true);
    }

    /**
     * Creates a <code>WritableRaster</code> with the specified
     * <code>SampleModel</code> and location. If <code>tileFactory</code> is
     * non-<code>null</code>, it will be used to create the
     * <code>WritableRaster</code>; otherwise
     * {@link RasterFactory#createWritableRaster(SampleModel,Point)} will be
     * used.
     *
     * @param sampleModel The <code>SampleModel</code> to use.
     * @param location The origin of the <code>WritableRaster</code>; if
     * <code>null</code>, <code>(0,&nbsp;0)</code> will be used.
     * @return
     *
     * @throws IllegalArgumentException if <code>sampleModel</code> is
     * <code>null</code>.
     *
     * @since JAI 1.1.2
     */
    protected final WritableRaster createWritableRaster(SampleModel sampleModel, Point location) {
        if (sampleModel == null) {
            throw new IllegalArgumentException("sampleModel == null!");
        }

        if (tileFactory != null) {
            return tileFactory.createTile(sampleModel, location);
        }

        if (location == null) {
            location = new Point(0, 0);
        }

        return Raster.createWritableRaster(sampleModel, sampleModel.createDataBuffer(), location);
    }

    /**
     * Returns the image's bounds as a <code>Rectangle</code>.
     *
     * <p>
     * The image's bounds are defined by the values returned by
     * <code>getMinX()</code>, <code>getMinY()</code>, <code>getWidth()</code>,
     * and <code>getHeight()</code>. A <code>Rectangle</code> is created based
     * on these four methods and cached in this class. Each time that this
     * method is invoked, the bounds of this <code>Rectangle</code> are updated
     * with the values returned by the four aforementioned accessors.
     *
     * <p>
     * Because this method returns the <code>bounds</code> variable by
     * reference, the caller should not change the settings of the
     * <code>Rectangle</code>. Otherwise, unexpected errors may occur. Likewise,
     * if the caller expects this variable to be immutable it should clone the
     * returned <code>Rectangle</code> if there is any possibility that it might
     * be changed by the <code>PlanarImage</code>. This may generally occur only
     * for instances of <code>RenderedOp</code>.
     */
    protected Rectangle getBounds() {
        Rectangle bounds = new Rectangle();
        bounds.setBounds(minX, minY, band.getProduct().getSceneRasterWidth(), band.getProduct().getSceneRasterHeight());
        return bounds;
    }

    protected static int getDataBufferType(int productDataType) {
        switch (productDataType) {
            case ProductData.TYPE_INT8:
            case ProductData.TYPE_UINT8:
                return DataBuffer.TYPE_BYTE;
            case ProductData.TYPE_INT16:
                return DataBuffer.TYPE_SHORT;
            case ProductData.TYPE_UINT16:
                return DataBuffer.TYPE_USHORT;
            case ProductData.TYPE_INT32:
            case ProductData.TYPE_UINT32:
                return DataBuffer.TYPE_INT;
            case ProductData.TYPE_FLOAT32:
                return DataBuffer.TYPE_FLOAT;
            case ProductData.TYPE_FLOAT64:
                return DataBuffer.TYPE_DOUBLE;
            default:
                throw new IllegalArgumentException("productDataType");
        }
    }

    protected static Dimension getPreferredTileSize(Product product) {
        Dimension tileSize;
        final Dimension preferredTileSize = product.getPreferredTileSize();
        if (preferredTileSize != null) {
            tileSize = preferredTileSize;
        } else {
            tileSize = JAIUtils.computePreferredTileSize(product.getSceneRasterWidth(),
                    product.getSceneRasterHeight(), 1);
        }
        return tileSize;
    }

    boolean isComputingImageOf(Band band) {
        if (band.isSourceImageSet()) {
            RenderedImage sourceImage = band.getSourceImage().getImage(0);
            OperatorImage targetImage = operatorContext.getTargetImage(band);
            //noinspection ObjectEquality
            return targetImage == sourceImage;
        } else {
            return false;
        }
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
}
