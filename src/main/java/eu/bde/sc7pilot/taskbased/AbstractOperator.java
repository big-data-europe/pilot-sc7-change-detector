package eu.bde.sc7pilot.taskbased;

import java.awt.Rectangle;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.RasterDataNode;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.Tile;
import org.esa.snap.core.gpf.internal.TileImpl;

public abstract class AbstractOperator {

    protected boolean requiresAllBands;

    protected final Map<Band, Band> sourceRasterMap;
    protected final Map<Rectangle, Rectangle> gcpMappings;
    protected final Map<Band, RenderedImage>  originalImages;
    protected final Map<Band, RenderedImage>  targetImages;
    
    protected Product masterProduct;
    protected Product targetProduct;
    protected String id;
    
    public AbstractOperator() {
        sourceRasterMap = new HashMap<>(10);
        gcpMappings = new HashMap<>();
        targetImages = new HashMap<Band, RenderedImage>();
        originalImages = new HashMap<Band, RenderedImage>();
    }
    
    public void addTargetImage(Band band, RenderedImage image) {
        targetImages.put(band, image);
    }
    
    public abstract void initialize();

    public void computeTile(Band targetBand, Tile targetTile) throws OperatorException {
        throw new RuntimeException(
                MessageFormat.format("{0}: ''computeTile()'' method not implemented", getClass().getSimpleName()));
    }

    public void computeTileStack(Map<Band, Tile> targetTiles, Rectangle targetRectangle) throws OperatorException {
        throw new RuntimeException(MessageFormat.format("{0}: ''computeTileStack()'' method not implemented", getClass().getSimpleName()));
    }
    
    public String getId() {
        return id;
    }

    public final Tile getSourceTile(RasterDataNode rasterDataNode, Rectangle region) throws OperatorException {
        GetData gd = new GetData((Band) rasterDataNode);
        Raster tile = gd.getData(region);
        if (tile == null) {
            System.err.println("Null source tile!!!");
            return null;
        }
        return new TileImpl(rasterDataNode, tile);
    }

    public RenderedImage getOriginalImage(Band band) {
        return originalImages.get(band);
    }
    
    public RenderedImage getTargetImage(Band band) {
        return targetImages.get(band);
    }

    public Product getTargetProduct() {
        return targetProduct;
    }

    public boolean requiresAllBands() {
        return requiresAllBands;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setRequiresAllBands(boolean requiresAllBands) {
        this.requiresAllBands = requiresAllBands;
    }

    public Map<Band, Band> getSourceRasterMap() {
        return sourceRasterMap;
    }
}
