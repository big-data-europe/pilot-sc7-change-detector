package serialProcessingNew;

import java.awt.Dimension;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.math.MathUtils;

/**
 *
 * @author G.A.P. II
 */

public class LoopLimits {
    
    private final int numXTiles;
    private final int numYTiles;
    
    public LoopLimits(Product targetProduct) {
        final Dimension tileSize = targetProduct.getPreferredTileSize();
        numXTiles = MathUtils.ceilInt(targetProduct.getSceneRasterWidth() / (double) tileSize.width);
        numYTiles = MathUtils.ceilInt(targetProduct.getSceneRasterHeight() / (double) tileSize.height);
    }

    public int getNumXTiles() {
        return numXTiles;
    }

    public int getNumYTiles() {
        return numYTiles;
    }
}