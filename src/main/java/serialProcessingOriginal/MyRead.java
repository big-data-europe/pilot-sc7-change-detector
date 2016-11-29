package serialProcessingOriginal;

import java.io.File;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.OperatorSpiRegistry;
import org.esa.snap.core.gpf.internal.OperatorContext;
import org.esa.snap.core.gpf.internal.OperatorImage;

public class MyRead extends AbstractOperator {

    public MyRead(File file, OperatorSpiRegistry spiRegistry) {
        super(spiRegistry, "Read");
        
        operator.setParameter("file", file);
        setOperatorContext();
        targetProduct = operator.getTargetProduct();
        limits = new LoopLimits(targetProduct);
    }

    @Override
    public Product computeProduct() {
        for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
            for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
                for (Band band : targetProduct.getBands()) {
                    GetTile getTile = new GetTile(band, operatorContext);
                    OperatorImage image = operatorContext.getTargetImage(band);
                    if (image != null) {
                        getTile.computeTile(tileX, tileY);
//                        image.computeTile(tileX, tileY);
                    } else if (OperatorContext.isRegularBand(band) && band.isSourceImageSet()) {
                        band.getSourceImage().getImage(0).getTile(tileX, tileY);
                    }
                }
            }
        }
        System.out.println("Product loaded successfully!");
        return operator.getTargetProduct();
    }
}
