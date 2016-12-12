package serialProcessingOriginal;

import java.awt.Dimension;
import java.lang.reflect.Field;
import javax.media.jai.PlanarImage;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.gpf.Operator;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.OperatorSpi;
import org.esa.snap.core.gpf.OperatorSpiRegistry;
import org.esa.snap.core.gpf.internal.OperatorContext;
import org.esa.snap.core.util.math.MathUtils;

public class MyWarp {

    Operator operator = null;
    OperatorContext operatorContext;
    Product targetProduct;
    final String OPERATOR_NAME = "Warp";

    public MyWarp() {
        final OperatorSpiRegistry spiRegistry = GPF.getDefaultInstance().getOperatorSpiRegistry();
        OperatorSpi operatorSpi = spiRegistry.getOperatorSpi(OPERATOR_NAME);
        if (operatorSpi == null) {
            System.out.println("SPI not found for operator '" + OPERATOR_NAME + "'");
        }

        try {
            operator = operatorSpi.createOperator();
        } catch (OperatorException e) {
            System.out.println("Failed to create instance of operator '" + OPERATOR_NAME + "'");
        }
    }

    public void setSourceProduct(Product sourceProduct) {
        operator.setSourceProduct(sourceProduct);
    }

    public void setParameters(float rmsThreshold, int warpPolynomialOrder, String interpolationMethod,
            boolean excludeMaster, Boolean openResidualsFile) {
        operator.setParameter("rmsThreshold", rmsThreshold);
        operator.setParameter("warpPolynomialOrder", warpPolynomialOrder);
        operator.setParameter("interpolationMethod", interpolationMethod);
        operator.setParameter("excludeMaster", excludeMaster);
        operator.setParameter("openResidualsFile", openResidualsFile);
        createOperatorContext();
        targetProduct = operator.getTargetProduct();
    }

    private void createOperatorContext() {
        try {
            Field field = Operator.class.getDeclaredField("context");
            field.setAccessible(true);
            operatorContext = (OperatorContext) field.get(operator);
            operatorContext.setId("1");
            field.setAccessible(false);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
    }

    public Product computeProduct() {
        System.out.println("start warp operator...");
        final Dimension tileSize = targetProduct.getPreferredTileSize();
        final int numXTiles = MathUtils.ceilInt(targetProduct.getSceneRasterWidth() / (double) tileSize.width);
        final int numYTiles = MathUtils.ceilInt(targetProduct.getSceneRasterHeight() / (double) tileSize.height);

        for (int tileY = 0; tileY < numYTiles; tileY++) {
            for (int tileX = 0; tileX < numXTiles; tileX++) {
                for (Band band : targetProduct.getBands()) {
                    PlanarImage image = operatorContext.getTargetImage(band);
                    if (image != null) {
                        image.getTile(tileX, tileY);
                    } else if (OperatorContext.isRegularBand(band) && band.isSourceImageSet()) {
                        band.getSourceImage().getTile(tileX, tileY);
                    }
                }
            }
        }
        System.out.println("tiles from warp computed successfully!");

        return operator.getTargetProduct();
    }
}
