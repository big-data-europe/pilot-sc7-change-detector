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

public class MyGCPSelection {

    Operator operator = null;
    OperatorContext operatorContext;
    Product targetProduct;
    final String OPERATOR_NAME = "GCP-Selection";

    public MyGCPSelection() {
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

    public void setParameters(int numGCPtoGenerate, String coarseRegistrationWindowWidth, String coarseRegistrationWindowHeight,
            String rowInterpFactor, String columnInterpFactor, int maxIteration,
            double gcpTolerance, boolean applyFineRegistration, String fineRegistrationWindowWidth, String fineRegistrationWindowHeight,
            int coherenceWindowSize, double coherenceThreshold, Boolean useSlidingWindow, boolean computeOffset, boolean onlyGCPsOnLand) {
        operator.setParameter("numGCPtoGenerate", numGCPtoGenerate);
        operator.setParameter("coarseRegistrationWindowWidth", coarseRegistrationWindowWidth);
        operator.setParameter("coarseRegistrationWindowHeight", coarseRegistrationWindowHeight);
        operator.setParameter("rowInterpFactor", rowInterpFactor);
        operator.setParameter("columnInterpFactor", columnInterpFactor);
        operator.setParameter("maxIteration", maxIteration);
        operator.setParameter("gcpTolerance", gcpTolerance);
        operator.setParameter("applyFineRegistration", applyFineRegistration);
        operator.setParameter("coherenceWindowSize", coherenceWindowSize);
        operator.setParameter("coherenceThreshold", coherenceThreshold);
        operator.setParameter("useSlidingWindow", useSlidingWindow);
        operator.setParameter("computeOffset", computeOffset);
        operator.setParameter("coherenceWindowSize", coherenceWindowSize);
        operator.setParameter("onlyGCPsOnLand", onlyGCPsOnLand);
        createOperatorContext();
        targetProduct = operator.getTargetProduct();
    }

    public void setSourceProduct(Product sourceProduct) {
        operator.setSourceProduct(sourceProduct);
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
        System.out.println("start GCPSelection operator...");
        Dimension tileSize = targetProduct.getPreferredTileSize();
        final int numXTiles = MathUtils.ceilInt(targetProduct.getSceneRasterWidth() / (double) tileSize.width);
        final int numYTiles = MathUtils.ceilInt(targetProduct.getSceneRasterHeight() / (double) tileSize.height);

        for (int tileY = 0; tileY < numYTiles; tileY++) {
            for (int tileX = 0; tileX < numXTiles; tileX++) {
                for (Band band : targetProduct.getBands()) {
                    PlanarImage image = operatorContext.getTargetImage(band);
                    if (image != null) {
                        image.getTile(tileX, tileY);
                        break;
                    }
                }
            }
        }
        System.out.println("tiles from GCPSelection computed successfully!");

        return operator.getTargetProduct();
    }
}
