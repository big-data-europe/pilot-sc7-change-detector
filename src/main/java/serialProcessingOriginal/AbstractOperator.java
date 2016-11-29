package serialProcessingOriginal;

import java.lang.reflect.Field;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.Operator;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.OperatorSpi;
import org.esa.snap.core.gpf.OperatorSpiRegistry;
import org.esa.snap.core.gpf.internal.OperatorContext;
import org.esa.snap.core.gpf.internal.OperatorImage;

/**
 *
 * @author G.A.P. II
 */

public abstract class AbstractOperator {
 
    protected LoopLimits limits;
    protected Product targetProduct;
    protected Operator operator;
    protected OperatorContext operatorContext;
    protected final String OPERATOR_NAME;
    
    public AbstractOperator (OperatorSpiRegistry spiRegistry, String name) {
        OPERATOR_NAME = name;
        
        OperatorSpi operatorSpi = spiRegistry.getOperatorSpi(OPERATOR_NAME);
        if (operatorSpi == null) {
            System.out.println("SPI not found for operator '" + OPERATOR_NAME + "'");
            System.exit(-1);
        }
        
        try {
            operator = operatorSpi.createOperator();
        } catch (OperatorException e) {
            System.out.println("Failed to create instance of operator '" + OPERATOR_NAME + "'");
            e.printStackTrace();
        }
    }

    public Product computeProduct() {
        for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
            for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
                for (Band band : targetProduct.getBands()) {
                    GetTile getTile = new GetTile(band, operatorContext);
                    OperatorImage image = operatorContext.getTargetImage(band);
                    if (image != null) {
                        getTile.computeTile(tileX, tileY);
                    } else if (OperatorContext.isRegularBand(band) && band.isSourceImageSet()) {
                        band.getSourceImage().getImage(0).getTile(tileX, tileY);
                    }
                }
            }
        }
        System.out.println("Product loaded successfully!");
        return operator.getTargetProduct();
    }
    
    protected void setOperatorContext() {
        try { //creating operator context
            Field field = Operator.class.getDeclaredField("context");
            field.setAccessible(true);
            operatorContext = (OperatorContext) field.get(operator);
            operatorContext.setId("1");
            field.setAccessible(false);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
    }
}