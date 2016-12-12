package serialProcessingNew;

import java.awt.Dimension;
import java.awt.Rectangle;
import java.io.IOException;

import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.Tile;
import org.esa.snap.core.util.jai.JAIUtils;

import com.bc.ceres.core.ProgressMonitor;

public class MySubset extends AbstractOperator {
	
	private transient ProductReader productReader;
	
	public MySubset(){
	    
    }
	
    public MySubset(Product prod,String polygon,String id) throws IOException {
        
    	super();
        setId(id);
        
        targetProduct = SnapS1Toolbox.subsetOperator(prod, polygon);
        productReader = targetProduct.getProductReader();
        if (productReader == null) {
            throw new OperatorException("Error:No product reader found for product");
        }
       
        Dimension tileSize = JAIUtils.computePreferredTileSize(targetProduct.getSceneRasterWidth(), targetProduct.getSceneRasterHeight(), 4);
        targetProduct.setPreferredTileSize(tileSize);
    }

	@Override
	public void initialize() {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
	
	@Override
    public void computeTile(Band band, Tile targetTile) throws OperatorException {
        ProductData dataBuffer = targetTile.getRawSamples();
        Rectangle rectangle = targetTile.getRectangle();
        try {
            productReader.readBandRasterData(band, rectangle.x, rectangle.y, rectangle.width, rectangle.height, dataBuffer, ProgressMonitor.NULL);
            targetTile.setRawSamples(dataBuffer);
        } catch (IOException e) {
            throw new OperatorException(e);
        }
    }
	
	

}
