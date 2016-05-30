package eu.bde.sc7pilot.taskbased;

import com.bc.ceres.core.ProgressMonitor;
import java.awt.Dimension;
import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.Tile;
import org.esa.snap.core.util.jai.JAIUtils;

public class MyRead extends AbstractOperator {

    private File file;
    private String formatName;
    private transient ProductReader productReader;
    public MyRead(){
    
    }
    public MyRead(File fl, String id) throws IOException {
        super();
        setId(id);
        file = fl;
        if (file == null) {
            throw new OperatorException("'file' parameter must be set");
        }

        productReader = ProductIO.getProductReaderForInput(file);
        if (productReader == null) {
            throw new OperatorException("No product reader found for file " + file);
        }

        targetProduct = productReader.readProductNodes(file, null);
        targetProduct.setFileLocation(file);
        
        Dimension tileSize = JAIUtils.computePreferredTileSize(targetProduct.getSceneRasterWidth(), targetProduct.getSceneRasterHeight(), 4);
        targetProduct.setPreferredTileSize(tileSize);
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

    public File getFile() {
        return file;
    }

    public String getFormatName() {
        return formatName;
    }

    public void setFile(File file) {
        this.file = file;
    }
    
    public void setFormatName(String formatName) {
        this.formatName = formatName;
    }

    @Override
    public void initialize() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
