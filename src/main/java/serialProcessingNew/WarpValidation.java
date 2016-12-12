package serialProcessingNew;

import java.awt.Dimension;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.media.jai.PlanarImage;
import javax.media.jai.RasterFactory;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.RasterDataNode;
import org.esa.snap.core.gpf.Tile;
import org.esa.snap.core.image.ImageManager;
import org.esa.snap.core.util.ImageUtils;
import org.esa.snap.core.util.jai.JAIUtils;
import org.esa.snap.engine_utilities.gpf.StackUtils;
import serialProcessingNew.calibration.MyCalibration;

/**
 *
 * @author G.A.P. II
 */
public class WarpValidation {

    public static void main(String[] args) throws IOException {
//        String filesPath = "E:\\ImageProcessing\\sentinel-images\\sentinel-images-subsets2\\";
//        File targetFile = new File(filesPath, "warpNew");
//        File masterFile = new File(filesPath, "subset3_of_S1A_IW_GRDH_1SSV_20141225T142407_20141225T142436_003877_004A54_040F.dim");
//        File slaveFile = new File(filesPath, "subset3_of_S1A_IW_GRDH_1SSV_20150518T142409_20150518T142438_005977_007B49_AF76.dim");
        //String filesPath = "E:\\ImageProcessing\\sentinel-images\\newsamples\\";
        String filesPath = "/home/efi/SNAP/sentinel-images/";
        File targetFile = new File(filesPath, "serialProcessor");
        File masterFile = new File(filesPath, "subset_0_of_S1A_IW_GRDH_1SDV_20151110T145915_20151110T145940_008544_00C1A6_F175.dim");
        File slaveFile = new File(filesPath, "subset_1_of_S1A_IW_GRDH_1SDV_20151029T145915_20151029T145940_008369_00BD0B_334C.dim");
        
        WarpValidation processor = new WarpValidation();
        processor.processImages(masterFile, slaveFile, targetFile);
    }

    public void processImages(File masterFile, File slaveFile, File targetFile) throws IOException {
        MyRead myRead1 = new MyRead(masterFile, "read1");
        getBufferedImage(myRead1);

        MyRead myRead2 = new MyRead(slaveFile, "read2");
        getBufferedImage(myRead2);

        Boolean[] bParams1 = {false, false, false, false, true, false, false, false};
        MyCalibration myCalibration1 = new MyCalibration(null, bParams1, null);
        myCalibration1.setSourceProduct(myRead1.getTargetProduct());
        myCalibration1.setId("myCalibration1");
        initOperatorForMultipleBands(myCalibration1);
        myRead1 = null;
        processTiles(myCalibration1);

        MyCalibration myCalibration2 = new MyCalibration(null, bParams1, null);
        myCalibration2.setSourceProduct(myRead2.getTargetProduct());
        myCalibration2.setId("myCalibration2");
        initOperatorForMultipleBands(myCalibration2);
        myRead2 = null;
        processTiles(myCalibration2);

        Product[] sourcesForCreateStack = new Product[2];
        sourcesForCreateStack[0] = myCalibration1.getTargetProduct();
        sourcesForCreateStack[1] = myCalibration2.getTargetProduct();

        String[] parameters = {"NONE", "Master", "Orbit"};
        MyCreateStack myCreateStack = new MyCreateStack(parameters);
        myCreateStack.setSourceProduct(sourcesForCreateStack);
        myCreateStack.setId("myCreateStack");
        initOperatorForMultipleBands(myCreateStack);
        myCalibration1 = null;
        myCalibration2 = null;
        processTiles(myCreateStack);

        boolean[] bParams = {false, false, false, false};
        int[] iParams = {2000, 10, 3};
        double[] dParams = {0.25, 0.6};
        String[] sParams = {"128", "128", "4", "4", "32", "32"};
        MyGCPSelection gcpSelectionOp = new MyGCPSelection(bParams, dParams, iParams, sParams);
        gcpSelectionOp.setSourceProduct(myCreateStack.getTargetProduct());
        gcpSelectionOp.setId("gcpSelection");
        initOperatorForMultipleBands(gcpSelectionOp);
        myCreateStack = null;
        processTiles(gcpSelectionOp);

        boolean[] bParams2 = {false, false};
        MyWarp warpOp = new MyWarp(bParams2, 0.05f, 1, "Bilinear interpolation");
        warpOp.setSourceProduct(gcpSelectionOp.getTargetProduct());
        warpOp.setId("warp");
        initOperatorForMultipleBands(warpOp);
        gcpSelectionOp = null;
        processTiles(warpOp);

        MyWrite writeOp = new MyWrite(warpOp.getTargetProduct(), targetFile, "BEAM-DIMAP");
        writeOp.setId("write");
        initOperatorForMultipleBands(writeOp);
        storeResult(writeOp);
    }

    private BufferedImage createSourceImages(Band band) {
        Product product = band.getProduct();
        int width = product.getSceneRasterWidth();
        int height = product.getSceneRasterHeight();
        int bufferType = ImageManager.getDataBufferType(band.getDataType());
        final SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(bufferType, width, height);
        final ColorModel cm = PlanarImage.createColorModel(sampleModel);
        WritableRaster raster = RasterFactory.createWritableRaster(sampleModel, new Point(0, 0));
        return new BufferedImage(cm, raster, false, new java.util.Hashtable());
    }

    private WritableRaster createWritableRaster(Rectangle rectangle, RasterDataNode band) {
        final int dataBufferType = ImageManager.getDataBufferType(band.getDataType());
        SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(dataBufferType, rectangle.width, rectangle.height);
        final Point location = new Point(rectangle.x, rectangle.y);
        return Raster.createWritableRaster(sampleModel, sampleModel.createDataBuffer(), location);
    }

    private void getBufferedImage(MyRead myRead) {
        Product targetProduct = myRead.getTargetProduct();
        LoopLimits limits = new LoopLimits(targetProduct);
        int noOfProducts = targetProduct.getNumBands();
        for (int i = 0; i < noOfProducts; i++) {
            Band band = targetProduct.getBandAt(i);
            if (band.getClass() == Band.class) {
                BufferedImage image = createSourceImages(band);
                for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
                    for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
                        GetTile getTile = new GetTile(band, myRead);
                        Tile tile = getTile.computeTile(tileX, tileY);

                        WritableRaster raster = createWritableRaster(tile.getRectangle(), band);
                        raster.setDataElements(tile.getMinX(), tile.getMinY(), tile.getWidth(), tile.getHeight(), tile.getRawSamples().getElems());
                        image.setData(raster);
                    }
                }
                band.setSourceImage(image);
                myRead.addTargetImage(band, image);
            }
        }
    }

    private Dimension getPreferredTileSize(Product product) {
        return JAIUtils.computePreferredTileSize(product.getSceneRasterWidth(), product.getSceneRasterHeight(), 4);
    }

    public void initOperatorForMultipleBands(AbstractOperator operator) {
        operator.initialize();
        Product targetProduct = operator.getTargetProduct();
        if (targetProduct.getPreferredTileSize() == null) {
            targetProduct.setPreferredTileSize(getPreferredTileSize(targetProduct));
        }

        for (Band band : targetProduct.getBands()) {
            if (band.isSourceImageSet()) {
                operator.addTargetImage(band, null);
            } else {
                band.setSourceImage(operator.getOriginalImage(band));
                operator.addTargetImage(band, band.getSourceImage().getImage(0));
            }
        }
    }

    private void processTiles(AbstractOperator operator) {
        Product targetProduct = operator.getTargetProduct();
        LoopLimits limits = new LoopLimits(targetProduct);
        String[] masterBandNames = StackUtils.getMasterBandNames(targetProduct);
        Set<String> masterBands = new HashSet(Arrays.asList(masterBandNames));
        for (int i = 0; i < targetProduct.getNumBands(); i++) {
            Band band = operator.getTargetProduct().getBandAt(i);
            if (masterBands.contains(band.getName())) {
                continue;
            }
            
            boolean imgPopulated = false;
            BufferedImage img = createSourceImages(band);
            for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
                for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
                    GetTile getTile = new GetTile(band, operator);
                    Tile tile = getTile.computeTile(tileX, tileY);
                    if (tile != null && getTile.isComputingImageOf(band)) {
                        imgPopulated = true;
                        img.getRaster().setDataElements(tile.getMinX(), tile.getMinY(), tile.getWidth(), tile.getHeight(), tile.getRawSamples().getElems());
                    }
                }
            }
            
            if (imgPopulated) {
                band.setSourceImage(img);
            }
        }
    }

    private void storeResult(AbstractOperator operator) {
        Product targetProduct = operator.getTargetProduct();
        LoopLimits limits = new LoopLimits(targetProduct);
        int noOfBands = targetProduct.getNumBands();
        for (int i = 0; i < noOfBands; i++) {
            Band band = targetProduct.getBandAt(i);
            for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
                for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
                    if (band.getClass() == Band.class && band.isSourceImageSet()) {
                        GetTile getTile = new GetTile(band, operator);
                        getTile.computeTile(tileX, tileY);
                    }
                }
            }
        }
    }
}
