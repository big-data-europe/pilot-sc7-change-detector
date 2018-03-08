package eu.bde.sc7pilot.taskbased;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import org.esa.snap.engine_utilities.gpf.OperatorUtils;
import org.esa.snap.engine_utilities.gpf.StackUtils;

import scala.Tuple2;
import eu.bde.sc7pilot.taskbased.calibration.MyCalibration;
import eu.bde.sc7pilot.tilebased.model.MyTile;

public class SerialProcessor {

    public static void main(String[] args) throws IOException {
    	long startAll = System.currentTimeMillis();
//        String filesPath = "/home/efi/SNAP/sentinel-images/";
//        File targetFile = new File(filesPath, "chd-serial-VH");
//        File masterFile = new File(filesPath, "S1A_IW_GRDH_1SSV_20141225T142407_20141225T142436_003877_004A54_040F.zip");
//        File slaveFile = new File(filesPath,  "S1A_IW_GRDH_1SSV_20150518T142409_20150518T142438_005977_007B49_AF76.zip");
    	String imgsDir = args[0];
    	String img1Name = args[1];
    	String img2Name = args[2];
    	String resultPrefName = args[3];
    	// PREPARING FILES
    	File masterFile = new File(imgsDir, img1Name);
    	File slaveFile = new File(imgsDir,  img2Name);
    	File resultFile = new File(imgsDir, resultPrefName);
        SerialProcessor processor = new SerialProcessor();
        processor.processImages(masterFile, slaveFile, resultFile);
        
        long endAll = System.currentTimeMillis();
        long spDuration = endAll - startAll;
        System.out.println(spDuration + "\tms for Serial Processing to run.");
    }

    public void processImages(File masterFile, File slaveFile, File targetFile) throws IOException {
    	
    	long startOperators = System.currentTimeMillis();

        String[] selectedPolarisations = null;
        MyRead myRead1 = new MyRead(masterFile, "read1");
        getBufferedImage(myRead1, selectedPolarisations);

        MyBandSelect bandselect1 = new MyBandSelect(selectedPolarisations, null);
        bandselect1.setSourceProduct(myRead1.getTargetProduct());
        initOperatorForMultipleBands(bandselect1);

        MyRead myRead2 = new MyRead(slaveFile, "read2");
        getBufferedImage(myRead2, selectedPolarisations);

        MyBandSelect bandselect2 = new MyBandSelect(selectedPolarisations, null);
        bandselect2.setSourceProduct(myRead2.getTargetProduct());
        initOperatorForMultipleBands(bandselect2);
        
        long endRead = System.currentTimeMillis();
        long readDuration = endRead - startOperators;
        System.out.println(readDuration + "\tms for Read to run.");

        Boolean[] bParams1 = {false, false, false, false, true, false, false, false};
        MyCalibration myCalibration1 = new MyCalibration(null, bParams1, null);
        myCalibration1.setSourceProduct(bandselect1.getTargetProduct());
        myCalibration1.setId("myCalibration1");
        initOperatorForMultipleBands(myCalibration1);
        myRead1 = null;
        processTiles(myCalibration1);

        MyCalibration myCalibration2 = new MyCalibration(null, bParams1, null);
        myCalibration2.setSourceProduct(bandselect2.getTargetProduct());
        myCalibration2.setId("myCalibration2");
        initOperatorForMultipleBands(myCalibration2);
        myRead2 = null;
        processTiles(myCalibration2);
        
        long endCalib = System.currentTimeMillis();
        long calibDuration = endCalib - endRead;
        System.out.println(calibDuration + "\tms for Calibrate to run.");

        Product[] sourcesForCreateStack = new Product[2];
        sourcesForCreateStack[0] = myCalibration1.getTargetProduct();
        sourcesForCreateStack[1] = myCalibration2.getTargetProduct();

        String[] parameters = {"NONE", "Master", "Orbit"};
        MyCreateStack myCreateStack = new MyCreateStack(parameters);
        myCreateStack.setSourceProduct(sourcesForCreateStack);
        myCreateStack.setId("myCreateStack");
        initOperatorForMultipleBands(myCreateStack);
        myRead1 = null;
        myRead2 = null;
        processTiles(myCreateStack);
        
        long endCS = System.currentTimeMillis();
        long csDuration = endCS - endCalib;
        System.out.println(csDuration + "\tms for CS to run.");

        boolean[] bParams = {false, false, false, false};
        int[] iParams = {1000, 10, 3};
        double[] dParams = {0.25, 0.6};
        String[] sParams = {"128", "128", "4", "4", "32", "32"};
        MyGCPSelection gcpSelectionOp = new MyGCPSelection(bParams, dParams, iParams, sParams);
        gcpSelectionOp.setSourceProduct(myCreateStack.getTargetProduct());
        gcpSelectionOp.setId("gcpSelection");
        initOperatorForMultipleBands(gcpSelectionOp);
        myCreateStack = null;
        processTiles(gcpSelectionOp);
        
        long endGCP = System.currentTimeMillis();
        long gcpDuration = endGCP - endCS;
        System.out.println(gcpDuration + "\tms for GCP to run.");

        boolean[] bParams2 = {false, false};
        MyWarp warpOp = new MyWarp(bParams2, 0.05f, 1, "Bilinear interpolation");
        warpOp.setSourceProduct(gcpSelectionOp.getTargetProduct());
        warpOp.setId("warp");
        initOperatorForMultipleBands(warpOp);
        gcpSelectionOp = null;
        processTiles(warpOp);
        
        long endWarp = System.currentTimeMillis();
        long warpDuration = endWarp - endGCP;
        System.out.println(warpDuration + "\tms for Warp to run.");

        boolean[] bParams3 = {false, false};
        float[] fParams = {2.0f, -2.0f};
        MyChangeDetection myChangeDetection = new MyChangeDetection(bParams3, fParams, null);
        myChangeDetection.setSourceProduct(warpOp.getTargetProduct());
        initOperatorForMultipleBands(myChangeDetection);
        warpOp = null;
        processTiles(myChangeDetection);
        
        long endCD = System.currentTimeMillis();
        long cdDuration = endCD - endWarp;
        System.out.println(cdDuration + "\tms for CD to run.");
        
        MyWrite writeOp = new MyWrite(myChangeDetection.getTargetProduct(), targetFile, "BEAM-DIMAP");
        writeOp.setId("write");
        initOperatorForMultipleBands(writeOp);
        storeResult(writeOp);
        
        long endWrite = System.currentTimeMillis();
        long writeDuration = endWrite - endCD;
        System.out.println(writeDuration + "\tms for Write to run.\n");
    }

    public BufferedImage createSourceImages(Band band) {
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
        SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(dataBufferType, rectangle.width,
                rectangle.height);
        final Point location = new Point(rectangle.x, rectangle.y);
        return Raster.createWritableRaster(sampleModel, sampleModel.createDataBuffer(), location);
    }

    public void getBufferedImage(MyRead myRead) {
        getBufferedImage(myRead, null);
    }

    public void getBufferedImage(MyRead myRead, String[] selectedPolarisations) {
        Product targetProduct = myRead.getTargetProduct();
        LoopLimits limits = new LoopLimits(targetProduct);
        int noOfBands = targetProduct.getNumBands();
        for (int i = 0; i < noOfBands; i++) {
            Band band = targetProduct.getBandAt(i);

            if (selectedPolarisations != null) {
                Set<String> selectedPols = new HashSet(Arrays.asList(Arrays.stream(selectedPolarisations).map(s -> s.toLowerCase()).toArray(String[]::new)));
                String pol = OperatorUtils.getPolarizationFromBandName(band.getName());
                if (!selectedPols.contains(pol.toLowerCase())) {
                    continue;
                }
            }
            if (band.getClass() == Band.class) {
                BufferedImage image = createSourceImages(band);
                for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
                    for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
                        GetTile getTile = new GetTile(band, myRead);
                        Tile tile = getTile.computeTile(tileX, tileY);

                        WritableRaster raster = createWritableRaster(tile.getRectangle(), band);
                        raster.setDataElements(tile.getMinX(), tile.getMinY(), tile.getWidth(), tile.getHeight(),
                                tile.getRawSamples().getElems());
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

    public void processTiles(AbstractOperator operator) {
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
                        img.getRaster().setDataElements(tile.getMinX(), tile.getMinY(), tile.getWidth(),
                                tile.getHeight(), tile.getRawSamples().getElems());
                    }
                }
            }

            if (imgPopulated) {
                band.setSourceImage(img);
            }
        }
    }

    public List<Tuple2<Rectangle, WritableRaster>> readTilesWithRecatngles(MyRead myRead) {
        List<Tuple2<Rectangle, WritableRaster>> rasters = new ArrayList<Tuple2<Rectangle, WritableRaster>>();
        Product targetProduct = myRead.getTargetProduct();
        LoopLimits limits = new LoopLimits(targetProduct);
        int noOfProducts = targetProduct.getNumBands();
        for (int i = 0; i < noOfProducts; i++) {
            Band band = targetProduct.getBandAt(i);
            for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
                for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
                    GetTile getTile = new GetTile(band, myRead);
                    Tile tile = getTile.computeTile(tileX, tileY);
                    WritableRaster raster = Utils.createWritableRaster(tile.getRectangle(), band.getDataType());
                    raster.setDataElements(tile.getMinX(), tile.getMinY(), tile.getWidth(), tile.getHeight(),
                            tile.getRawSamples().getElems());
                    Tuple2<Rectangle, WritableRaster> tuple = new Tuple2<Rectangle, WritableRaster>(tile.getRectangle(),
                            raster);
                    rasters.add(tuple);
                }

            }
        }
        return rasters;
    }

    public List<Tuple2<String, MyTile>> readTiles(MyRead myRead) {
        return readTiles(myRead, null);
    }

    public List<Tuple2<String, MyTile>> readTiles(MyRead myRead, String[] selectedPolarisations) {
        List<Tuple2<String, MyTile>> tiles = new ArrayList<Tuple2<String, MyTile>>();
        Product targetProduct = myRead.getTargetProduct();
        LoopLimits limits = new LoopLimits(targetProduct);
        int noOfProducts = targetProduct.getNumBands();
        for (int i = 0; i < noOfProducts; i++) {
            Band band = targetProduct.getBandAt(i);
            if (selectedPolarisations != null) {
                Set<String> selectedPols = new HashSet(Arrays.asList(Arrays.stream(selectedPolarisations).map(s -> s.toLowerCase()).toArray(String[]::new)));
                String pol = OperatorUtils.getPolarizationFromBandName(band.getName());
                if (!selectedPols.contains(pol.toLowerCase())) {
                    continue;
                }
            }
            for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
                for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
                    if (band.getClass() == Band.class) {
                        GetTile getTile = new GetTile(band, myRead);
                        Tile tile = getTile.computeTile(tileX, tileY);
                        WritableRaster raster = Utils.createWritableRaster(tile.getRectangle(), band.getDataType());
                        raster.setDataElements(tile.getMinX(), tile.getMinY(), tile.getWidth(), tile.getHeight(),
                                tile.getRawSamples().getElems());
                        MyTile myTile = new MyTile(raster, tile.getRectangle(), band.getDataType());
                        // myTile.setRawSamples(tile.getRawSamples());
                        tiles.add(new Tuple2<String, MyTile>(band.getName(), myTile));
                    }
                }
            }

        }
        return tiles;
    }

    public List<Tuple2<String, Point>> readTilesIndices(Product targetProduct, String[] selectedPolarisations) {
        List<Tuple2<String, Point>> indices = new ArrayList<Tuple2<String, Point>>();
        LoopLimits limits = new LoopLimits(targetProduct);
        int noOfProducts = targetProduct.getNumBands();
        for (int i = 0; i < noOfProducts; i++) {
            Band band = targetProduct.getBandAt(i);
            if (selectedPolarisations != null) {
                Set<String> selectedPols = new HashSet(Arrays.asList(Arrays.stream(selectedPolarisations).map(s -> s.toLowerCase()).toArray(String[]::new)));
                String pol = OperatorUtils.getPolarizationFromBandName(band.getName());
                if (!selectedPols.contains(pol.toLowerCase())) {
                    continue;
                }
            }
            for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
                for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
                    if (band.getClass() == Band.class) {
                        indices.add(new Tuple2<String, Point>(band.getName(), new Point(tileX, tileY)));
                    }
                }
            }

        }
        return indices;
    }

    public List<MyTile> readTiles2(MyRead myRead) {
        List<MyTile> tiles = new ArrayList<MyTile>();
        Product targetProduct = myRead.getTargetProduct();
        LoopLimits limits = new LoopLimits(targetProduct);
        int noOfProducts = targetProduct.getNumBands();
        for (int i = 0; i < noOfProducts; i++) {
            Band band = targetProduct.getBandAt(i);
            for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
                for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
                    if (band.getClass() == Band.class) {
                        GetTile getTile = new GetTile(band, myRead);
                        Tile tile = getTile.computeTile(tileX, tileY);
                        WritableRaster raster = Utils.createWritableRaster(tile.getRectangle(), band.getDataType());
                        raster.setDataElements(tile.getMinX(), tile.getMinY(), tile.getWidth(), tile.getHeight(),
                                tile.getRawSamples().getElems());
                        MyTile myTile = new MyTile(raster, tile.getRectangle(), band.getDataType());
                        // myTile.setRawSamples(tile.getRawSamples());
                        tiles.add(myTile);
                    }
                }
            }

        }
        return tiles;
    }

    public void initOperator(AbstractOperator operator) {
        operator.initialize();
        Product targetProduct = operator.getTargetProduct();
        if (targetProduct.getPreferredTileSize() == null) {
            targetProduct.setPreferredTileSize(getPreferredTileSize(targetProduct));
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

    public Map<String, List<Tuple2<Point, Tile>>> processTiles2(AbstractOperator operator) {
        Map<String, List<Tuple2<Point, Tile>>> bandsMap = new HashMap<String, List<Tuple2<Point, Tile>>>();

        System.out.println("!!!!!!! operator: " + operator.getId() + " started");
        Product targetProduct = operator.getTargetProduct();
        LoopLimits limits = new LoopLimits(targetProduct);
        String[] masterBandNames = StackUtils.getMasterBandNames(targetProduct);

        for (int i = 0; i < targetProduct.getNumBands(); i++) {
            boolean isMasterBand = false;
            Band band = operator.getTargetProduct().getBandAt(i);
            for (int j = 0; j < masterBandNames.length; j++) {
                if (band.getName().equals(masterBandNames[j])) {
                    isMasterBand = true;
                }
            }
            if (isMasterBand) {
                continue;
            }

            List<Tuple2<Point, Tile>> tilesList = new ArrayList<Tuple2<Point, Tile>>();
            for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
                for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
                    GetTile getTile = new GetTile(band, operator);
                    Tile tile = getTile.computeTile(tileX, tileY);
                    if (tile == null) {
                        continue;
                    }

                    tilesList.add(new Tuple2<Point, Tile>(new Point(tileX, tileY), tile));
                }
            }
            bandsMap.put(band.getName(), tilesList);
        }
        return bandsMap;

    }
}
