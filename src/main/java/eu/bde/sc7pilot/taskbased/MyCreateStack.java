package eu.bde.sc7pilot.taskbased;

import java.awt.Rectangle;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.esa.s1tbx.insar.gpf.coregistration.GCPManager;
import org.esa.snap.core.dataio.ProductSubsetBuilder;
import org.esa.snap.core.dataio.ProductSubsetDef;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.MetadataAttribute;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Placemark;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.ProductNodeGroup;
import org.esa.snap.core.datamodel.TiePointGeoCoding;
import org.esa.snap.core.datamodel.TiePointGrid;
import org.esa.snap.core.datamodel.VirtualBand;
import org.esa.snap.core.dataop.resamp.Resampling;
import org.esa.snap.core.dataop.resamp.ResamplingFactory;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.Tile;
import org.esa.snap.core.util.FeatureUtils;
import org.esa.snap.core.util.ProductUtils;
import org.esa.snap.engine_utilities.datamodel.AbstractMetadata;
import org.esa.snap.engine_utilities.datamodel.ProductInformation;
import org.esa.snap.engine_utilities.datamodel.Unit;
import org.esa.snap.engine_utilities.gpf.OperatorUtils;
import org.esa.snap.engine_utilities.gpf.StackUtils;
import org.esa.snap.engine_utilities.gpf.TileIndex;
import org.jlinda.core.Orbit;
import org.jlinda.core.Point;
import org.jlinda.core.SLCImage;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class MyCreateStack extends AbstractOperator {

    public final static String INITIAL_OFFSET_GEOLOCATION = "Product Geolocation";
    public final static String INITIAL_OFFSET_ORBIT = "Orbit";
    public final static String MASTER_EXTENT = "Master";
    public final static String MAX_EXTENT = "Maximum";
    public final static String MIN_EXTENT = "Minimum";

    private boolean appendToMaster;
    private boolean productPixelSpacingChecked;

    private String extent;
    private String initialOffsetMethod;
    private String resamplingType;
    private Resampling selectedResampling;

    private final Band[] masterBands;
    private Product[] sourceProduct;
    private String[] masterBandNames;
    private final Map<Product, int[]> slaveOffsettMap;
    
    public MyCreateStack(String[] parameters) {
        super();
        appendToMaster = false;
        productPixelSpacingChecked = false;
        masterBands = new Band[2];
        resamplingType = parameters[0];
        extent = parameters[1];
        initialOffsetMethod = parameters[2];
        slaveOffsettMap = new HashMap<>(10);
    }

    private void addOffset(final Product slvProd, final int offsetX, final int offsetY) {
        getSlaveOffsettMap().put(slvProd, new int[]{offsetX, offsetY});
    }

    public static void checkPixelSpacing(final Product[] sourceProducts)
            throws Exception {
        double savedRangeSpacing = 0.0;
        double savedAzimuthSpacing = 0.0;
        for (final Product prod : sourceProducts) {
            final MetadataElement absRoot = AbstractMetadata.getAbstractedMetadata(prod);
            if (absRoot == null) {
                throw new OperatorException(MessageFormat.format(
                        "Product ''{0}'' has no abstract metadata.",
                        prod.getName()));
            }

            final double rangeSpacing = AbstractMetadata.getAttributeDouble(absRoot, AbstractMetadata.range_spacing);
            final double azimuthSpacing = AbstractMetadata.getAttributeDouble(absRoot, AbstractMetadata.azimuth_spacing);
            if (savedRangeSpacing > 0.0
                    && savedAzimuthSpacing > 0.0
                    && (Math.abs(rangeSpacing - savedRangeSpacing) > 0.05 || Math
                    .abs(azimuthSpacing - savedAzimuthSpacing) > 0.05)) {
                throw new OperatorException(
                        "Resampling type cannot be NONE because pixel spacings"
                        + " are different for master and slave products");
            } else {
                savedRangeSpacing = rangeSpacing;
                savedAzimuthSpacing = azimuthSpacing;
            }
        }
    }

    private synchronized void checkProductPixelSpacings() throws OperatorException {
        if (productPixelSpacingChecked) {
            return;
        }
        productPixelSpacingChecked = true;
    }

    private void computeTargetSlaveCoordinateOffsets_GCP() {
        final GeoCoding targGeoCoding = targetProduct.getSceneGeoCoding();
        final int targImageWidth = targetProduct.getSceneRasterWidth();
        final int targImageHeight = targetProduct.getSceneRasterHeight();

        final Geometry tgtGeometry = FeatureUtils.createGeoBoundaryPolygon(targetProduct);

        final PixelPos slvPixelPos = new PixelPos();
        final PixelPos tgtPixelPos = new PixelPos();
        final GeoPos slvGeoPos = new GeoPos();

        for (final Product slvProd : sourceProduct) {
            if (slvProd == masterProduct && extent.equals(MASTER_EXTENT)) {
                getSlaveOffsettMap().put(slvProd, new int[]{0, 0});
                continue;
            }

            final GeoCoding slvGeoCoding = slvProd.getSceneGeoCoding();
            final int slvImageWidth = slvProd.getSceneRasterWidth();
            final int slvImageHeight = slvProd.getSceneRasterHeight();

            boolean foundOverlapPoint = false;

            // test corners
            slvGeoCoding.getGeoPos(new PixelPos(10, 10), slvGeoPos);
            if (false) {
                addOffset(slvProd, 0 - (int) tgtPixelPos.x, 0 - (int) tgtPixelPos.y);
                foundOverlapPoint = true;
            }
            if (false) {// !foundOverlapPoint) {
                slvGeoCoding.getGeoPos(new PixelPos(slvImageWidth - 10, slvImageHeight - 10), slvGeoPos);
                if (pixelPosValid(targGeoCoding, slvGeoPos, tgtPixelPos, targImageWidth, targImageHeight)) {
                    addOffset(slvProd, 0 - slvImageWidth - (int) tgtPixelPos.x, slvImageHeight - (int) tgtPixelPos.y);
                    foundOverlapPoint = true;
                }
            }

            if (!foundOverlapPoint) {
                final Geometry slvGeometry = FeatureUtils.createGeoBoundaryPolygon(slvProd);
                final Geometry intersect = tgtGeometry.intersection(slvGeometry);

                for (Coordinate c : intersect.getCoordinates()) {
                    getPixelPos(c.y, c.x, slvGeoCoding, slvPixelPos);

                    if (slvPixelPos.isValid() && slvPixelPos.x >= 0
                            && slvPixelPos.x < slvImageWidth
                            && slvPixelPos.y >= 0
                            && slvPixelPos.y < slvImageHeight) {

                        getPixelPos(c.y, c.x, targGeoCoding, tgtPixelPos);
                        if (tgtPixelPos.isValid() && tgtPixelPos.x >= 0
                                && tgtPixelPos.x < targImageWidth
                                && tgtPixelPos.y >= 0
                                && tgtPixelPos.y < targImageHeight) {

                            addOffset(slvProd, (int) slvPixelPos.x
                                    - (int) tgtPixelPos.x, (int) slvPixelPos.y
                                    - (int) tgtPixelPos.y);
                            foundOverlapPoint = true;
                            break;
                        }
                    }
                }
            }

            if (!foundOverlapPoint) {
                throw new OperatorException("Product " + slvProd.getName()
                        + " has no overlap with master product.");
            }
        }
    }

    private void computeTargetSlaveCoordinateOffsets_Orbits() throws Exception {
        // Note: This procedure will always compute some overlap
        // Similar as for GCPs but for every GCP use orbit information
        MetadataElement root = AbstractMetadata.getAbstractedMetadata(targetProduct);

        final int orbitDegree = 3;

        SLCImage metaMaster = new SLCImage(root);
        Orbit orbitMaster = new Orbit(root, orbitDegree);
        SLCImage metaSlave;
        Orbit orbitSlave;

        // Reference point in Master radar geometry
        Point tgtLP = metaMaster.getApproxRadarCentreOriginal();

        for (final Product slvProd : sourceProduct) {
            if (slvProd == masterProduct) {
                // if master is ref product put 0-es for offset
                getSlaveOffsettMap().put(slvProd, new int[]{0, 0});
                continue;
            }

            // Slave metadata
            root = AbstractMetadata.getAbstractedMetadata(slvProd);
            metaSlave = new SLCImage(root);
            orbitSlave = new Orbit(root, orbitDegree);

            // (lp_master) & (master_orbit)-> (xyz_master) & (slave_orbit)->
            // (lp_slave)
            Point tgtXYZ = orbitMaster.lp2xyz(tgtLP, metaMaster);
            Point slvLP = orbitSlave.xyz2lp(tgtXYZ, metaSlave);

            // Offset: slave minus master
            Point offsetLP = slvLP.min(tgtLP);

            int offsetX = (int) Math.floor(offsetLP.x + .5);
            int offsetY = (int) Math.floor(offsetLP.y + .5);

            addOffset(slvProd, offsetX, offsetY);
        }
    }

    @Override
    public void computeTile(final Band targetBand, final Tile targetTile) throws OperatorException {
        try {
            final Band sourceRaster = getSourceRasterMap().get(targetBand);
            final Product srcProduct = sourceRaster.getProduct();
            final int srcImageWidth = srcProduct.getSceneRasterWidth();
            final int srcImageHeight = srcProduct.getSceneRasterHeight();

            if (resamplingType.contains("NONE")) { // without resampling
                if (!productPixelSpacingChecked) {
                    checkProductPixelSpacings();
                }

                final float noDataValue = (float) targetBand.getGeophysicalNoDataValue();
                final Rectangle targetRectangle = targetTile.getRectangle();
                final ProductData trgData = targetTile.getDataBuffer();
                final int tx0 = targetRectangle.x;
                final int ty0 = targetRectangle.y;
                final int tw = targetRectangle.width;
                final int th = targetRectangle.height;
                final int maxX = tx0 + tw;
                final int maxY = ty0 + th;

                final int[] offset = getSlaveOffsettMap().get(srcProduct);
                final int sx0 = Math.min(Math.max(0, tx0 + offset[0]), srcImageWidth - 1);
                final int sy0 = Math.min(Math.max(0, ty0 + offset[1]), srcImageHeight - 1);
                final int sw = Math.min(sx0 + tw - 1, srcImageWidth - 1) - sx0 + 1;
                final int sh = Math.min(sy0 + th - 1, srcImageHeight - 1) - sy0 + 1;
                final Rectangle srcRectangle = new Rectangle(sx0, sy0, sw, sh);

                final Tile srcTile = getSourceTile(sourceRaster, srcRectangle);
                if (srcTile == null) {
                    return;
                }
                final ProductData srcData = srcTile.getDataBuffer();

                final TileIndex trgIndex = new TileIndex(targetTile);
                final TileIndex srcIndex = new TileIndex(srcTile);

                boolean isInt = false;
                final int trgDataType = trgData.getType();
                if (trgDataType == srcData.getType()
                        && (trgDataType == ProductData.TYPE_INT16 || trgDataType == ProductData.TYPE_INT32)) {
                    isInt = true;
                }

                for (int ty = ty0; ty < maxY; ++ty) {
                    final int sy = ty + offset[1];
                    final int trgOffset = trgIndex.calculateStride(ty);
                    if (sy < 0 || sy >= srcImageHeight) {
                        for (int tx = tx0; tx < maxX; ++tx) {
                            trgData.setElemDoubleAt(tx - trgOffset, noDataValue);
                        }
                        continue;
                    }
                    final int srcOffset = srcIndex.calculateStride(sy);
                    for (int tx = tx0; tx < maxX; ++tx) {
                        final int sx = tx + offset[0];

                        if (sx < 0 || sx >= srcImageWidth) {
                            trgData.setElemDoubleAt(tx - trgOffset, noDataValue);
                        } else if (isInt) {
                            trgData.setElemIntAt(tx - trgOffset, srcData.getElemIntAt(sx - srcOffset));
                        } else {
                            trgData.setElemDoubleAt(tx - trgOffset, srcData.getElemDoubleAt(sx - srcOffset));
                        }
                    }
                }
            } else { // with resampling
                final MyCollocator col = new MyCollocator(this, srcProduct, targetProduct, targetTile.getRectangle());
                col.collocateSourceBand(sourceRaster, targetTile, selectedResampling);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            OperatorUtils.catchOperatorException(getId(), e);
        }
    }

    private void copySlaveMetadata() {
        final MetadataElement targetSlaveMetadataRoot = AbstractMetadata.getSlaveMetadata(targetProduct.getMetadataRoot());
        for (Product prod : sourceProduct) {
            if (prod != masterProduct) {
                final MetadataElement slvAbsMetadata = AbstractMetadata.getAbstractedMetadata(prod);
                if (slvAbsMetadata != null) {
                    final String timeStamp = StackUtils.createBandTimeStamp(prod);
                    final MetadataElement targetSlaveMetadata = new MetadataElement(prod.getName() + timeStamp);
                    targetSlaveMetadataRoot.addElement(targetSlaveMetadata);
                    ProductUtils.copyMetadata(slvAbsMetadata, targetSlaveMetadata);
                }
            }
        }
    }

    /**
     * Maximum extents consists of the overall area
     */
    private void determinMaxExtents() throws Exception {
        try {
            final GeoCoding masterGeoCoding = masterProduct.getSceneGeoCoding();

            double xMin = 0;
            double xMax = masterProduct.getSceneRasterWidth();
            double yMin = 0;
            double yMax = masterProduct.getSceneRasterHeight();

            PixelPos pixelPosUL = new PixelPos();
            PixelPos pixelPosUR = new PixelPos();
            PixelPos pixelPosLL = new PixelPos();
            PixelPos pixelPosLR = new PixelPos();
            GeoPos geoPosUL = new GeoPos();
            GeoPos geoPosUR = new GeoPos();
            GeoPos geoPosLL = new GeoPos();
            GeoPos geoPosLR = new GeoPos();

            for (final Product slvProd : sourceProduct) {
                if (slvProd != masterProduct) {
                    final GeoCoding slaveGeoCoding = slvProd
                            .getSceneGeoCoding();

                    final GeoPos geoPosFirstNear = slaveGeoCoding.getGeoPos(
                            new PixelPos(0, 0), null);
                    final GeoPos geoPosFirstFar = slaveGeoCoding.getGeoPos(
                            new PixelPos(slvProd.getSceneRasterWidth() - 1, 0),
                            null);
                    final GeoPos geoPosLastNear = slaveGeoCoding
                            .getGeoPos(
                                    new PixelPos(0, slvProd
                                            .getSceneRasterHeight() - 1), null);
                    final GeoPos geoPosLastFar = slaveGeoCoding.getGeoPos(
                            new PixelPos(slvProd.getSceneRasterWidth() - 1,
                                    slvProd.getSceneRasterHeight() - 1), null);

                    masterGeoCoding.getPixelPos(geoPosFirstNear, pixelPosUL);
                    masterGeoCoding.getPixelPos(geoPosFirstFar, pixelPosUR);
                    masterGeoCoding.getPixelPos(geoPosLastNear, pixelPosLL);
                    masterGeoCoding.getPixelPos(geoPosLastFar, pixelPosLR);

                    final double[] xArray = {pixelPosUL.x, pixelPosUR.x, pixelPosLL.x, pixelPosLR.x};
                    final double[] yArray = {pixelPosUL.y, pixelPosUR.y, pixelPosLL.y, pixelPosLR.y};

                    for (int i = 0; i < 4; i++) {
                        xMin = Math.min(xMin, xArray[i]);
                        xMax = Math.max(xMax, xArray[i]);
                        yMin = Math.min(yMin, yArray[i]);
                        yMax = Math.max(yMax, yArray[i]);
                    }
                }
            }

            final int sceneWidth = (int) (xMax - xMin) + 1;
            final int sceneHeight = (int) (yMax - yMin) + 1;

            targetProduct = new Product(masterProduct.getName(), masterProduct.getProductType(), sceneWidth, sceneHeight);

            masterGeoCoding.getGeoPos(new PixelPos(xMin, yMin), geoPosUL);
            masterGeoCoding.getGeoPos(new PixelPos(xMax, yMin), geoPosUR);
            masterGeoCoding.getGeoPos(new PixelPos(xMin, yMax), geoPosLL);
            masterGeoCoding.getGeoPos(new PixelPos(xMax, yMax), geoPosLR);

            final float[] latTiePoints = {(float) geoPosUL.lat,
                (float) geoPosUR.lat, (float) geoPosLL.lat,
                (float) geoPosLR.lat};
            final float[] lonTiePoints = {(float) geoPosUL.lon,
                (float) geoPosUR.lon, (float) geoPosLL.lon,
                (float) geoPosLR.lon};

            final TiePointGrid latGrid = new TiePointGrid("latitude", 2, 2,
                    0.5f, 0.5f, sceneWidth - 1, sceneHeight - 1, latTiePoints);
            latGrid.setUnit(Unit.DEGREES);

            final TiePointGrid lonGrid = new TiePointGrid("longitude", 2, 2,
                    0.5f, 0.5f, sceneWidth - 1, sceneHeight - 1, lonTiePoints,
                    TiePointGrid.DISCONT_AT_180);
            lonGrid.setUnit(Unit.DEGREES);

            targetProduct.addTiePointGrid(latGrid);
            targetProduct.addTiePointGrid(lonGrid);
            targetProduct.setSceneGeoCoding(new TiePointGeoCoding(latGrid, lonGrid));
        } catch (Throwable e) {
            OperatorUtils.catchOperatorException(getId(), e);
        }
    }

    /**
     * Minimum extents consists of the overlapping area
     */
    private void determinMinExtents() {
        Geometry tgtGeometry = FeatureUtils.createGeoBoundaryPolygon(masterProduct);
        for (final Product slvProd : sourceProduct) {
            if (slvProd == masterProduct) {
                continue;
            }

            final Geometry slvGeometry = FeatureUtils.createGeoBoundaryPolygon(slvProd);
            tgtGeometry = tgtGeometry.intersection(slvGeometry);
        }

        final GeoCoding mstGeoCoding = masterProduct.getSceneGeoCoding();
        final PixelPos pixPos = new PixelPos();
        final GeoPos geoPos = new GeoPos();
        final double mstWidth = masterProduct.getSceneRasterWidth();
        final double mstHeight = masterProduct.getSceneRasterHeight();

        double maxX = 0, maxY = 0;
        double minX = mstWidth;
        double minY = mstHeight;
        for (Coordinate c : tgtGeometry.getCoordinates()) {
            geoPos.setLocation(c.y, c.x);
            mstGeoCoding.getPixelPos(geoPos, pixPos);
            if (pixPos.isValid() && pixPos.x != -1 && pixPos.y != -1) {
                if (pixPos.x < minX) {
                    minX = Math.max(0, pixPos.x);
                }
                if (pixPos.y < minY) {
                    minY = Math.max(0, pixPos.y);
                }
                if (pixPos.x > maxX) {
                    maxX = Math.min(mstWidth, pixPos.x);
                }
                if (pixPos.y > maxY) {
                    maxY = Math.min(mstHeight, pixPos.y);
                }
            }
        }

        final ProductSubsetBuilder subsetReader = new ProductSubsetBuilder();
        final ProductSubsetDef subsetDef = new ProductSubsetDef();
        subsetDef.addNodeNames(masterProduct.getTiePointGridNames());

        subsetDef.setRegion((int) minX, (int) minY, (int) (maxX - minX), (int) (maxY - minY));
        subsetDef.setSubSampling(1, 1);
        subsetDef.setIgnoreMetadata(false);

        try {
            targetProduct = subsetReader.readProductNodes(masterProduct, subsetDef);
            final Band[] bands = targetProduct.getBands();
            for (Band b : bands) {
                targetProduct.removeBand(b);
            }
        } catch (Throwable t) {
            throw new OperatorException(t);
        }
    }

    private static String getBandName(final String name) {
        if (name.contains("::")) {
            return name.substring(0, name.indexOf("::"));
        }
        return name;
    }

    public String getExtent() {
        return extent;
    }

    public String getInitialOffsetMethod() {
        return initialOffsetMethod;
    }

    private Product getMasterProduct(final String name) {
        final String masterName = getProductName(name);
        for (Product prod : sourceProduct) {
            if (prod.getName().equals(masterName)) {
                return prod;
            }
        }
        return null;
    }

    private static void getPixelPos(final double lat, final double lon,
            final GeoCoding srcGeoCoding, final PixelPos pixelPos) {
        srcGeoCoding.getPixelPos(new GeoPos(lat, lon), pixelPos);
    }

    private String getProductName(final String name) {
        if (name.contains("::")) {
            return name.substring(name.indexOf("::") + 2, name.length());
        }
        return sourceProduct[0].getName();
    }

    public String getResamplingType() {
        return resamplingType;
    }

    private Band[] getSlaveBands() throws OperatorException {
        final List<Band> bandList = new ArrayList<>(5);

        // add master band
        if (masterProduct == null) {
            throw new OperatorException("masterProduct is null");
        }
        if (masterBandNames.length > 2) {
            throw new OperatorException(
                    "Master band should be one real band or a real and imaginary band");
        }
        masterBands[0] = masterProduct.getBand(getBandName(masterBandNames[0]));
        if (!appendToMaster) {
            bandList.add(masterBands[0]);
        }

        final String unit = masterBands[0].getUnit();
        if (unit != null) {
            if (unit.contains(Unit.PHASE)) {
                throw new OperatorException(
                        "Phase band should not be selected for co-registration");
            } else if (unit.contains(Unit.IMAGINARY)) {
                throw new OperatorException(
                        "Real and imaginary master bands should be selected in pairs");
            } else if (unit.contains(Unit.REAL)) {
                if (masterBandNames.length < 2) {
                    final int iBandIdx = masterProduct
                            .getBandIndex(getBandName(masterBandNames[0]));
                    masterBands[1] = masterProduct.getBandAt(iBandIdx + 1);
                    if (!masterBands[1].getUnit().equals(Unit.IMAGINARY)) {
                        throw new OperatorException(
                                "For complex products select a real and an imaginary band");
                    }
                    if (!appendToMaster) {
                        bandList.add(masterBands[1]);
                    }
                } else {
                    final Product prod = getMasterProduct(masterBandNames[1]);
                    if (prod != masterProduct) {
                        // throw new
                        // OperatorException("Please select master bands from the same product");
                    }
                    masterBands[1] = masterProduct
                            .getBand(getBandName(masterBandNames[1]));
                    if (!masterBands[1].getUnit().equals(Unit.IMAGINARY)) {
                        throw new OperatorException(
                                "For complex products select a real and an imaginary band");
                    }
                    if (!appendToMaster) {
                        bandList.add(masterBands[1]);
                    }
                }
            }
        }

        // add slave bands
        for (Product slvProduct : sourceProduct) {
            for (Band band : slvProduct.getBands()) {
                if (band.getUnit() != null
                        && band.getUnit().equals(Unit.PHASE)) {
                    continue;
                }
                if (band instanceof VirtualBand) {
                    continue;
                }
                if (slvProduct == masterProduct
                        && (band == masterBands[0]
                        || band == masterBands[1] || appendToMaster)) {
                    continue;
                }

                bandList.add(band);
            }
        }

        return bandList.toArray(new Band[bandList.size()]);
    }

    /**
     * @return the slaveOffsettMap
     */
    public Map<Product, int[]> getSlaveOffsettMap() {
        return slaveOffsettMap;
    }

    public Product[] getSourceProduct() {
        return sourceProduct;
    }
    
    @Override
    public void initialize() throws OperatorException {
        try {
            if (sourceProduct == null) {
                return;
            }

            if (sourceProduct.length < 2) {
                throw new OperatorException("Please select at least two source products");
            }

            for (final Product prod : sourceProduct) {
                if (prod.getSceneGeoCoding() == null) {
                    throw new OperatorException(MessageFormat.format("Product ''{0}'' has no geo-coding.", prod.getName()));
                }
            }

            if (masterBandNames == null
                    || masterBandNames.length == 0
                    || getMasterProduct(masterBandNames[0]) == null) {
                final Product defaultProd = sourceProduct[0];
                if (defaultProd != null) {
                    final Band defaultBand = defaultProd.getBandAt(0);
                    if (defaultBand != null) {
                        if (defaultBand.getUnit() != null && defaultBand.getUnit().equals(Unit.REAL)) {
                            masterBandNames = new String[]{defaultProd.getBandAt(0).getName(),
                                defaultProd.getBandAt(1).getName()};
                        } else {
                            masterBandNames = new String[]{defaultBand.getName()};
                        }
                    }
                }
                if (masterBandNames.length == 0) {
                    targetProduct = OperatorUtils.createDummyTargetProduct(sourceProduct);
                    return;
                }
            }

            masterProduct = getMasterProduct(masterBandNames[0]);
            if (masterProduct == null) {
                targetProduct = OperatorUtils.createDummyTargetProduct(sourceProduct);
                return;
            }

            appendToMaster = AbstractMetadata.getAbstractedMetadata(masterProduct).getAttributeInt(AbstractMetadata.coregistered_stack, 0) == 1;
            final List<String> masterProductBands = new ArrayList<>(masterProduct.getNumBands());
            final Band[] slaveBandList = getSlaveBands();
            if (masterProduct == null
                    || slaveBandList.length == 0
                    || slaveBandList[0] == null) {
                targetProduct = OperatorUtils.createDummyTargetProduct(sourceProduct);
                return;
            }

            if (resamplingType.contains("NONE") && !extent.equals(MASTER_EXTENT)) {
                throw new OperatorException("Please select only Master extents when resampling type is None");
            }

            if (appendToMaster) {
                extent = MASTER_EXTENT;
            }

            switch (extent) {
                case MASTER_EXTENT:
                    //it gets here!!!
                    targetProduct = new Product(masterProduct.getName(),
                            masterProduct.getProductType(),
                            masterProduct.getSceneRasterWidth(),
                            masterProduct.getSceneRasterHeight());
                    ProductUtils.copyProductNodes(masterProduct, targetProduct);
                    break;
                case MIN_EXTENT:
                    determinMinExtents();
                    break;
                default:
                    determinMaxExtents();
                    break;
            }

            if (appendToMaster) {
                // add all master bands
                for (Band b : masterProduct.getBands()) {
                    if (!(b instanceof VirtualBand)) {
                        final Band targetBand = new Band(b.getName(),
                                b.getDataType(),
                                targetProduct.getSceneRasterWidth(),
                                targetProduct.getSceneRasterHeight());
                        ProductUtils.copyRasterDataNodeProperties(b, targetBand);
                        targetBand.setSourceImage(b.getSourceImage());

                        masterProductBands.add(b.getName());
                        getSourceRasterMap().put(targetBand, b);
                        targetProduct.addBand(targetBand);
                        originalImages.put(targetBand, b.getSourceImage());
                    }
                }
            }

            String suffix = "_mst";
            // add master bands first
            if (!appendToMaster) {
                for (final Band srcBand : slaveBandList) {
                    if (srcBand == masterBands[0]
                            || (masterBands.length > 1 && srcBand == masterBands[1])) {
                        suffix = "_mst" + StackUtils.createBandTimeStamp(srcBand.getProduct());

                        final Band targetBand = new Band(srcBand.getName()
                                + suffix, srcBand.getDataType(),
                                targetProduct.getSceneRasterWidth(),
                                targetProduct.getSceneRasterHeight());
                        ProductUtils.copyRasterDataNodeProperties(srcBand, targetBand);
                        if (extent.equals(MASTER_EXTENT)) {
                            targetBand.setSourceImage(srcBand.getSourceImage());
                        }

                        masterProductBands.add(targetBand.getName());
                        getSourceRasterMap().put(targetBand, srcBand);
                        targetProduct.addBand(targetBand);
                        originalImages.put(targetBand, srcBand.getSourceImage());
                    }
                }
            }

            // then add slave bands
            int cnt = 1;
            if (appendToMaster) {
                for (Band trgBand : targetProduct.getBands()) {
                    final String name = trgBand.getName();
                    if (name.contains("slv" + cnt)) {
                        ++cnt;
                    }
                }
            }
            for (final Band srcBand : slaveBandList) {
                if (!(srcBand == masterBands[0] || (masterBands.length > 1 && srcBand == masterBands[1]))) {
                    if (srcBand.getUnit() != null
                            && srcBand.getUnit().equals(Unit.IMAGINARY)) {
                    } else {
                        suffix = "_slv"
                                + cnt++
                                + StackUtils.createBandTimeStamp(srcBand.getProduct());
                    }
                    final String tgtBandName = srcBand.getName() + suffix;

                    if (targetProduct.getBand(tgtBandName) == null) {
                        final Product srcProduct = srcBand.getProduct();
                        final Band targetBand = new Band(tgtBandName,
                                srcBand.getDataType(),
                                targetProduct.getSceneRasterWidth(),
                                targetProduct.getSceneRasterHeight());
                        ProductUtils.copyRasterDataNodeProperties(srcBand, targetBand);
                        if (extent.equals(MASTER_EXTENT)
                                && (srcProduct == masterProduct
                                || srcProduct.isCompatibleProduct(targetProduct, 1.0e-3f))) {
                            targetBand.setSourceImage(srcBand.getSourceImage());
                        }

                        if (srcBand.getProduct() == masterProduct) {
                            masterProductBands.add(tgtBandName);
                        }

                        getSourceRasterMap().put(targetBand, srcBand);
                        targetProduct.addBand(targetBand);
                        originalImages.put(targetBand, srcBand.getSourceImage());
                    }
                }
            }

            // copy slave abstracted metadata
            copySlaveMetadata();

            StackUtils.saveMasterProductBandNames(targetProduct, masterProductBands.toArray(new String[masterProductBands.size()]));
            saveSlaveProductNames(targetProduct, getSourceRasterMap());

            updateMetadata();

            // copy GCPs if found to master band
            final ProductNodeGroup<Placemark> masterGCPgroup = masterProduct.getGcpGroup();
            if (masterGCPgroup.getNodeCount() > 0) {
                OperatorUtils.copyGCPsToTarget(masterGCPgroup, GCPManager.instance().getGcpGroup(targetProduct.getBandAt(0)), targetProduct.getSceneGeoCoding());
            }

            if (!resamplingType.contains("NONE")) {
                selectedResampling = ResamplingFactory.createResampling(resamplingType);
            } else {
                if (initialOffsetMethod.equals(INITIAL_OFFSET_GEOLOCATION)) {
                    computeTargetSlaveCoordinateOffsets_GCP();
                }

                if (initialOffsetMethod.equals(INITIAL_OFFSET_ORBIT)) {
                    computeTargetSlaveCoordinateOffsets_Orbits();
                }
            }
        } catch (Throwable e) {
            OperatorUtils.catchOperatorException(getId(), e);
        }
    }

    private static boolean pixelPosValid(final GeoCoding geoCoding,
            final GeoPos geoPos, final PixelPos pixelPos, final int width,
            final int height) {
        geoCoding.getPixelPos(geoPos, pixelPos);
        return (pixelPos.isValid() && pixelPos.x >= 0 && pixelPos.x < width
                && pixelPos.y >= 0 && pixelPos.y < height);
    }

    private void saveSlaveProductNames(final Product targetProduct, final Map<Band, Band> sourceRasterMap) {
        for (Product prod : sourceProduct) {
            if (prod != masterProduct) {
                final String suffix = StackUtils.createBandTimeStamp(prod);
                final List<String> bandNames = new ArrayList<>(10);
                for (Band tgtBand : sourceRasterMap.keySet()) {
                    final Band srcBand = sourceRasterMap.get(tgtBand);
                    final Product srcProduct = srcBand.getProduct();
                    if (srcProduct == prod) {
                        bandNames.add(tgtBand.getName());
                    }
                }
                final String prodName = prod.getName() + suffix;
                StackUtils.saveSlaveProductBandNames(targetProduct, prodName, bandNames.toArray(new String[bandNames.size()]));
            }
        }
    }

    public void setExtent(String extent) {
        this.extent = extent;
    }

    public void setInitialOffsetMethod(String initialOffsetMethod) {
        this.initialOffsetMethod = initialOffsetMethod;
    }

    public void setResamplingType(String resamplingType) {
        this.resamplingType = resamplingType;
    }

    public void setSourceProduct(Product[] sourceProduct) {
        this.sourceProduct = sourceProduct;
    }

    private void updateMetadata() {
        final MetadataElement abstractedMetadata = AbstractMetadata.getAbstractedMetadata(targetProduct);
        abstractedMetadata.setAttributeInt("collocated_stack", 1);

        final MetadataElement inputElem = ProductInformation.getInputProducts(targetProduct);

        for (Product srcProduct : sourceProduct) {
            if (srcProduct == masterProduct) {
                continue;
            }

            final MetadataElement slvInputElem = ProductInformation.getInputProducts(srcProduct);
            final MetadataAttribute[] slvInputProductAttrbList = slvInputElem.getAttributes();
            for (MetadataAttribute attrib : slvInputProductAttrbList) {
                final MetadataAttribute inputAttrb = AbstractMetadata.addAbstractedAttribute(inputElem, "InputProduct", ProductData.TYPE_ASCII, "", "");
                inputAttrb.getData().setElems(attrib.getData().getElemString());
            }
        }
    }
}
