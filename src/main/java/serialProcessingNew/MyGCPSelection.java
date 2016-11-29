package serialProcessingNew;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.awt.image.renderable.ParameterBlock;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.media.jai.BorderExtender;
import javax.media.jai.DataBufferDouble;
import javax.media.jai.JAI;
import javax.media.jai.PlanarImage;
import javax.media.jai.RasterFactory;
import javax.media.jai.operator.DFTDescriptor;

import org.apache.commons.math3.util.FastMath;
import org.esa.s1tbx.insar.gpf.coregistration.GCPManager;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GcpDescriptor;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.MetadataAttribute;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Placemark;
import org.esa.snap.core.datamodel.PlacemarkNameFactory;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.ProductNodeGroup;
import org.esa.snap.core.datamodel.TiePointGrid;
import org.esa.snap.core.dataop.dem.ElevationModel;
import org.esa.snap.core.dataop.dem.ElevationModelDescriptor;
import org.esa.snap.core.dataop.dem.ElevationModelRegistry;
import org.esa.snap.core.dataop.downloadable.StatusProgressMonitor;
import org.esa.snap.core.dataop.resamp.ResamplingFactory;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.Tile;
import org.esa.snap.core.util.ProductUtils;
import org.esa.snap.core.util.StringUtils;
import org.esa.snap.core.util.math.MathUtils;
import org.esa.snap.engine_utilities.datamodel.AbstractMetadata;
import org.esa.snap.engine_utilities.datamodel.Unit;
import org.esa.snap.engine_utilities.eo.Constants;
import org.esa.snap.engine_utilities.gpf.OperatorUtils;
import org.esa.snap.engine_utilities.gpf.StackUtils;
import org.esa.snap.engine_utilities.gpf.ThreadManager;
import org.esa.snap.engine_utilities.gpf.TileIndex;
import org.esa.snap.engine_utilities.util.MemUtils;
import org.jtransforms.fft.DoubleFFT_1D;

public class MyGCPSelection extends AbstractOperator {

    Map<Band, Tile> sourceTileMap = new HashMap<Band, Tile>();
    private Product sourceProduct;
    private int numGCPtoGenerate = 200;

    private String coarseRegistrationWindowWidth = "128";

    private String coarseRegistrationWindowHeight = "128";
    private String rowInterpFactor = "2";

    private String columnInterpFactor = "2";
    private int maxIteration = 10;
    private double gcpTolerance = 0.5;

    // ==================== input parameters used for complex co-registration ==================
    private boolean applyFineRegistration = true;

    private String fineRegistrationWindowWidth = "32";
    private String fineRegistrationWindowHeight = "32";
    private int coherenceWindowSize = 3;
    private double coherenceThreshold = 0.6;
    private Boolean useSlidingWindow = false;

    private boolean useAllPolarimetricBands = false;

    //    @Parameter(description = "The coherence function tolerance", interval = "(0, *)", defaultValue = "1.e-6",
//                label="Coherence Function Tolerance")
    private static final double coherenceFuncToler = 1.e-5;
    //    @Parameter(description = "The coherence value tolerance", interval = "(0, *)", defaultValue = "1.e-3",
//                label="Coherence Value Tolerance")
    private static final double coherenceValueToler = 1.e-2;
    // =========================================================================================
    private boolean computeOffset = false;
    private boolean onlyGCPsOnLand = false;

    private Band masterBand1 = null;
    private Band masterBand2 = null;

    private boolean complexCoregistration = false;

    public ProductNodeGroup<Placemark> masterGcpGroup = null;

    private int sourceImageWidth;
    private int sourceImageHeight;
    public int cWindowWidth = 0; // row dimension for master and slave imagette for cross correlation, must be power of 2
    public int cWindowHeight = 0; // column dimension for master and slave imagette for cross correlation, must be power of 2
    private int rowUpSamplingFactor = 0; // cross correlation interpolation factor in row direction, must be power of 2
    private int colUpSamplingFactor = 0; // cross correlation interpolation factor in column direction, must be power of 2
    public int cHalfWindowWidth;
    public int cHalfWindowHeight;

    // parameters used for complex co-registration
    private int fWindowWidth = 0;  // row dimension for master and slave imagette for computing coherence, must be power of 2
    private int fWindowHeight = 0; // column dimension for master and slave imagette for computing coherence, must be power of 2

    private final static int ITMAX = 200;
    private final static double TOL = 2.0e-4;      // Tolerance passed to brent
    private final static double GOLD = 1.618034;   // Here GOLD is the default ratio by which successive intervals are magnified
    private final static double GLIMIT = 100.0;    // GLIMIT is the maximum magnification allowed for a parabolic-fit step.
    private final static double TINY = 1.0e-20;
    private final static double CGOLD = 0.3819660; // CGOLD is the golden ratio;
    private final static double ZEPS = 1.0e-10;    // ZEPS is a small number that protects against trying to achieve fractional
    private final static double MaxInvalidPixelPercentage = 0.66; // maximum percentage of invalid pixels allowed in xcorrelation

    private final Map<Band, Band> sourceRasterMap = new HashMap<>(10);
    private final Map<Band, Band> complexSrcMap = new HashMap<>(10);
    private final Map<Band, Boolean> gcpsComputedMap = new HashMap<>(10);
    private Band primarySlaveBand = null;    // the slave band to process
    private boolean collocatedStack = false;

    private ElevationModel dem = null;
            
    public MyGCPSelection(boolean[] bParams, double[] dParams, int[] iParams, String[] sParams) {
        super();
        numGCPtoGenerate = iParams[0];
        coarseRegistrationWindowWidth = sParams[0];
        coarseRegistrationWindowHeight = sParams[1];
        rowInterpFactor = sParams[2];
        columnInterpFactor = sParams[3];
        fineRegistrationWindowWidth = sParams[4];
        fineRegistrationWindowHeight = sParams[5];
        maxIteration = iParams[1];
        gcpTolerance = dParams[0];
        applyFineRegistration = bParams[0];
        coherenceWindowSize = iParams[2];
        coherenceThreshold = dParams[1];
        useSlidingWindow = bParams[1];
        computeOffset = bParams[2];
        onlyGCPsOnLand = bParams[3];
    }

    @Override
    public void initialize() throws OperatorException {
        try {
            cWindowWidth = Integer.parseInt(coarseRegistrationWindowWidth);
            cWindowHeight = Integer.parseInt(coarseRegistrationWindowHeight);
            cHalfWindowWidth = cWindowWidth / 2;
            cHalfWindowHeight = cWindowHeight / 2;
            rowUpSamplingFactor = Integer.parseInt(rowInterpFactor);
            colUpSamplingFactor = Integer.parseInt(columnInterpFactor);

            final double achievableAccuracy = 1.0 / (double) Math.max(rowUpSamplingFactor, colUpSamplingFactor);
            if (gcpTolerance < achievableAccuracy) {
                throw new OperatorException("The achievable accuracy with current interpolation factors is "
                        + achievableAccuracy + ", GCP Tolerance is below it.");
            }

            getMasterBands();

            sourceImageWidth = sourceProduct.getSceneRasterWidth();
            sourceImageHeight = sourceProduct.getSceneRasterHeight();

            getCollocatedStackFlag();
            
            createTargetProduct();

            GCPManager.instance().removeAllGcpGroups(); // need this line, otherwise cached data from previous run is used
            masterGcpGroup = GCPManager.instance().getGcpGroup(masterBand1);
            if (masterGcpGroup.getNodeCount() <= 0) {
                addGCPGrid(sourceImageWidth, sourceImageHeight, numGCPtoGenerate, masterGcpGroup,
                        targetProduct.getSceneGeoCoding());
            }

            if (complexCoregistration && applyFineRegistration) {
                fWindowWidth = Integer.parseInt(fineRegistrationWindowWidth);
                fWindowHeight = Integer.parseInt(fineRegistrationWindowHeight);
            }
        } catch (Throwable e) {
            //OperatorUtils.catchOperatorException(getId(), e);
        }
    }

    private void getMasterBands() {
        String mstBandName = sourceProduct.getBandAt(0).getName();
        
        // find co-pol bands
        final String[] masterBandNames = StackUtils.getMasterBandNames(sourceProduct);
        for (String bandName : masterBandNames) {
            final String mstPol = OperatorUtils.getPolarizationFromBandName(bandName);
            if (mstPol != null && (mstPol.equals("hh") || mstPol.equals("vv"))) {
                mstBandName = bandName;
                break;
            }
        }
        masterBand1 = sourceProduct.getBand(mstBandName);
        if (masterBand1.getUnit() != null && masterBand1.getUnit().equals(Unit.REAL)) {
            int mstIdx = sourceProduct.getBandIndex(mstBandName);
            if (sourceProduct.getNumBands() > mstIdx + 1) {
                masterBand2 = sourceProduct.getBandAt(mstIdx + 1);
                complexCoregistration = true;
            }
        }
    }

    private static void addGCPGrid(final int width, final int height, final int numPins,
            final ProductNodeGroup<Placemark> group,
            final GeoCoding targetGeoCoding) {
        final float ratio = width / (float) height;
        final float n = (float) Math.sqrt(numPins / ratio);
        final float m = ratio * n;
        final float spacingX = width / m;
        final float spacingY = height / n;
        final GcpDescriptor gcpDescriptor = GcpDescriptor.getInstance();

        group.removeAll();
        int pinNumber = group.getNodeCount() + 1;

        for (float y = spacingY / 2f; y < height; y += spacingY) {

            for (float x = spacingX / 2f; x < width; x += spacingX) {

                final String name = PlacemarkNameFactory.createName(gcpDescriptor, pinNumber);
                final String label = PlacemarkNameFactory.createLabel(gcpDescriptor, pinNumber, true);

                final Placemark newPin = Placemark.createPointPlacemark(gcpDescriptor,
                        name, label, "",
                        new PixelPos((int) x, (int) y), null,
                        targetGeoCoding);
                group.add(newPin);
                ++pinNumber;
            }
        }
    }

    private void getCollocatedStackFlag() {
        final MetadataElement absRoot = AbstractMetadata.getAbstractedMetadata(sourceProduct);
        MetadataAttribute attr = absRoot.getAttribute("collocated_stack");
        if (attr == null) {
            collocatedStack = false;
        } else {
            collocatedStack = true;
            absRoot.removeAttribute(attr);
        }
    }

    /**
     * Create target product.
     */
    private void createTargetProduct() {
        targetProduct = new Product(sourceProduct.getName(), sourceProduct.getProductType(), sourceImageWidth, sourceImageHeight);
        ProductUtils.copyProductNodes(sourceProduct, targetProduct);

        final String[] masterBandNames = StackUtils.getMasterBandNames(sourceProduct);
        final int numSrcBands = sourceProduct.getNumBands();

        //find slave band matching master pol
        Band slvBand1 = null;
        final String mstPol = OperatorUtils.getPolarizationFromBandName(masterBand1.getName());
        for (Band slvBand : sourceProduct.getBands()) {
            if (!StringUtils.contains(masterBandNames, slvBand.getName()) && slvBand != masterBand1) {
                final String slvPol = OperatorUtils.getPolarizationFromBandName(slvBand.getName());
                if (mstPol == null || mstPol.equals(slvPol)) {
                    final String unit = slvBand.getUnit();
                    if (unit != null && !unit.contains(Unit.IMAGINARY)) {
                        slvBand1 = slvBand;
                        break;
                    } else if (unit == null) {
                        // Assume that the image is real-valued if no unit is set
                        slvBand1 = slvBand;
                    }
                }
            }
        }

        boolean oneSlaveProcessed = false;          // all other use setSourceImage
        for (int i = 0; i < numSrcBands; ++i) {
            final Band srcBand = sourceProduct.getBandAt(i);
            final Band targetBand = targetProduct.addBand(srcBand.getName(), srcBand.getDataType());
            originalImages.put(targetBand, srcBand.getSourceImage());
            ProductUtils.copyRasterDataNodeProperties(srcBand, targetBand);
            sourceRasterMap.put(targetBand, srcBand);
            gcpsComputedMap.put(srcBand, false);

            if (srcBand == masterBand1 || srcBand == masterBand2 || oneSlaveProcessed || srcBand != slvBand1
                    || StringUtils.contains(masterBandNames, srcBand.getName())) {
                targetBand.setSourceImage(srcBand.getSourceImage());
            } else {
                final String unit = srcBand.getUnit();
                if (!oneSlaveProcessed && (unit == null || !unit.contains(Unit.IMAGINARY))) {
                    oneSlaveProcessed = true;
                    primarySlaveBand = srcBand;
                    final MetadataElement absRoot = AbstractMetadata.getAbstractedMetadata(targetProduct);
                    AbstractMetadata.addAbstractedAttribute(absRoot, "processed_slave", ProductData.TYPE_ASCII, "", "");
                    absRoot.setAttributeString("processed_slave", primarySlaveBand.getName());
                }
            }

            if (complexCoregistration) {
                if (srcBand.getUnit() != null && srcBand.getUnit().equals(Unit.REAL)) {
                    if (i + 1 < numSrcBands) {
                        complexSrcMap.put(srcBand, sourceProduct.getBandAt(i + 1));
                    }
                }
            }
        }
    }

    private synchronized void createDEM() {
        if (dem != null) {
            return;
        }

        final ElevationModelRegistry elevationModelRegistry = ElevationModelRegistry.getInstance();
        final ElevationModelDescriptor demDescriptor = elevationModelRegistry.getDescriptor("SRTM 3Sec");
        dem = demDescriptor.createDem(ResamplingFactory.createResampling(ResamplingFactory.NEAREST_NEIGHBOUR_NAME));
    }

    /**
     * Called by the framework in order to compute a tile for the given target
     * band.
     * <p>
     * The default implementation throws a runtime exception with the message
     * "not implemented".</p>
     *
     * @param targetTileMap The target tiles associated with all target bands to
     * be computed.
     * @param targetRectangle The rectangle of target tile.
     * @param pm A progress monitor which should be used to determine
     * computation cancelation requests.
     * @throws OperatorException If an error occurs during computation of the
     * target raster.
     */
    @Override
    public void computeTileStack(Map<Band, Tile> targetTileMap, Rectangle targetRectangle) throws OperatorException {
        try {
            if (onlyGCPsOnLand && dem == null) {
                createDEM();
            }

            final String[] masterBandNames = StackUtils.getMasterBandNames(sourceProduct);

            // select only one band per slave product
            final Map<String, Band> singleSlvBandMap = new HashMap<>();
            final Map<Band, Band> bandList = new HashMap<>();
            for (Band targetBand : targetProduct.getBands()) {
                final Band slaveBand = sourceRasterMap.get(targetBand);
                if (gcpsComputedMap.get(slaveBand)) {
                    bandList.put(targetBand, primarySlaveBand);
                    break;
                }

                if (slaveBand == masterBand1 || slaveBand == masterBand2
                        || StringUtils.contains(masterBandNames, slaveBand.getName())) {
                    continue;
                }

                if (collocatedStack && !useAllPolarimetricBands) {
                    final String mstPol = OperatorUtils.getPolarizationFromBandName(masterBand1.getName());
                    final String slvProductName = StackUtils.getSlaveProductName(targetProduct, targetBand, mstPol);
                    if (slvProductName == null || singleSlvBandMap.get(slvProductName) != null) {
                        continue;
                    }
                    singleSlvBandMap.put(slvProductName, targetBand);
                }

                final String unit = slaveBand.getUnit();
                if (unit != null && (unit.contains(Unit.IMAGINARY) || unit.contains(Unit.BIT))) {
                    continue;
                }
                bandList.put(targetBand, slaveBand);
            }

            int bandCnt = 0;
            Band firstTargetBand = null;
            for (Band targetBand : bandList.keySet()) {
                ++bandCnt;
                final Band slaveBand = bandList.get(targetBand);

                if (collocatedStack || !collocatedStack && bandCnt == 1) {
                    if (complexCoregistration) {
                        computeSlaveGCPs(slaveBand, complexSrcMap.get(slaveBand), targetBand);
                    } else {
                        computeSlaveGCPs(slaveBand, null, targetBand);
                    }

                    if (bandCnt == 1) {
                        firstTargetBand = targetBand;
                    }
                } else {
                    copyFirstTargetBandGCPs(firstTargetBand, targetBand);
                }

                // copy slave data to target
                if (slaveBand == primarySlaveBand) {
                    final Tile targetTile = targetTileMap.get(targetBand);
                    if (targetTile != null) {
                        targetTile.setRawSamples(getSourceTile(slaveBand, targetRectangle).getRawSamples());
                    }
                }
            }
        } catch (Throwable e) {
            OperatorUtils.catchOperatorException(getId(), e);
        }
    }

    /**
     * Compute slave GCPs for the given tile.
     *
     * @param slaveBand the input band
     * @param slaveBand2 for complex
     * @param targetBand the output band
     */
    private synchronized void computeSlaveGCPs(final Band slaveBand, final Band slaveBand2, final Band targetBand) throws OperatorException {
        if (gcpsComputedMap.get(slaveBand)) {
            return;
        }

        gcpsComputedMap.put(slaveBand, true);
        try {
            final ProductNodeGroup<Placemark> targetGCPGroup = GCPManager.instance().getGcpGroup(targetBand);
            final GeoCoding tgtGeoCoding = targetProduct.getSceneGeoCoding();

            final int[] offset = new int[2]; // 0-x, 1-y
            if (computeOffset) {
                determiningImageOffset(slaveBand, slaveBand2, offset); // calls determiningImageOffset 2 or 3
            }
            
            final int numberOfMasterGCPs = masterGcpGroup.getNodeCount();
            for (int i = 0; i < numberOfMasterGCPs; ++i) {
                final Placemark mPin = masterGcpGroup.get(i);

                if (checkMasterGCPValidity(mPin)) {

                    final GeoPos mGCPGeoPos = mPin.getGeoPos();
                    final PixelPos mGCPPixelPos = mPin.getPixelPos();
                    final PixelPos sGCPPixelPos = new PixelPos(mPin.getPixelPos().x + offset[0],
                            mPin.getPixelPos().y + offset[1]);
                    if (!checkSlaveGCPValidity(sGCPPixelPos)) {
                        continue;
                    }

                    boolean getSlaveGCP = getCoarseSlaveGCPPosition(slaveBand, slaveBand2, mGCPPixelPos, sGCPPixelPos); // calss getSlaveImagette and getMasterImagette (4-7)

                    if (getSlaveGCP && complexCoregistration && applyFineRegistration) {
                        getSlaveGCP = getFineSlaveGCPPosition(slaveBand, slaveBand2, mGCPPixelPos, sGCPPixelPos); // calls getComplexMasterImagette and getInitialComplexSlaveImagette (8-9)
                    }

                    if (getSlaveGCP) {
                        final Placemark sPin = Placemark.createPointPlacemark(
                                GcpDescriptor.getInstance(),
                                mPin.getName(),
                                mPin.getLabel(),
                                mPin.getDescription(),
                                sGCPPixelPos,
                                mGCPGeoPos,
                                tgtGeoCoding);

                        targetGCPGroup.add(sPin);
                    }
                }
            }

            MemUtils.tileCacheFreeOldTiles();

        } catch (Throwable e) {
            //   OperatorUtils.catchOperatorException(getId() + " computeSlaveGCPs ", e);
        }
    }

    private void determiningImageOffset(final Band slaveBand1, final Band slaveBand2, int[] offset) {
        try {
            // get master and slave imagettes
            final MetadataElement absRoot = AbstractMetadata.getAbstractedMetadata(sourceProduct);
            double groundRangeSpacing = absRoot.getAttributeDouble(AbstractMetadata.range_spacing, 1);
            final double azimuthSpacing = absRoot.getAttributeDouble(AbstractMetadata.azimuth_spacing, 1);
            final boolean srgrFlag = AbstractMetadata.getAttributeBoolean(absRoot, AbstractMetadata.srgr_flag);
            if (!srgrFlag) {
                final TiePointGrid incidenceAngle = OperatorUtils.getIncidenceAngle(sourceProduct);
                final double incidenceAngleAtCentreRangePixel
                        = incidenceAngle.getPixelDouble(sourceImageWidth / 2f, sourceImageHeight / 2f);
                groundRangeSpacing /= FastMath.sin(incidenceAngleAtCentreRangePixel * Constants.DTOR);
            }
            final int nRgLooks = Math.max(1, sourceImageWidth / 2048);
            final int nAzLooks = Math.max(1, (int) ((double) nRgLooks * groundRangeSpacing / azimuthSpacing + 0.5));
            final int targetImageWidth = sourceImageWidth / nRgLooks;
            final int targetImageHeight = sourceImageHeight / nAzLooks;
            final int windowWidth = (int) FastMath.pow(2, (int) (Math.log10(targetImageWidth) / Math.log10(2)));
            final int windowHeight = (int) FastMath.pow(2, (int) (Math.log10(targetImageHeight) / Math.log10(2)));
            final double[] mI = new double[windowWidth * windowHeight];
            final double[] sI = new double[windowWidth * windowHeight];

            final int tileCountX = 4;
            final int tileCountY = 4;
            final int tileWidth = windowWidth / tileCountX;
            final int tileHeight = windowHeight / tileCountY;
            final Rectangle[] tileRectangles = new Rectangle[tileCountX * tileCountY];
            int index = 0;
            for (int tileY = 0; tileY < tileCountY; tileY++) {
                final int ypos = tileY * tileHeight;
                for (int tileX = 0; tileX < tileCountX; tileX++) {
                    final Rectangle tileRectangle = new Rectangle(tileX * tileWidth, ypos,
                            tileWidth, tileHeight);
                    tileRectangles[index++] = tileRectangle;
                }
            }

            final StatusProgressMonitor status = new StatusProgressMonitor(StatusProgressMonitor.TYPE.SUBTASK);
            status.beginTask("Computing offset... ", tileRectangles.length);

            final ThreadManager threadManager = new ThreadManager();
            try {
                for (final Rectangle rectangle : tileRectangles) {
                    // checkForCancellation();

                    final Thread worker = new Thread() {

                        @Override
                        public void run() {
                            final int x0 = rectangle.x;
                            final int y0 = rectangle.y;
                            final int w = rectangle.width;
                            final int h = rectangle.height;
                            final int xMax = x0 + w;
                            final int yMax = y0 + h;

                            final int xStart = x0 * nRgLooks;
                            final int yStart = y0 * nAzLooks;
                            final int xEnd = xMax * nRgLooks;
                            final int yEnd = yMax * nAzLooks;

                            final Rectangle srcRect = new Rectangle(xStart, yStart, xEnd - xStart, yEnd - yStart);
                            final Tile mstTile1 = getSourceTile(masterBand1, srcRect);
                            final ProductData mstData1 = mstTile1.getDataBuffer();
                            final TileIndex mstIndex = new TileIndex(mstTile1);
                            final Tile slvTile1 = getSourceTile(slaveBand1, srcRect);
                            final ProductData slvData1 = slvTile1.getDataBuffer();
                            final TileIndex slvIndex = new TileIndex(slvTile1);

                            ProductData mstData2 = null;
                            ProductData slvData2 = null;
                            if (complexCoregistration) {
                                mstData2 = getSourceTile(masterBand2, srcRect).getDataBuffer();
                                slvData2 = getSourceTile(slaveBand2, srcRect).getDataBuffer();
                            }

                            final double rgAzLooks = nRgLooks * nAzLooks;

                            for (int y = y0; y < yMax; y++) {
                                final int yByWidth = y * windowWidth;
                                final int y1 = y * nAzLooks;
                                final int y2 = y1 + nAzLooks;
                                for (int x = x0; x < xMax; x++) {
                                    final int x1 = x * nRgLooks;
                                    final int x2 = x1 + nRgLooks;
                                    mI[yByWidth + x] = getMeanValue(x1, x2, y1, y2, mstData1, mstData2, mstIndex, rgAzLooks);
                                    sI[yByWidth + x] = getMeanValue(x1, x2, y1, y2, slvData1, slvData2, slvIndex, rgAzLooks);
                                }
                            }

                            status.worked(1);
                        }
                    };
                    threadManager.add(worker);

                }
                threadManager.finish();

            } catch (Throwable e) {
                OperatorUtils.catchOperatorException("GCPSelectionOp", e);
            } finally {
                status.done();
            }

            // correlate master and slave imagettes
            final RenderedImage masterImage = createRenderedImage(mI, windowWidth, windowHeight);
            final PlanarImage masterSpectrum = dft(masterImage);

            final RenderedImage slaveImage = createRenderedImage(sI, windowWidth, windowHeight);
            final PlanarImage slaveSpectrum = dft(slaveImage);
            final PlanarImage conjugateSlaveSpectrum = conjugate(slaveSpectrum);

            final PlanarImage crossSpectrum = multiplyComplex(masterSpectrum, conjugateSlaveSpectrum);
            final PlanarImage correlatedImage = idft(crossSpectrum);
            final PlanarImage crossCorrelatedImage = magnitude(correlatedImage);

            // compute offset
            final int w = crossCorrelatedImage.getWidth();
            final int h = crossCorrelatedImage.getHeight();
            final Raster idftData = crossCorrelatedImage.getData();
            final double[] real = idftData.getSamples(0, 0, w, h, 0, (double[]) null);

            int peakRow = 0;
            int peakCol = 0;
            double peak = 0;
            for (int r = 0; r < h; r++) {
                for (int c = 0; c < w; c++) {
                    if (r >= h / 4 && r <= h * 3 / 4 || c >= w / 4 && c <= w * 3 / 4) {
                        continue;
                    }
                    final int s = r * w + c;
                    if (peak < real[s]) {
                        peak = real[s];
                        peakRow = r;
                        peakCol = c;
                    }
                }
            }

            if (peakRow <= h / 2) {
                offset[1] = -peakRow * nAzLooks;
            } else {
                offset[1] = (h - peakRow) * nAzLooks;
            }

            if (peakCol <= w / 2) {
                offset[0] = -peakCol * nRgLooks;
            } else {
                offset[0] = (w - peakCol) * nRgLooks;
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private double getMeanValue(final int xStart, final int xEnd, final int yStart, final int yEnd,
            final ProductData srcData1, final ProductData srcData2,
            final TileIndex srcIndex, final double rgAzLooks) {

        double v1, v2;
        double meanValue = 0.0;
        if (complexCoregistration) {

            for (int y = yStart; y < yEnd; y++) {
                srcIndex.calculateStride(y);
                for (int x = xStart; x < xEnd; x++) {
                    final int idx = srcIndex.getIndex(x);
                    v1 = srcData1.getElemDoubleAt(idx);
                    v2 = srcData2.getElemDoubleAt(idx);
                    meanValue += v1 * v1 + v2 * v2;
                }
            }

        } else {

            for (int y = yStart; y < yEnd; y++) {
                srcIndex.calculateStride(y);
                for (int x = xStart; x < xEnd; x++) {
                    meanValue += srcData1.getElemDoubleAt(srcIndex.getIndex(x));
                }
            }
        }

        return meanValue / rgAzLooks;
    }

    /**
     * Copy GCPs of the first target band to current target band.
     *
     * @param firstTargetBand First target band.
     * @param targetBand Current target band.
     */
    private void copyFirstTargetBandGCPs(final Band firstTargetBand, final Band targetBand) {

        final ProductNodeGroup<Placemark> firstTargetBandGcpGroup = GCPManager.instance().getGcpGroup(firstTargetBand);
        final ProductNodeGroup<Placemark> currentTargetBandGCPGroup = GCPManager.instance().getGcpGroup(targetBand);
        final int numberOfGCPs = firstTargetBandGcpGroup.getNodeCount();
        for (int i = 0; i < numberOfGCPs; ++i) {
            currentTargetBandGCPGroup.add(firstTargetBandGcpGroup.get(i));
        }
    }

    /**
     * Check if a given master GCP is within the given tile and the GCP imagette
     * is within the image.
     *
     * @param mPin The GCP position.
     * @return flag Return true if the GCP is within the given tile and the GCP
     * imagette is within the image, false otherwise.
     */
    private boolean checkMasterGCPValidity(final Placemark mPin) throws Exception {
        final PixelPos pixelPos = mPin.getPixelPos();
        if (onlyGCPsOnLand) {
            double alt = dem.getElevation(mPin.getGeoPos());
            if (alt == dem.getDescriptor().getNoDataValue()) {
                return false;
            }
        }
        return (pixelPos.x - cHalfWindowWidth + 1 >= 0 && pixelPos.x + cHalfWindowWidth <= sourceImageWidth - 1)
                && (pixelPos.y - cHalfWindowHeight + 1 >= 0 && pixelPos.y + cHalfWindowHeight <= sourceImageHeight - 1);
    }

    /**
     * Check if a given slave GCP imagette is within the image.
     *
     * @param pixelPos The GCP pixel position.
     * @return flag Return true if the GCP is within the image, false otherwise.
     */
    private boolean checkSlaveGCPValidity(final PixelPos pixelPos) {
        return (pixelPos.x - cHalfWindowWidth + 1 >= 0 && pixelPos.x + cHalfWindowWidth <= sourceImageWidth - 1)
                && (pixelPos.y - cHalfWindowHeight + 1 >= 0 && pixelPos.y + cHalfWindowHeight <= sourceImageHeight - 1);
    }

    private boolean getCoarseSlaveGCPPosition(final Band slaveBand, final Band slaveBand2,
            final PixelPos mGCPPixelPos, final PixelPos sGCPPixelPos) {
        try {
            final double[] mI = new double[cWindowWidth * cWindowHeight];
            final double[] sI = new double[cWindowWidth * cWindowHeight];

            final boolean getMISuccess = getMasterImagette(mGCPPixelPos, mI);
            if (!getMISuccess) {
                return false;
            }

            double rowShift = gcpTolerance + 1;
            double colShift = gcpTolerance + 1;
            int numIter = 0;

            while (Math.abs(rowShift) >= gcpTolerance || Math.abs(colShift) >= gcpTolerance) {
                if (numIter >= maxIteration) {
                    return false;
                }

                if (!checkSlaveGCPValidity(sGCPPixelPos)) {
                    return false;
                }

                final boolean getSISuccess = getSlaveImagette(slaveBand, slaveBand2, sGCPPixelPos, sI);
                if (!getSISuccess) {
                    return false;
                }

                final double[] shift = {0, 0};
                if (!getSlaveGCPShift(shift, mI, sI)) {
                    return false;
                }

                rowShift = shift[0];
                colShift = shift[1];
                sGCPPixelPos.x += (float) colShift;
                sGCPPixelPos.y += (float) rowShift;
                numIter++;
            }

            return true;
        } catch (Throwable e) {
            OperatorUtils.catchOperatorException(getId() + " getCoarseSlaveGCPPosition ", e);
        }
        return false;
    }

    private boolean getMasterImagette(final PixelPos gcpPixelPos, final double[] mI) throws OperatorException {
        final int x0 = (int) gcpPixelPos.x;
        final int y0 = (int) gcpPixelPos.y;
        final int xul = x0 - cHalfWindowWidth + 1;
        final int yul = y0 - cHalfWindowHeight + 1;
        final Rectangle masterImagetteRectangle = new Rectangle(xul, yul, cWindowWidth, cWindowHeight);

        try {
            final Tile masterImagetteRaster1 = getSourceTile(masterBand1, masterImagetteRectangle);
            final ProductData masterData1 = masterImagetteRaster1.getDataBuffer();
            final double noDataValue1 = masterBand1.getNoDataValue();

            ProductData masterData2 = null;
            double noDataValue2 = 0.0;
            if (complexCoregistration) {
                final Tile masterImagetteRaster2 = getSourceTile(masterBand2, masterImagetteRectangle);
                masterData2 = masterImagetteRaster2.getDataBuffer();
                noDataValue2 = masterBand2.getNoDataValue();
            }

            final TileIndex mstIndex = new TileIndex(masterImagetteRaster1);

            int k = 0;
            int numInvalidPixels = 0;
            for (int j = 0; j < cWindowHeight; j++) {
                final int offset = mstIndex.calculateStride(yul + j);
                for (int i = 0; i < cWindowWidth; i++) {
                    final int index = xul + i - offset;
                    if (complexCoregistration) {
                        final double v1 = masterData1.getElemDoubleAt(index);
                        final double v2 = masterData2.getElemDoubleAt(index);
                        if (v1 == noDataValue1 && v2 == noDataValue2) {
                            numInvalidPixels++;
                        }
                        mI[k++] = v1 * v1 + v2 * v2;
                    } else {
                        final double v = masterData1.getElemDoubleAt(index);
                        if (v == noDataValue1) {
                            numInvalidPixels++;
                        }
                        mI[k++] = v;
                    }
                }
            }

            masterData1.dispose();
            if (masterData2 != null) {
                masterData2.dispose();
            }

            if (numInvalidPixels > MaxInvalidPixelPercentage * cWindowHeight * cWindowWidth) {
                return false;
            }
            return true;

        } catch (Throwable e) {
            OperatorUtils.catchOperatorException("getMasterImagette", e);
        }
        return false;
    }

    private boolean getSlaveImagette(
            final Band slaveBand1, final Band slaveBand2, final PixelPos gcpPixelPos, final double[] sI)
            throws OperatorException {
    	
        final double xx = gcpPixelPos.x;
        final double yy = gcpPixelPos.y;
        final int xul = (int) xx - cHalfWindowWidth;
        final int yul = (int) yy - cHalfWindowHeight;
        final Rectangle slaveImagetteRectangle = new Rectangle(xul, yul, cWindowWidth + 3, cWindowHeight + 3);
        int k = 0;
//        if(!slaveBand1.getSourceImage().getBounds().contains(slaveImagetteRectangle))
//		{	
//        	System.out.println(gcpPixelPos+ " is out of bounds -- master");
//			
//		}
        try {
            final Tile slaveImagetteRaster1 = getSourceTile(slaveBand1, slaveImagetteRectangle);
            final ProductData slaveData1 = slaveImagetteRaster1.getDataBuffer();
            final double noDataValue1 = slaveBand1.getNoDataValue();

            Tile slaveImagetteRaster2 = null;
            ProductData slaveData2 = null;
            double noDataValue2 = 0.0;
            if (complexCoregistration) {
                slaveImagetteRaster2 = getSourceTile(slaveBand2, slaveImagetteRectangle);
                slaveData2 = slaveImagetteRaster2.getDataBuffer();
                noDataValue2 = slaveBand2.getNoDataValue();
            }

            final TileIndex index0 = new TileIndex(slaveImagetteRaster1);
            final TileIndex index1 = new TileIndex(slaveImagetteRaster1);

            int numInvalidPixels = 0;
            for (int j = 0; j < cWindowHeight; j++) {
                final double y = yy - cHalfWindowHeight + j + 1;
                final int y0 = (int) y;
                final int y1 = y0 + 1;
                final int offset0 = index0.calculateStride(y0);
                final int offset1 = index1.calculateStride(y1);
                final double wy = y - y0;
                for (int i = 0; i < cWindowWidth; i++) {
                    final double x = xx - cHalfWindowWidth + i + 1;
                    final int x0 = (int) x;
                    final int x1 = x0 + 1;
                    final double wx = x - x0;

                    final int x00 = x0 - offset0;
                    final int x01 = x0 - offset1;
                    final int x10 = x1 - offset0;
                    final int x11 = x1 - offset1;

                    if (complexCoregistration) {
                        final double v1 = MathUtils.interpolate2D(wy, wx, slaveData1.getElemDoubleAt(x00),
                                slaveData1.getElemDoubleAt(x01),
                                slaveData1.getElemDoubleAt(x10),
                                slaveData1.getElemDoubleAt(x11));

                        final double v2 = MathUtils.interpolate2D(wy, wx, slaveData2.getElemDoubleAt(x00),
                                slaveData2.getElemDoubleAt(x01),
                                slaveData2.getElemDoubleAt(x10),
                                slaveData2.getElemDoubleAt(x11));

                        if (v1 == noDataValue1 && v2 == noDataValue2) {
                            numInvalidPixels++;
                        }
                        sI[k] = v1 * v1 + v2 * v2;
                    } else {

                        final double v = MathUtils.interpolate2D(wy, wx, slaveData1.getElemDoubleAt(x00),
                                slaveData1.getElemDoubleAt(x01),
                                slaveData1.getElemDoubleAt(x10),
                                slaveData1.getElemDoubleAt(x11));

                        if (v == noDataValue1) {
                            numInvalidPixels++;
                        }
                        sI[k] = v;
                    }
                    ++k;
                }
            }
            slaveData1.dispose();
            if (slaveData2 != null) {
                slaveData2.dispose();
            }

            if (numInvalidPixels > MaxInvalidPixelPercentage * cWindowHeight * cWindowWidth) {
                return false;
            }
            return true;

        } catch (Throwable e) {
            OperatorUtils.catchOperatorException("getSlaveImagette", e);
        }
        return false;
    }

    private boolean getSlaveGCPShift(final double[] shift, final double[] mI, final double[] sI) {
        try {
            // perform cross correlation
            final PlanarImage crossCorrelatedImage = computeCrossCorrelatedImage(mI, sI);

            // get peak shift: row and col
            final int w = crossCorrelatedImage.getWidth();
            final int h = crossCorrelatedImage.getHeight();

            final Raster idftData = crossCorrelatedImage.getData();
            final double[] real = idftData.getSamples(0, 0, w, h, 0, (double[]) null);

            int peakRow = 0;
            int peakCol = 0;
            double peak = real[0];
            for (int r = 0; r < h; r++) {
                for (int c = 0; c < w; c++) {
                    final int k = r * w + c;
                    if (real[k] > peak) {
                        peak = real[k];
                        peakRow = r;
                        peakCol = c;
                    }
                }
            }

            if (peakRow <= h / 2) {
                shift[0] = (double) (-peakRow) / (double) rowUpSamplingFactor;
            } else {
                shift[0] = (double) (h - peakRow) / (double) rowUpSamplingFactor;
            }

            if (peakCol <= w / 2) {
                shift[1] = (double) (-peakCol) / (double) colUpSamplingFactor;
            } else {
                shift[1] = (double) (w - peakCol) / (double) colUpSamplingFactor;
            }

            return true;
        } catch (Throwable t) {
            System.out.println("getSlaveGCPShift failed " + t.getMessage());
            return false;
        }
    }

    private PlanarImage computeCrossCorrelatedImage(final double[] mI, final double[] sI) {
        // get master imagette spectrum
        final RenderedImage masterImage = createRenderedImage(mI, cWindowWidth, cWindowHeight);
        final PlanarImage masterSpectrum = dft(masterImage);

        // get slave imagette spectrum
        final RenderedImage slaveImage = createRenderedImage(sI, cWindowWidth, cWindowHeight);
        final PlanarImage slaveSpectrum = dft(slaveImage);

        // get conjugate slave spectrum
        final PlanarImage conjugateSlaveSpectrum = conjugate(slaveSpectrum);

        // multiply master spectrum and conjugate slave spectrum
        final PlanarImage crossSpectrum = multiplyComplex(masterSpectrum, conjugateSlaveSpectrum);

        // upsampling cross spectrum
        final RenderedImage upsampledCrossSpectrum = upsampling(crossSpectrum);

        // perform IDF on the cross spectrum
        final PlanarImage correlatedImage = idft(upsampledCrossSpectrum);

        // compute the magnitode of the cross correlated image
        return magnitude(correlatedImage);
    }

    private static RenderedImage createRenderedImage(final double[] array, final int w, final int h) {
        // create rendered image with demension being width by height
        final SampleModel sampleModel = RasterFactory.createBandedSampleModel(DataBuffer.TYPE_DOUBLE, w, h, 1);
        final ColorModel colourModel = PlanarImage.createColorModel(sampleModel);
        final DataBufferDouble dataBuffer = new DataBufferDouble(array, array.length);
        final WritableRaster raster = RasterFactory.createWritableRaster(sampleModel, dataBuffer, new Point(0, 0));

        return new BufferedImage(colourModel, raster, false, new Hashtable());
    }

    private static PlanarImage dft(final RenderedImage image) {
        final ParameterBlock pb = new ParameterBlock();
        pb.addSource(image);
        pb.add(DFTDescriptor.SCALING_NONE);
        pb.add(DFTDescriptor.REAL_TO_COMPLEX);
        return JAI.create("dft", pb, null);
    }

    private static PlanarImage idft(final RenderedImage image) {
        final ParameterBlock pb = new ParameterBlock();
        pb.addSource(image);
        pb.add(DFTDescriptor.SCALING_DIMENSIONS);
        pb.add(DFTDescriptor.COMPLEX_TO_COMPLEX);
        return JAI.create("idft", pb, null);
    }

    private static PlanarImage conjugate(final PlanarImage image) {
        final ParameterBlock pb = new ParameterBlock();
        pb.addSource(image);
        return JAI.create("conjugate", pb, null);
    }

    private static PlanarImage multiplyComplex(final PlanarImage image1, final PlanarImage image2) {
        final ParameterBlock pb = new ParameterBlock();
        pb.addSource(image1);
        pb.addSource(image2);
        return JAI.create("MultiplyComplex", pb, null);
    }

    private RenderedImage upsampling(final PlanarImage image) {
        final int w = image.getWidth();  // w is power of 2
        final int h = image.getHeight(); // h is power of 2
        final int newWidth = rowUpSamplingFactor * w; // rowInterpFactor should be power of 2 to avoid zero padding in idft
        final int newHeight = colUpSamplingFactor * h; // colInterpFactor should be power of 2 to avoid zero padding in idft

        // create shifted image
        final ParameterBlock pb1 = new ParameterBlock();
        pb1.addSource(image);
        pb1.add(w / 2);
        pb1.add(h / 2);
        PlanarImage shiftedImage = JAI.create("PeriodicShift", pb1, null);

        // create zero padded image
        final ParameterBlock pb2 = new ParameterBlock();
        final int leftPad = (newWidth - w) / 2;
        final int rightPad = leftPad;
        final int topPad = (newHeight - h) / 2;
        final int bottomPad = topPad;
        pb2.addSource(shiftedImage);
        pb2.add(leftPad);
        pb2.add(rightPad);
        pb2.add(topPad);
        pb2.add(bottomPad);
        pb2.add(BorderExtender.createInstance(BorderExtender.BORDER_ZERO));
        final PlanarImage zeroPaddedImage = JAI.create("border", pb2);

        // reposition zero padded image so the image origin is back at (0,0)
        final ParameterBlock pb3 = new ParameterBlock();
        pb3.addSource(zeroPaddedImage);
        pb3.add(1.0f * leftPad);
        pb3.add(1.0f * topPad);
        final PlanarImage zeroBorderedImage = JAI.create("translate", pb3, null);

        // shift the zero padded image
        final ParameterBlock pb4 = new ParameterBlock();
        pb4.addSource(zeroBorderedImage);
        pb4.add(newWidth / 2);
        pb4.add(newHeight / 2);
        final PlanarImage shiftedZeroPaddedImage = JAI.create("PeriodicShift", pb4, null);

        return shiftedZeroPaddedImage;
    }

    private static PlanarImage magnitude(final PlanarImage image) {
        final ParameterBlock pb = new ParameterBlock();
        pb.addSource(image);
        return JAI.create("magnitude", pb, null);
    }

    /**
     * The function is for unit test only.
     *
     * @param windowWidth The window width for cross-correlation
     * @param windowHeight The window height for cross-correlation
     * @param rowUpSamplingFactor The row up sampling rate
     * @param colUpSamplingFactor The column up sampling rate
     * @param maxIter The maximum number of iterations in computing slave GCP
     * shift
     * @param tolerance The stopping criterion for slave GCP shift calculation
     */
    public void setTestParameters(final String windowWidth,
            final String windowHeight,
            final String rowUpSamplingFactor,
            final String colUpSamplingFactor,
            final int maxIter,
            final double tolerance) {

        coarseRegistrationWindowWidth = windowWidth;
        coarseRegistrationWindowHeight = windowHeight;
        rowInterpFactor = rowUpSamplingFactor;
        columnInterpFactor = colUpSamplingFactor;
        maxIteration = maxIter;
        gcpTolerance = tolerance;
    }

    //=========================================== Complex Co-registration ==============================================
    private boolean getFineSlaveGCPPosition(final Band slaveBand1, final Band slaveBand2,
            final PixelPos mGCPPixelPos, final PixelPos sGCPPixelPos) {
        try {
            //System.out.println("mGCP = (" + mGCPPixelPos.x + ", " + mGCPPixelPos.y + ")");
            //System.out.println("Initial sGCP = (" + sGCPPixelPos.x + ", " + sGCPPixelPos.y + ")");

            final ComplexCoregData complexData = new ComplexCoregData(coherenceWindowSize,
                    coherenceFuncToler, coherenceValueToler,
                    fWindowWidth, fWindowHeight, useSlidingWindow);
            getComplexMasterImagette(complexData, mGCPPixelPos);
            /*
             System.out.println("Real part of master imagette:");
             outputRealImage(compleData.mII);
             System.out.println("Imaginary part of master imagette:");
             outputRealImage(compleData.mIQ);
             */

            getInitialComplexSlaveImagette(complexData, slaveBand1, slaveBand2, sGCPPixelPos);
            /*
             System.out.println("Real part of initial slave imagette:");
             outputRealImage(compleData.sII0);
             System.out.println("Imaginary part of initial slave imagette:");
             outputRealImage(compleData.sIQ0);
             */

            final double[] p = {sGCPPixelPos.x, sGCPPixelPos.y};

            final double coherence = powell(complexData, p);
            //System.out.println("Final sGCP = (" + p[0] + ", " + p[1] + "), coherence = " + (1-coherence));

            complexData.dispose();

            if (1 - coherence < coherenceThreshold) {
                //System.out.println("Invalid GCP");
                return false;
            } else {
                sGCPPixelPos.x = (float) p[0];
                sGCPPixelPos.y = (float) p[1];
                //System.out.println("Valid GCP");
                return true;
            }
        } catch (Throwable e) {
            //OperatorUtils.catchOperatorException(getId() + " getFineSlaveGCPPosition ", e);
        }
        return false;
    }

    private void getComplexMasterImagette(final ComplexCoregData compleData, final PixelPos gcpPixelPos) {
        compleData.mII = new double[compleData.fWindowHeight][compleData.fWindowWidth];
        compleData.mIQ = new double[compleData.fWindowHeight][compleData.fWindowWidth];
        final int x0 = (int) gcpPixelPos.x;
        final int y0 = (int) gcpPixelPos.y;
        final int xul = x0 - compleData.fHalfWindowWidth + 1;
        final int yul = y0 - compleData.fHalfWindowHeight + 1;
        final Rectangle masterImagetteRectangle = new Rectangle(xul, yul, compleData.fWindowWidth, compleData.fWindowHeight);

        final Tile masterImagetteRaster1 = getSourceTile(masterBand1, masterImagetteRectangle);
        final Tile masterImagetteRaster2 = getSourceTile(masterBand2, masterImagetteRectangle);

        final ProductData masterData1 = masterImagetteRaster1.getDataBuffer();
        final ProductData masterData2 = masterImagetteRaster2.getDataBuffer();

        final TileIndex index = new TileIndex(masterImagetteRaster1);

        final double[][] mIIdata = compleData.mII;
        final double[][] mIQdata = compleData.mIQ;
        for (int j = 0; j < compleData.fWindowHeight; j++) {
            index.calculateStride(yul + j);
            for (int i = 0; i < compleData.fWindowWidth; i++) {
                final int idx = index.getIndex(xul + i);
                mIIdata[j][i] = masterData1.getElemDoubleAt(idx);
                mIQdata[j][i] = masterData2.getElemDoubleAt(idx);
            }
        }
        masterData1.dispose();
        masterData2.dispose();
    }

    private void getInitialComplexSlaveImagette(final ComplexCoregData compleData,
            final Band slaveBand1, final Band slaveBand2,
            final PixelPos sGCPPixelPos) {

        compleData.sII0 = new double[compleData.fWindowHeight][compleData.fWindowWidth];
        compleData.sIQ0 = new double[compleData.fWindowHeight][compleData.fWindowWidth];

        final int x0 = (int) (sGCPPixelPos.x + 0.5);
        final int y0 = (int) (sGCPPixelPos.y + 0.5);

        compleData.point0[0] = sGCPPixelPos.x;
        compleData.point0[1] = sGCPPixelPos.y;

        final int xul = x0 - compleData.fHalfWindowWidth + 1;
        final int yul = y0 - compleData.fHalfWindowHeight + 1;
        final Rectangle slaveImagetteRectangle = new Rectangle(xul, yul, compleData.fWindowWidth, compleData.fWindowHeight);

        final Tile slaveImagetteRaster1 = getSourceTile(slaveBand1, slaveImagetteRectangle);
        final Tile slaveImagetteRaster2 = getSourceTile(slaveBand2, slaveImagetteRectangle);

        final ProductData slaveData1 = slaveImagetteRaster1.getDataBuffer();
        final ProductData slaveData2 = slaveImagetteRaster2.getDataBuffer();
        final TileIndex index = new TileIndex(slaveImagetteRaster1);

        final double[][] sII0data = compleData.sII0;
        final double[][] sIQ0data = compleData.sIQ0;
        for (int j = 0; j < compleData.fWindowHeight; j++) {
            index.calculateStride(yul + j);
            for (int i = 0; i < compleData.fWindowWidth; i++) {
                final int idx = index.getIndex(xul + i);
                sII0data[j][i] = slaveData1.getElemDoubleAt(idx);
                sIQ0data[j][i] = slaveData2.getElemDoubleAt(idx);
            }
        }
        slaveData1.dispose();
        slaveData2.dispose();
    }

    private static double computeCoherence(final ComplexCoregData complexData, final double[] point) {
        // Set penalty at the boundary of the pixel so that the searching area is within a pixel
        final double xShift = Math.abs(complexData.point0[0] - point[0]);
        final double yShift = Math.abs(complexData.point0[1] - point[1]);
        if (xShift >= 0.5 || yShift >= 0.5) {
            return 1.0;
        }

        getComplexSlaveImagette(complexData, point);
        /*
         System.out.println("Real part of master imagette:");
         outputRealImage(compleData.mII);
         System.out.println("Imaginary part of master imagette:");
         outputRealImage(compleData.mIQ);
         System.out.println("Real part of slave imagette:");
         outputRealImage(compleData.sII);
         System.out.println("Imaginary part of slave imagette:");
         outputRealImage(compleData.sIQ);
         */

        double coherence = 0.0;
        if (complexData.useSlidingWindow) {

            final int maxR = complexData.fWindowHeight - complexData.coherenceWindowSize;
            final int maxC = complexData.fWindowWidth - complexData.coherenceWindowSize;
            for (int r = 0; r <= maxR; r++) {
                for (int c = 0; c <= maxC; c++) {
                    coherence += getCoherence(complexData, r, c, complexData.coherenceWindowSize, complexData.coherenceWindowSize);
                }
            }

            coherence /= (maxR + 1) * (maxC + 1);

        } else {
            coherence = getCoherence(complexData, 0, 0, complexData.fWindowWidth, complexData.fWindowHeight);
        }
        //System.out.println("coherence = " + coherence);

        return 1 - coherence;
    }

    private static double computeCoherence(final ComplexCoregData compleData,
            final double a, final double[] p, final double[] d) {
        final double[] point = {p[0] + a * d[0], p[1] + a * d[1]};
        return computeCoherence(compleData, point);
    }

    private static void getComplexSlaveImagette(final ComplexCoregData compleData, final double[] point) {
        compleData.sII = new double[compleData.fWindowHeight][compleData.fWindowWidth];
        compleData.sIQ = new double[compleData.fWindowHeight][compleData.fWindowWidth];

        final double[][] sII0data = compleData.sII0;
        final double[][] sIQ0data = compleData.sIQ0;
        final double[][] sIIdata = compleData.sII;
        final double[][] sIQdata = compleData.sIQ;

        final int x0 = (int) (compleData.point0[0] + 0.5);
        final int y0 = (int) (compleData.point0[1] + 0.5);

        final double xShift = x0 - point[0];
        final double yShift = y0 - point[1];

        final double[] rowArray = new double[compleData.fTwoWindowWidth];
        final double[] rowPhaseArray = new double[compleData.fTwoWindowWidth];
        final DoubleFFT_1D row_fft = new DoubleFFT_1D(compleData.fWindowWidth);

        int signalLength = rowArray.length / 2;
        computeShiftPhaseArray(xShift, signalLength, rowPhaseArray);
        for (int r = 0; r < compleData.fWindowHeight; r++) {
            int k = 0;
            final double[] sII = sII0data[r];
            final double[] sIQ = sIQ0data[r];
            for (int c = 0; c < compleData.fWindowWidth; c++) {
                rowArray[k++] = sII[c];
                rowArray[k++] = sIQ[c];
            }

            row_fft.complexForward(rowArray);
            multiplySpectrumByShiftFactor(rowArray, rowPhaseArray);
            row_fft.complexInverse(rowArray, true);
            for (int c = 0; c < compleData.fWindowWidth; c++) {
                sIIdata[r][c] = rowArray[2 * c];
                sIQdata[r][c] = rowArray[2 * c + 1];
            }
        }

        final double[] colArray = new double[compleData.fTwoWindowHeight];
        final double[] colPhaseArray = new double[compleData.fTwoWindowHeight];
        final DoubleFFT_1D col_fft = new DoubleFFT_1D(compleData.fWindowHeight);

        signalLength = colArray.length / 2;
        computeShiftPhaseArray(yShift, signalLength, colPhaseArray);
        for (int c = 0; c < compleData.fWindowWidth; c++) {
            int k = 0;
            for (int r = 0; r < compleData.fWindowHeight; r++) {
                colArray[k++] = sIIdata[r][c];
                colArray[k++] = sIQdata[r][c];
            }

            col_fft.complexForward(colArray);
            multiplySpectrumByShiftFactor(colArray, colPhaseArray);
            col_fft.complexInverse(colArray, true);
            for (int r = 0; r < compleData.fWindowHeight; r++) {
                sIIdata[r][c] = colArray[2 * r];
                sIQdata[r][c] = colArray[2 * r + 1];
            }
        }
    }

    private static void computeShiftPhaseArray(final double shift, final int signalLength, final double[] phaseArray) {
        int k2;
        double phaseK;
        final double phase = -2.0 * Constants.PI * shift / signalLength;
        final int halfSignalLength = (int) (signalLength * 0.5 + 0.5);

        for (int k = 0; k < signalLength; ++k) {
            if (k < halfSignalLength) {
                phaseK = phase * k;
            } else {
                phaseK = phase * (k - signalLength);
            }
            k2 = k * 2;
            phaseArray[k2] = FastMath.cos(phaseK);
            phaseArray[k2 + 1] = FastMath.sin(phaseK);
        }
    }

    private static void multiplySpectrumByShiftFactor(final double[] array, final double[] phaseArray) {
        int k2;
        double c, s;
        double real, imag;
        final int signalLength = array.length / 2;
        for (int k = 0; k < signalLength; ++k) {
            k2 = k * 2;
            c = phaseArray[k2];
            s = phaseArray[k2 + 1];
            real = array[k2];
            imag = array[k2 + 1];
            array[k2] = real * c - imag * s;
            array[k2 + 1] = real * s + imag * c;
        }
    }

    private static double getCoherence(final ComplexCoregData compleData, final int row, final int col,
            final int coherenceWindowWidth, final int coherenceWindowHeight) {

        // Compute coherence of master and slave imagettes by creating a coherence image
        double sum1 = 0.0;
        double sum2 = 0.0;
        double sum3 = 0.0;
        double sum4 = 0.0;
        double mr, mi, sr, si;
        final double[][] mIIdata = compleData.mII;
        final double[][] mIQdata = compleData.mIQ;
        final double[][] sIIdata = compleData.sII;
        final double[][] sIQdata = compleData.sIQ;
        double[] mII, mIQ, sII, sIQ;
        int rIdx, cIdx;
        for (int r = 0; r < coherenceWindowHeight; r++) {
            rIdx = row + r;
            mII = mIIdata[rIdx];
            mIQ = mIQdata[rIdx];
            sII = sIIdata[rIdx];
            sIQ = sIQdata[rIdx];
            for (int c = 0; c < coherenceWindowWidth; c++) {
                cIdx = col + c;
                mr = mII[cIdx];
                mi = mIQ[cIdx];
                sr = sII[cIdx];
                si = sIQ[cIdx];
                sum1 += mr * sr + mi * si;
                sum2 += mi * sr - mr * si;
                sum3 += mr * mr + mi * mi;
                sum4 += sr * sr + si * si;
            }
        }

        return Math.sqrt(sum1 * sum1 + sum2 * sum2) / Math.sqrt(sum3 * sum4);
    }

    /**
     * Minimize coherence as a function of row shift and column shift using
     * Powell's method. The 1-D minimization subroutine linmin() is used. p is
     * the starting point and also the final optimal point. \
     *
     * @param complexData the master and slave complex data
     * @param p Starting point for the minimization.
     * @return fp
     */
    private static double powell(final ComplexCoregData complexData, final double[] p) {
        final double[][] directions = {{0, 1}, {1, 0}}; // set initial searching directions
        double fp = computeCoherence(complexData, p); // get function value for initial point
        //System.out.println("Initial 1 - coherence = " + fp);

        final double[] p0 = {p[0], p[1]}; // save the initial point
        final double[] currentDirection = {0.0, 0.0}; // current searching direction

        for (int iter = 0; iter < ITMAX; iter++) {
            p0[0] = p[0];
            p0[1] = p[1];
            double fp0 = fp; // save function value for the initial point
            int imax = 0;     // direction index for the largest single step decrement
            double maxDecrement = 0.0; // the largest single step decrement

            for (int i = 0; i < 2; i++) { // for each iteration, loop through all directions in the set

                // copy the ith searching direction
                currentDirection[0] = directions[i][0];
                currentDirection[1] = directions[i][1];

                final double fpc = fp; // save function value at current point
                fp = linmin(complexData, p, currentDirection); // minimize function along the ith direction, and get new point in p

                final double decrement = Math.abs(fpc - fp);
                if (decrement > maxDecrement) { // if the single step decrement is the largest so far,
                    maxDecrement = decrement;   // record the decrement and the direction index.
                    imax = i;
                }
            }

            // After trying all directions, check the decrement from start point to end point.
            // If the decrement is less than certain amount, then stop.
            /*
             if (2.0*Math.abs(fp0 - fp) <= ftol*(Math.abs(fp0) + Math.abs(fp))) { //Termination criterion.
             System.out.println("Number of iterations: " + (iter+1));
             return fp;
             }
             */
            //Termination criterion 1: stop if coherence change is small
            if (Math.abs(fp0 - fp) < complexData.coherenceFuncToler) {
                //System.out.println("C1: Number of iterations: " + (iter+1));
                return fp;
            }

            //Termination criterion 2: stop if GCP shift is small
            if (Math.sqrt((p0[0] - p[0]) * (p0[0] - p[0]) + (p0[1] - p[1]) * (p0[1] - p[1])) < complexData.coherenceValueToler) {
                //System.out.println("C2: Number of iterations: " + (iter+1));
                return fp;
            }
            // Otherwise, prepare for the next iteration
            //final double[] pe = new double[2];
            final double[] averageDirection = {p[0] - p0[0], p[1] - p0[1]};
            final double norm = Math.sqrt(averageDirection[0] * averageDirection[0]
                    + averageDirection[1] * averageDirection[1]);
            for (int j = 0; j < 2; j++) {
                averageDirection[j] /= norm; // construct the average direction
                //pe[j] = p[j] + averageDirection[j]; // construct the extrapolated point
                //p0[j] = p[j]; // save the final opint of current iteration as the initial point for the next iteration
            }

            //final double fpe = computeCoherence(complexData, pe); // get function value for the extrapolated point.
            final double fpe = linmin(complexData, p, averageDirection); // JL test

            if (fpe < fp0) { // condition 1 for updating search direction

                final double d1 = (fp0 - fp - maxDecrement) * (fp0 - fp - maxDecrement);
                final double d2 = (fp0 - fpe) * (fp0 - fpe);

                if (2.0 * (fp0 - 2.0 * fp + fpe) * d1 < maxDecrement * d2) { // condition 2 for updating searching direction

                    // The calling of linmin() next line should be commented out because it changes
                    // the starting point for the next iteration and this average direction will be
                    // added to the searching directions anyway.
                    //fp = linmin(complexData, p, averageDirection); // minimize function along the average direction
                    for (int j = 0; j < 2; j++) {
                        directions[imax][j] = directions[1][j]; // discard the direction for the largest decrement
                        directions[1][j] = averageDirection[j]; // add the average direction as a new direction
                    }
                }
            }
        }
        return fp;
    }

    /**
     * Given a starting point p and a searching direction xi, moves and resets p
     * to where the function takes on a minimum value along the direction xi
     * from p, and replaces xi by the actual vector displacement that p was
     * moved. Also returns the minimum value. This is accomplished by calling
     * the routines mnbrak() and brent().
     *
     * @param complexData the master and slave complex data
     * @param p The starting point
     * @param xi The searching direction
     * @return The minimum function value
     */
    private static double linmin(final ComplexCoregData complexData, final double[] p, final double[] xi) {

        // set initial guess for brackets: [ax, bx, cx]
        final double[] bracketPoints = {0.0, 0.02, 0.0};

        // get new brackets [ax, bx, cx] that bracket a minimum of the function
        mnbrak(complexData, bracketPoints, p, xi);

        // find function minimum in the brackets
        return brent(complexData, bracketPoints, p, xi);
    }

    /**
     * Given a distinct initial points ax and bx in bracketPoints, this routine
     * searches in the downhill direction (defined by the function as evaluated
     * at the initial points) and returns new points ax, bx, cx that bracket a
     * minimum of the function.
     *
     * @param complexData the master and slave complex data
     * @param bracketPoints The bracket points ax, bx and cx
     * @param p The starting point
     * @param xi The searching direction
     */
    private static void mnbrak(final ComplexCoregData complexData,
            final double[] bracketPoints, final double[] p, final double[] xi) {

        double ax = bracketPoints[0];
        double bx = bracketPoints[1];

        double fa = computeCoherence(complexData, ax, p, xi);
        double fb = computeCoherence(complexData, bx, p, xi);

        if (fb > fa) { // Switch roles of a and b so that we can go
            // downhill in the direction from a to b.
            double tmp = ax;
            ax = bx;
            bx = tmp;

            tmp = fa;
            fa = fb;
            fb = tmp;
        }

        double cx = bx + GOLD * (bx - ax); // First guess for c.
        double fc = computeCoherence(complexData, cx, p, xi);

        double fu;
        while (fb > fc) { // Keep returning here until we bracket.

            final double r = (bx - ax) * (fb - fc); // Compute u by parabolic extrapolation from a; b; c.
            // TINY is used to prevent any possible division by zero.
            final double q = (bx - cx) * (fb - fa);

            double u = bx - ((bx - cx) * q - (bx - ax) * r)
                    / (2.0 * sign(Math.max(Math.abs(q - r), TINY), q - r));

            final double ulim = bx + GLIMIT * (cx - bx);

            // We won't go farther than this. Test various possibilities:
            if ((bx - u) * (u - cx) > 0.0) { // Parabolic u is between b and c: try it.

                fu = computeCoherence(complexData, u, p, xi);

                if (fu < fc) { // Got a minimum between b and c.

                    ax = bx;
                    bx = u;
                    break;

                } else if (fu > fb) { // Got a minimum between between a and u.

                    cx = u;
                    break;
                }

                // reach this point can only be:  fc <= fu <= fb
                u = cx + GOLD * (cx - bx); // Parabolic fit was no use. Use default magnification.
                fu = computeCoherence(complexData, u, p, xi);

            } else if ((cx - u) * (u - ulim) > 0.0) { // Parabolic fit is between c and its allowed limit.

                fu = computeCoherence(complexData, u, p, xi);

                if (fu < fc) {
                    bx = cx;
                    cx = u;
                    u = cx + GOLD * (cx - bx);
                    fb = fc;
                    fc = fu;
                    fu = computeCoherence(complexData, u, p, xi);
                }

            } else if ((u - ulim) * (ulim - cx) >= 0.0) { // Limit parabolic u to maximum allowed value.

                u = ulim;
                fu = computeCoherence(complexData, u, p, xi);

            } else { // Reject parabolic u, use default magnification.
                u = cx + GOLD * (cx - bx);
                fu = computeCoherence(complexData, u, p, xi);
            }

            ax = bx;
            bx = cx;
            cx = u; // Eliminate oldest point and continue.

            fa = fb;
            fb = fc;
            fc = fu;
        }

        bracketPoints[0] = ax;
        bracketPoints[1] = bx;
        bracketPoints[2] = cx;
    }

    /**
     * Given a bracketing triplet of abscissas [ax, bx, cx] (such that bx is
     * between ax and cx, and f(bx) is less than both f(ax) and f(cx)), this
     * routine isolates the minimum to a fractional precision of about tol using
     * Brent's method. p is reset to the point where function takes on a minimum
     * value along direction xi from p, and xi is replaced by the axtual
     * displacement that p moved. The minimum function value is returned.
     *
     * @param complexData the master and slave complex data
     * @param bracketPoints The bracket points ax, bx and cx
     * @param pp The starting point
     * @param xi The searching direction
     * @return The minimum unction value
     */
    private static double brent(final ComplexCoregData complexData,
            final double[] bracketPoints, final double[] pp, final double[] xi) {

        final int maxNumIterations = 100; // the maximum number of iterations

        final double ax = bracketPoints[0];
        final double bx = bracketPoints[1];
        final double cx = bracketPoints[2];

        double d = 0.0;
        double u = 0.0;
        double e = 0.0; //This will be the distance moved on the step before last.
        double a = (ax < cx ? ax : cx); // a and b must be in ascending order,
        double b = (ax > cx ? ax : cx); // but input abscissas need not be.
        double x = bx; // Initializations...
        double w = bx;
        double v = bx;
        double fw = computeCoherence(complexData, x, pp, xi);
        double fv = fw;
        double fx = fw;

        for (int iter = 0; iter < maxNumIterations; iter++) { // Main loop.

            final double xm = 0.5 * (a + b);
            final double tol1 = TOL * Math.abs(x) + ZEPS;
            final double tol2 = 2.0 * tol1;

            if (Math.abs(x - xm) <= (tol2 - 0.5 * (b - a))) { // Test for done here.
                xi[0] *= x;
                xi[1] *= x;
                pp[0] += xi[0];
                pp[1] += xi[1];
                return fx;
            }

            if (Math.abs(e) > tol1) { // Construct a trial parabolic fit.

                final double r = (x - w) * (fx - fv);
                double q = (x - v) * (fx - fw);
                double p = (x - v) * q - (x - w) * r;
                q = 2.0 * (q - r);

                if (q > 0.0) {
                    p = -p;
                }

                q = Math.abs(q);
                final double etemp = e;
                e = d;

                if (Math.abs(p) >= Math.abs(0.5 * q * etemp)
                        || p <= q * (a - x) || p >= q * (b - x)) {

                    e = (x >= xm ? a - x : b - x);
                    d = CGOLD * e;

                    // The above conditions determine the acceptability of the parabolic fit. Here we
                    // take the golden section step into the larger of the two segments.
                } else {

                    d = p / q; // Take the parabolic step.
                    u = x + d;
                    if (u - a < tol2 || b - u < tol2) {
                        d = sign(tol1, xm - x);
                    }
                }

            } else {

                e = (x >= xm ? a - x : b - x); // larger part: from x to both ends
                d = CGOLD * e;
            }

            u = (Math.abs(d) >= tol1 ? x + d : x + sign(tol1, d));
            final double fu = computeCoherence(complexData, u, pp, xi);

            // This is the one function evaluation per iteration.
            if (fu <= fx) { // Now decide what to do with our func tion evaluation.

                if (u >= x) {
                    a = x;
                } else {
                    b = x;
                }
                v = w;
                w = x;
                x = u;

                fv = fw;
                fw = fx;
                fx = fu;

            } else {

                if (u < x) {
                    a = u;
                } else {
                    b = u;
                }

                if (fu <= fw || w == x) {

                    v = w;
                    w = u;
                    fv = fw;
                    fw = fu;

                } else if (fu <= fv || v == x || v == w) {

                    v = u;
                    fv = fu;
                }
            } // Done with housekeeping. Back for another iteration.
        }

        System.out.println("Too many iterations in brent");
        return -1.0;
    }

    private static double sign(final double a, final double b) {
        if (b >= 0) {
            return a;
        }
        return -a;
    }

    private static class ComplexCoregData {
        private double[][] mII = null;          // real part of master imagette for coherence computation
        private double[][] mIQ = null;          // imaginary part of master imagette for coherence computation
        private double[][] sII = null;          // real part of slave imagette for coherence computation
        private double[][] sIQ = null;          // imaginary part of slave imagette for coherence computation
        private double[][] sII0 = null;         // real part of initial slave imagette for coherence computation
        private double[][] sIQ0 = null;         // imaginary part of initial slave imagette for coherence computation
        final double[] point0 = new double[2];  // initial slave GCP position

        private final int coherenceWindowSize;
        private final double coherenceFuncToler;
        private final double coherenceValueToler;

        private final int fWindowWidth;  // row dimension for master and slave imagette for computing coherence, must be power of 2
        private final int fWindowHeight; // column dimension for master and slave imagette for computing coherence, must be power of 2
        private final int fHalfWindowWidth;
        private final int fHalfWindowHeight;
        private final int fTwoWindowWidth;
        private final int fTwoWindowHeight;

        private final boolean useSlidingWindow;

        ComplexCoregData(final int coherenceWindowSize, final double coherenceFuncToler, final double coherenceValueToler,
                final int fWindowWidth, final int fWindowHeight, final boolean useSlidingWindow) {
            this.coherenceWindowSize = coherenceWindowSize;
            this.coherenceFuncToler = coherenceFuncToler;
            this.coherenceValueToler = coherenceValueToler;
            this.fWindowWidth = fWindowWidth;
            this.fWindowHeight = fWindowHeight;
            this.fHalfWindowWidth = fWindowWidth / 2;
            this.fHalfWindowHeight = fWindowHeight / 2;
            this.fTwoWindowWidth = fWindowWidth * 2;
            this.fTwoWindowHeight = fWindowHeight * 2;
            this.useSlidingWindow = useSlidingWindow;
        }

        void dispose() {
            mII = null;
            mIQ = null;
            sII = null;
            sIQ = null;
            sII0 = null;
            sIQ0 = null;
        }
    }

    public String getCoarseRegistrationWindowWidth() {
        return coarseRegistrationWindowWidth;
    }

    public void setCoarseRegistrationWindowWidth(
            String coarseRegistrationWindowWidth) {
        this.coarseRegistrationWindowWidth = coarseRegistrationWindowWidth;
    }

    public String getCoarseRegistrationWindowHeight() {
        return coarseRegistrationWindowHeight;
    }

    public void setCoarseRegistrationWindowHeight(
            String coarseRegistrationWindowHeight) {
        this.coarseRegistrationWindowHeight = coarseRegistrationWindowHeight;
    }

    public String getRowInterpFactor() {
        return rowInterpFactor;
    }

    public void setRowInterpFactor(String rowInterpFactor) {
        this.rowInterpFactor = rowInterpFactor;
    }

    public double getGcpTolerance() {
        return gcpTolerance;
    }

    public void setGcpTolerance(double gcpTolerance) {
        this.gcpTolerance = gcpTolerance;
    }

    public boolean isApplyFineRegistration() {
        return applyFineRegistration;
    }

    public void setApplyFineRegistration(boolean applyFineRegistration) {
        this.applyFineRegistration = applyFineRegistration;
    }

    public double getCoherenceThreshold() {
        return coherenceThreshold;
    }

    public void setCoherenceThreshold(double coherenceThreshold) {
        this.coherenceThreshold = coherenceThreshold;
    }

    public Boolean getUseSlidingWindow() {
        return useSlidingWindow;
    }

    public void setUseSlidingWindow(Boolean useSlidingWindow) {
        this.useSlidingWindow = useSlidingWindow;
    }

    public String getColumnInterpFactor() {
        return columnInterpFactor;
    }

    public void setColumnInterpFactor(String columnInterpFactor) {
        this.columnInterpFactor = columnInterpFactor;
    }

    public boolean isOnlyGCPsOnLand() {
        return onlyGCPsOnLand;
    }

    public void setOnlyGCPsOnLand(boolean onlyGCPsOnLand) {
        this.onlyGCPsOnLand = onlyGCPsOnLand;
    }

    public int getMaxIteration() {
        return maxIteration;
    }

    public void setMaxIteration(int maxIteration) {
        this.maxIteration = maxIteration;
    }

    public int getCoherenceWindowSize() {
        return coherenceWindowSize;
    }

    public void setCoherenceWindowSize(int coherenceWindowSize) {
        this.coherenceWindowSize = coherenceWindowSize;
    }

    public boolean isComputeOffset() {
        return computeOffset;
    }

    public void setComputeOffset(boolean computeOffset) {
        this.computeOffset = computeOffset;
    }

    public String getFineRegistrationWindowWidth() {
        return fineRegistrationWindowWidth;
    }

    public void setFineRegistrationWindowWidth(String fineRegistrationWindowWidth) {
        this.fineRegistrationWindowWidth = fineRegistrationWindowWidth;
    }

    public String getFineRegistrationWindowHeight() {
        return fineRegistrationWindowHeight;
    }

    public void setFineRegistrationWindowHeight(String fineRegistrationWindowHeight) {
        this.fineRegistrationWindowHeight = fineRegistrationWindowHeight;
    }

    public Product getSourceProduct() {
        return sourceProduct;
    }

    public void setSourceProduct(Product sourceProduct) {
        this.sourceProduct = sourceProduct;
    }

	public ProductNodeGroup<Placemark> getMasterGcpGroup() {
		return masterGcpGroup;
	}

	public void setMasterGcpGroup(ProductNodeGroup<Placemark> masterGcpGroup) {
		this.masterGcpGroup = masterGcpGroup;
	}

	public Map<Band, Band> getSourceRasterMap() {
		return sourceRasterMap;
	}
	public Map<String, String> slavebandsForGCP() {
        final String[] masterBandNames = StackUtils.getMasterBandNames(sourceProduct);

        // select only one band per slave product
        final Map<String, Band> singleSlvBandMap = new HashMap<>();
        final Map<String, String> bandList = new HashMap<>();
        for (Band targetBand : targetProduct.getBands()) {
            final Band slaveBand = sourceRasterMap.get(targetBand);
            if (gcpsComputedMap.get(slaveBand)) {
                bandList.put(targetBand.getName(), primarySlaveBand.getName());
                break;
            }

            if (slaveBand == masterBand1 || slaveBand == masterBand2
                    || StringUtils.contains(masterBandNames, slaveBand.getName())) {
                continue;
            }

            if (collocatedStack && !useAllPolarimetricBands) {
                final String mstPol = OperatorUtils.getPolarizationFromBandName(masterBand1.getName());
                final String slvProductName = StackUtils.getSlaveProductName(targetProduct, targetBand, mstPol);
                if (slvProductName == null || singleSlvBandMap.get(slvProductName) != null) {
                    continue;
                }
                singleSlvBandMap.put(slvProductName, targetBand);
            }

            final String unit = slaveBand.getUnit();
            if (unit != null && (unit.contains(Unit.IMAGINARY) || unit.contains(Unit.BIT))) {
                continue;
            }
            bandList.put(targetBand.getName(), slaveBand.getName());
           
        }
        return bandList;
	}
}