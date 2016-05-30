package eu.bde.sc7pilot.taskbased;

import java.awt.Desktop;
import java.awt.Rectangle;
import java.awt.image.DataBuffer;
import java.awt.image.RenderedImage;
import java.awt.image.renderable.ParameterBlock;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.media.jai.Interpolation;
import javax.media.jai.InterpolationTable;
import javax.media.jai.JAI;
import javax.media.jai.RenderedOp;
import javax.media.jai.WarpPolynomial;

import org.esa.s1tbx.insar.gpf.coregistration.GCPManager;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GcpDescriptor;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.MetadataAttribute;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Placemark;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.ProductNodeGroup;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.Tile;
import org.esa.snap.core.util.ProductUtils;
import org.esa.snap.core.util.StringUtils;
import org.esa.snap.engine_utilities.datamodel.AbstractMetadata;
import org.esa.snap.engine_utilities.datamodel.Unit;
import org.esa.snap.engine_utilities.eo.Constants;
import org.esa.snap.engine_utilities.gpf.OperatorUtils;
import org.esa.snap.engine_utilities.gpf.ReaderUtils;
import org.esa.snap.engine_utilities.gpf.StackUtils;
import org.esa.snap.engine_utilities.util.ResourceUtils;
import org.jlinda.core.coregistration.SimpleLUT;


public class MyWarp extends AbstractOperator {

    private Product sourceProduct;

    private float rmsThreshold = 0.5f;

    private int warpPolynomialOrder = 2;

    private String interpolationMethod = BILINEAR;

    private boolean excludeMaster = false;

    private Interpolation interp = null;
    private InterpolationTable interpTable = null;

    private Boolean openResidualsFile = false;

    private Band masterBand = null;
    private Band masterBand2 = null;
    private boolean complexCoregistration = false;
    private boolean warpDataAvailable = false;

    public static final String NEAREST_NEIGHBOR = "Nearest-neighbor interpolation";
    public static final String BILINEAR = "Bilinear interpolation";
    public static final String BICUBIC = "Bicubic interpolation";
    public static final String BICUBIC2 = "Bicubic2 interpolation";
    public static final String RECT = "Step function (nearest-neighbor)";
    public static final String TRI = "Linear interpolation";
    public static final String CC4P = "Cubic convolution (4 points)";
    public static final String CC6P = "Cubic convolution (6 points)";
    public static final String TS6P = "Truncated sinc (6 points)";
    public static final String TS8P = "Truncated sinc (8 points)";
    public static final String TS16P = "Truncated sinc (16 points)";

    private final Map<Band, Band> sourceRasterMap = new HashMap<>(10);
    private final Map<Band, Band> complexSrcMap = new HashMap<>(10);
    private final Map<Band, WarpData> warpDataMap = new HashMap<>(10);

    private String processedSlaveBand;
    private String[] masterBandNames = null;

    private int maxIterations = 20;

    public MyWarp(boolean[] bParams, float rmsThreshold, int warpPolynomialOrder, String interpolationMethod) {
        super();
        this.rmsThreshold = rmsThreshold;
        this.warpPolynomialOrder = warpPolynomialOrder;
        this.interpolationMethod = interpolationMethod;
        this.excludeMaster = bParams[0];
        this.openResidualsFile = bParams[1];
    }

    @Override
    public void initialize() throws OperatorException {
        try {
            // clear any old residual file
            final File residualsFile = getResidualsFile(sourceProduct);
            if (residualsFile.exists()) {
                residualsFile.delete();
            }

            getMasterBands();

            // The following code is temporary
            if (complexCoregistration) {
                switch (interpolationMethod) {
                    case CC4P:
                        constructInterpolationTable(CC4P);
                        break;
                    case CC6P:
                        constructInterpolationTable(CC6P);
                        break;
                    case TS6P:
                        constructInterpolationTable(TS6P);
                        break;
                    case TS8P:
                        constructInterpolationTable(TS8P);
                        break;
                    case TS16P:
                        constructInterpolationTable(TS16P);
                        break;
                    default:
                        interp = Interpolation.getInstance(Interpolation.INTERP_NEAREST);
                        break;
                }
            } else { // detected products
                switch (interpolationMethod) {
                    case NEAREST_NEIGHBOR:
                        interp = Interpolation.getInstance(Interpolation.INTERP_NEAREST);
                        break;
                    case BILINEAR:
                        interp = Interpolation.getInstance(Interpolation.INTERP_BILINEAR);
                        break;
                    case BICUBIC:
                        interp = Interpolation.getInstance(Interpolation.INTERP_BICUBIC);
                        break;
                    case BICUBIC2:
                        interp = Interpolation.getInstance(Interpolation.INTERP_BICUBIC_2);
                        break;
                }
            }

            final MetadataElement absRoot = AbstractMetadata.getAbstractedMetadata(sourceProduct);
            processedSlaveBand = absRoot.getAttributeString("processed_slave");

            createTargetProduct();

        } catch (Throwable e) {
            e.printStackTrace();
            openResidualsFile = true;
            OperatorUtils.catchOperatorException(getId(), e);
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
        masterBand = sourceProduct.getBand(mstBandName);
        if (masterBand.getUnit() != null && masterBand.getUnit().equals(Unit.REAL)) {
            int mstIdx = sourceProduct.getBandIndex(mstBandName);
            if (sourceProduct.getNumBands() > mstIdx + 1) {
                masterBand2 = sourceProduct.getBandAt(mstIdx + 1);
                complexCoregistration = true;
            }
        }
    }

    private void addSlaveGCPs(final WarpData warpData, final String bandName) {
        final GeoCoding targetGeoCoding = targetProduct.getSceneGeoCoding();
        final ProductNodeGroup<Placemark> targetGCPGroup = GCPManager.instance().getGcpGroup(targetProduct.getBand(bandName));
        targetGCPGroup.removeAll();

        for (int i = 0; i < warpData.slaveGCPList.size(); ++i) {
            final Placemark sPin = warpData.slaveGCPList.get(i);
            final Placemark tPin = Placemark.createPointPlacemark(GcpDescriptor.getInstance(),
                    sPin.getName(),
                    sPin.getLabel(),
                    sPin.getDescription(),
                    sPin.getPixelPos(),
                    sPin.getGeoPos(),
                    targetGeoCoding);

            targetGCPGroup.add(tPin);
        }
    }

    private String formatName(final Band srcBand) {
        String name = srcBand.getName();
        if (excludeMaster) {  // multi-output without master
            String newName = StackUtils.getBandNameWithoutDate(name);
            if (name.equals(processedSlaveBand)) {
                processedSlaveBand = newName;
            }
            return newName;
        }
        return name;
    }

    /**
     * Create target product.
     */
    private void createTargetProduct() {
        masterBandNames = StackUtils.getMasterBandNames(sourceProduct);
        targetProduct = new Product(sourceProduct.getName(), sourceProduct.getProductType(), sourceProduct.getSceneRasterWidth(), sourceProduct.getSceneRasterHeight());
        
        final int numSrcBands = sourceProduct.getNumBands();
        int inc = 1;
        if (complexCoregistration) {
            inc = 2;
        }
        for (int i = 0; i < numSrcBands; i += inc) {
            final Band srcBand = sourceProduct.getBandAt(i);
            Band targetBand;
            if (srcBand == masterBand || srcBand == masterBand2
                    || StringUtils.contains(masterBandNames, srcBand.getName())) {
                if (excludeMaster) {
                    continue;
                }
                targetBand = ProductUtils.copyBand(srcBand.getName(), sourceProduct, targetProduct, false);
                originalImages.put(targetBand, srcBand.getSourceImage());
                targetBand.setSourceImage(srcBand.getSourceImage());
            } else {
                targetBand = targetProduct.addBand(formatName(srcBand), ProductData.TYPE_FLOAT32);
                originalImages.put(targetBand, srcBand.getSourceImage());
                ProductUtils.copyRasterDataNodeProperties(srcBand, targetBand);
            }
            sourceRasterMap.put(targetBand, srcBand);

            if (complexCoregistration) {
                final Band srcBandQ = sourceProduct.getBandAt(i + 1);
                Band targetBandQ;
                if (srcBand == masterBand || srcBand == masterBand2
                        || StringUtils.contains(masterBandNames, srcBand.getName())) {
                    targetBandQ = ProductUtils.copyBand(srcBandQ.getName(), sourceProduct, targetProduct, false);
                    originalImages.put(targetBandQ, srcBandQ.getSourceImage());
                    targetBandQ.setSourceImage(srcBandQ.getSourceImage());
                } else {
                    targetBandQ = targetProduct.addBand(formatName(srcBandQ), ProductData.TYPE_FLOAT32);
                    originalImages.put(targetBandQ, srcBandQ.getSourceImage());
                    ProductUtils.copyRasterDataNodeProperties(srcBandQ, targetBandQ);
                }
                sourceRasterMap.put(targetBandQ, srcBandQ);

                complexSrcMap.put(srcBandQ, srcBand);
                String suffix = "";
                if (excludeMaster) { // multi-output without master
                    String pol = OperatorUtils.getPolarizationFromBandName(srcBand.getName());
                    if (pol != null && !pol.isEmpty()) {
                        suffix = '_' + pol.toUpperCase();
                    }
                } else {
                    suffix = '_' + OperatorUtils.getSuffixFromBandName(srcBand.getName());
                }
                ReaderUtils.createVirtualIntensityBand(targetProduct, targetBand, targetBandQ, suffix);
            }
        }

        // co-registered image should have the same geo-coding as the master image
        ProductUtils.copyProductNodes(sourceProduct, targetProduct);
        updateTargetProductMetadata();
    }

    /**
     * Update metadata in the target product.
     */
    private void updateTargetProductMetadata() {
        final MetadataElement absTgt = AbstractMetadata.getAbstractedMetadata(targetProduct);
        if (excludeMaster) {
            final String[] slaveNames = StackUtils.getSlaveProductNames(sourceProduct);
            absTgt.setAttributeString(AbstractMetadata.PRODUCT, slaveNames[0]);

            final ProductData.UTC[] times = StackUtils.getProductTimes(sourceProduct);
            targetProduct.setStartTime(times[1]);

            double lineTimeInterval = absTgt.getAttributeDouble(AbstractMetadata.line_time_interval);
            int height = sourceProduct.getSceneRasterHeight();
            ProductData.UTC endTime = new ProductData.UTC(times[1].getMJD() + (lineTimeInterval * height) / Constants.secondsInDay);
            targetProduct.setEndTime(endTime);

        } else {
            // only if its a full coregistered stack including master band
            AbstractMetadata.setAttribute(absTgt, AbstractMetadata.coregistered_stack, 1);
        }
    }

    /**
     * Called by the framework in order to compute a tile for the given target
     * band.
     * <p>
     * The default implementation throws a runtime exception with the message
     * "not implemented".</p>
     *
     * @param targetBand The target band.
     * @param targetTile The current tile associated with the target band to be
     * computed.
     * @param pm A progress monitor which should be used to determine
     * computation cancelation requests.
     * @throws OperatorException If an error occurs during computation of the
     * target raster.
     */
    @Override
    public void computeTile(Band targetBand, Tile targetTile) throws OperatorException {
        final Rectangle targetRectangle = targetTile.getRectangle();
        final int x0 = targetRectangle.x;
        final int y0 = targetRectangle.y;
        final int w = targetRectangle.width;
        final int h = targetRectangle.height;

        try {
            if (!warpDataAvailable) { // called just once to compute warp function
                getWarpData(targetRectangle);
            }

            final Band srcBand = sourceRasterMap.get(targetBand);
            if (srcBand == null) {
                return;
            }
            
            Band realSrcBand = complexSrcMap.get(srcBand);
            if (realSrcBand == null) {
                realSrcBand = srcBand;
            }

            // create source image
            final Tile sourceRaster = getSourceTile(srcBand, targetRectangle);

            final WarpData warpData = warpDataMap.get(realSrcBand);
            if (warpData.notEnoughGCPs) {
                return;
            }

            final RenderedImage srcImage = sourceRaster.getRasterDataNode().getSourceImage();

            // get warped image
            final RenderedOp warpedImage = createWarpImage(warpData.jaiWarp, srcImage);
     //       WarpBilinearOpImage w=(WarpBilinearOpImage) warpedImage.getRendering();
            // copy warped image data to target
            final float[] dataArray = warpedImage.getData(targetRectangle).getSamples(x0, y0, w, h, 0, (float[]) null);

            targetTile.setRawSamples(ProductData.createInstance(dataArray));

        } catch (Throwable e) {
            e.printStackTrace();
            OperatorUtils.catchOperatorException(getId(), e);
        } 
    }

    /**
     * Compute WARP polynomial function using master and slave GCP pairs.
     *
     * @param warpData Stores the warp information per band.
     * @param warpPolynomialOrder The WARP polynimal order.
     * @param masterGCPGroup The master GCPs.
     */
    public static void computeWARPPolynomial(
            final WarpData warpData, final int warpPolynomialOrder, final ProductNodeGroup<Placemark> masterGCPGroup) {

        getNumOfValidGCPs(warpData, warpPolynomialOrder);

        getMasterAndSlaveGCPCoordinates(warpData, masterGCPGroup);
        if (warpData.notEnoughGCPs) {
            return;
        }

        warpData.computeWARP(warpPolynomialOrder);

        computeRMS(warpData, warpPolynomialOrder);
    }
    public synchronized void getWarpData() throws OperatorException {

        if (warpDataAvailable) {
            return;
        }

        // find first real slave band
        final Band targetBand = targetProduct.getBand(processedSlaveBand);
        
        // for all slave bands or band pairs compute a warp
        final int numSrcBands = sourceProduct.getNumBands();
        int inc = 1;
        if (complexCoregistration) {
            inc = 2;
        }

        final ProductNodeGroup<Placemark> masterGCPGroup = GCPManager.instance().getGcpGroup(masterBand);

        boolean appendFlag = false;
        for (int i = 0; i < numSrcBands; i += inc) {

            final Band srcBand = sourceProduct.getBandAt(i);
            if (srcBand == masterBand || srcBand == masterBand2
                    || StringUtils.contains(masterBandNames, srcBand.getName())) {
                continue;
            }

            ProductNodeGroup<Placemark> slaveGCPGroup = GCPManager.instance().getGcpGroup(srcBand);
            if (slaveGCPGroup.getNodeCount() < 3) {
                // find others for same slave product
                final String slvProductName = StackUtils.getSlaveProductName(sourceProduct, srcBand, null);
                for (Band band : sourceProduct.getBands()) {
                    if (band != srcBand) {
                        final String productName = StackUtils.getSlaveProductName(sourceProduct, band, null);
                        if (slvProductName != null && slvProductName.equals(productName)) {
                            slaveGCPGroup = GCPManager.instance().getGcpGroup(band);
                            if (slaveGCPGroup.getNodeCount() >= 3) {
                                break;
                            }
                        }
                    }
                }
            }
            final WarpData warpData = new WarpData(slaveGCPGroup);
            warpDataMap.put(srcBand, warpData);

            if (slaveGCPGroup.getNodeCount() < 3) {
                warpData.notEnoughGCPs = true;
                continue;
            }

            computeWARPPolynomialFromGCPs(sourceProduct, srcBand, warpPolynomialOrder, masterGCPGroup, maxIterations,
                    rmsThreshold, appendFlag, warpData);

            if (warpData.notEnoughGCPs) {
                continue;
            }

            if (!appendFlag) {
                appendFlag = true;
            }

            addSlaveGCPs(warpData, targetBand.getName());
        }

        announceGCPWarning();

        GCPManager.instance().removeAllGcpGroups();

        if (openResidualsFile) {
            final File residualsFile = getResidualsFile(sourceProduct);
            if (Desktop.isDesktopSupported() && residualsFile.exists()) {
                try {
                    Desktop.getDesktop().open(residualsFile);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Error opening residuals file " + e.getMessage());
                    // do nothing
                }
            }
        }

        // update metadata
        writeWarpDataToMetadata();

        warpDataAvailable = true;
    }
    private synchronized void getWarpData(final Rectangle targetRectangle) throws OperatorException {
        if (warpDataAvailable) {
            return;
        }

        // find first real slave band
        final Band targetBand = targetProduct.getBand(processedSlaveBand);
        // force getSourceTile to computeTiles on GCPSelection
//        final Tile sourceRaster = getSourceTile(sourceRasterMap.get(targetBand), targetRectangle);

        // for all slave bands or band pairs compute a warp
        final int numSrcBands = sourceProduct.getNumBands();
        int inc = 1;
        if (complexCoregistration) {
            inc = 2;
        }

        final ProductNodeGroup<Placemark> masterGCPGroup = GCPManager.instance().getGcpGroup(masterBand);

        boolean appendFlag = false;
        for (int i = 0; i < numSrcBands; i += inc) {
            final Band srcBand = sourceProduct.getBandAt(i);
            if (srcBand == masterBand || srcBand == masterBand2
                    || StringUtils.contains(masterBandNames, srcBand.getName())) {
                continue;
            }

            ProductNodeGroup<Placemark> slaveGCPGroup = GCPManager.instance().getGcpGroup(srcBand);
            if (slaveGCPGroup.getNodeCount() < 3) {
                // find others for same slave product
                final String slvProductName = StackUtils.getSlaveProductName(sourceProduct, srcBand, null);
                for (Band band : sourceProduct.getBands()) {
                    if (band != srcBand) {
                        final String productName = StackUtils.getSlaveProductName(sourceProduct, band, null);
                        if (slvProductName != null && slvProductName.equals(productName)) {
                            slaveGCPGroup = GCPManager.instance().getGcpGroup(band);
                            if (slaveGCPGroup.getNodeCount() >= 3) {
                                break;
                            }
                        }
                    }
                }
            }
            final WarpData warpData = new WarpData(slaveGCPGroup);
            warpDataMap.put(srcBand, warpData);

            if (slaveGCPGroup.getNodeCount() < 3) {
                warpData.notEnoughGCPs = true;
                continue;
            }

            computeWARPPolynomialFromGCPs(sourceProduct, srcBand, warpPolynomialOrder, masterGCPGroup, maxIterations,
                    rmsThreshold, appendFlag, warpData);

            if (warpData.notEnoughGCPs) {
                continue;
            }

            if (!appendFlag) {
                appendFlag = true;
            }

            addSlaveGCPs(warpData, targetBand.getName());
        }

        announceGCPWarning();

        GCPManager.instance().removeAllGcpGroups();

        if (openResidualsFile) {
            final File residualsFile = getResidualsFile(sourceProduct);
            if (Desktop.isDesktopSupported() && residualsFile.exists()) {
                try {
                    Desktop.getDesktop().open(residualsFile);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Error opening residuals file " + e.getMessage());
                    // do nothing
                }
            }
        }

        // update metadata
        writeWarpDataToMetadata();

        warpDataAvailable = true;
    }

    public static void computeWARPPolynomialFromGCPs(
            final Product sourceProduct, final Band srcBand, final int warpPolynomialOrder,
            final ProductNodeGroup<Placemark> masterGCPGroup, final int maxIterations, final float rmsThreshold,
            final boolean appendFlag, WarpData warpData) {
        boolean append;
        float threshold = 0.0f;
        for (int iter = 0; iter < maxIterations; iter++) {
            if (iter == 0) {
                append = appendFlag;
            } else {
                append = true;

                if (iter < maxIterations - 1 && warpData.rmsMean > rmsThreshold) {
                    threshold = (float) (warpData.rmsMean + warpData.rmsStd);
                } else {
                    threshold = rmsThreshold;
                }
            }

            if (threshold > 0.0f) {
                eliminateGCPsBasedOnRMS(warpData, threshold);
            }

            computeWARPPolynomial(warpData, warpPolynomialOrder, masterGCPGroup);

            outputCoRegistrationInfo(
                    sourceProduct, warpPolynomialOrder, warpData, append, threshold, iter, srcBand.getName());

            if (warpData.notEnoughGCPs || iter > 0 && threshold <= rmsThreshold) {
                break;
            }
        }
    }

    private void writeWarpDataToMetadata() {
        final MetadataElement absRoot = AbstractMetadata.getAbstractedMetadata(targetProduct);
        final Set<Band> bandSet = warpDataMap.keySet();

        for (Band band : bandSet) {
            final MetadataElement bandElem = AbstractMetadata.getBandAbsMetadata(absRoot, band.getName(), true);

            MetadataElement warpDataElem = bandElem.getElement("WarpData");
            if (warpDataElem == null) {
                warpDataElem = new MetadataElement("WarpData");
                bandElem.addElement(warpDataElem);
            } else {
                // empty out element
                final MetadataAttribute[] attribList = warpDataElem.getAttributes();
                for (MetadataAttribute attrib : attribList) {
                    warpDataElem.removeAttribute(attrib);
                }
            }

            final WarpData warpData = warpDataMap.get(band);
            if (warpData.numValidGCPs > 0) {
                for (int i = 0; i < warpData.numValidGCPs; i++) {
                    final MetadataElement gcpElem = new MetadataElement("GCP" + i);
                    warpDataElem.addElement(gcpElem);

                    gcpElem.setAttributeDouble("mst_x", warpData.masterGCPCoords[2 * i]);
                    gcpElem.setAttributeDouble("mst_y", warpData.masterGCPCoords[2 * i + 1]);

                    gcpElem.setAttributeDouble("slv_x", warpData.slaveGCPCoords[2 * i]);
                    gcpElem.setAttributeDouble("slv_y", warpData.slaveGCPCoords[2 * i + 1]);

                    if (!warpData.notEnoughGCPs) {
                        gcpElem.setAttributeDouble("rms", warpData.rms[i]);
                    }
                }
            }
        }
    }

    /**
     * Get the number of valid GCPs.
     *
     * @param warpData Stores the warp information per band.
     * @param warpPolynomialOrder The WARP polynimal order.
     * @throws OperatorException The exceptions.
     */
    private static void getNumOfValidGCPs(
            final WarpData warpData, final int warpPolynomialOrder) throws OperatorException {

        warpData.numValidGCPs = warpData.slaveGCPList.size();
        final int requiredGCPs = (warpPolynomialOrder + 2) * (warpPolynomialOrder + 1) / 2;
        if (warpData.numValidGCPs < requiredGCPs) {
            warpData.notEnoughGCPs = true;
            //throw new OperatorException("Order " + warpPolynomialOrder + " requires " + requiredGCPs +
            //        " GCPs, valid GCPs are " + warpData.numValidGCPs + ", try a larger RMS threshold.");
        }
    }

    /**
     * Get GCP coordinates for master and slave bands.
     *
     * @param warpData Stores the warp information per band.
     * @param masterGCPGroup The master GCPs.
     */
    private static void getMasterAndSlaveGCPCoordinates(
            final WarpData warpData, final ProductNodeGroup<Placemark> masterGCPGroup) {

        warpData.masterGCPCoords = new float[2 * warpData.numValidGCPs];
        warpData.slaveGCPCoords = new float[2 * warpData.numValidGCPs];

        for (int i = 0; i < warpData.numValidGCPs; ++i) {

            final Placemark sPin = warpData.slaveGCPList.get(i);
            final PixelPos sGCPPos = sPin.getPixelPos();

            final Placemark mPin = masterGCPGroup.get(sPin.getName());
            final PixelPos mGCPPos = mPin.getPixelPos();

            final int j = 2 * i;
            warpData.masterGCPCoords[j] = (float) mGCPPos.x;
            warpData.masterGCPCoords[j + 1] = (float) mGCPPos.y;
            warpData.slaveGCPCoords[j] = (float) sGCPPos.x;
            warpData.slaveGCPCoords[j + 1] = (float) sGCPPos.y;
        }
    }

    /**
     * Compute root mean square error of the warped GCPs for given WARP function
     * and given GCPs.
     *
     * @param warpData Stores the warp information per band.
     * @param warpPolynomialOrder The WARP polynimal order.
     */
    private static void computeRMS(final WarpData warpData, final int warpPolynomialOrder) {
        // compute RMS for all valid GCPs
        warpData.rms = new float[warpData.numValidGCPs];
        warpData.colResiduals = new float[warpData.numValidGCPs];
        warpData.rowResiduals = new float[warpData.numValidGCPs];
        final PixelPos slavePos = new PixelPos(0.0f, 0.0f);
        for (int i = 0; i < warpData.rms.length; i++) {
            final int i2 = 2 * i;
            getWarpedCoords(warpData,
                    warpPolynomialOrder,
                    warpData.masterGCPCoords[i2],
                    warpData.masterGCPCoords[i2 + 1],
                    slavePos);
            final double dX = slavePos.x - warpData.slaveGCPCoords[i2];
            final double dY = slavePos.y - warpData.slaveGCPCoords[i2 + 1];
            warpData.colResiduals[i] = (float) dX;
            warpData.rowResiduals[i] = (float) dY;
            warpData.rms[i] = (float) Math.sqrt(dX * dX + dY * dY);
        }

        // compute some statistics
        warpData.rmsMean = 0.0;
        warpData.rowResidualMean = 0.0;
        warpData.colResidualMean = 0.0;
        double rms2Mean = 0.0;
        double rowResidual2Mean = 0.0;
        double colResidual2Mean = 0.0;

        for (int i = 0; i < warpData.rms.length; i++) {
            warpData.rmsMean += warpData.rms[i];
            rms2Mean += warpData.rms[i] * warpData.rms[i];
            warpData.rowResidualMean += warpData.rowResiduals[i];
            rowResidual2Mean += warpData.rowResiduals[i] * warpData.rowResiduals[i];
            warpData.colResidualMean += warpData.colResiduals[i];
            colResidual2Mean += warpData.colResiduals[i] * warpData.colResiduals[i];
        }
        warpData.rmsMean /= warpData.rms.length;
        rms2Mean /= warpData.rms.length;
        warpData.rowResidualMean /= warpData.rms.length;
        rowResidual2Mean /= warpData.rms.length;
        warpData.colResidualMean /= warpData.rms.length;
        colResidual2Mean /= warpData.rms.length;

        warpData.rmsStd = Math.sqrt(rms2Mean - warpData.rmsMean * warpData.rmsMean);
        warpData.rowResidualStd = Math.sqrt(rowResidual2Mean - warpData.rowResidualMean * warpData.rowResidualMean);
        warpData.colResidualStd = Math.sqrt(colResidual2Mean - warpData.colResidualMean * warpData.colResidualMean);
    }

    private void constructInterpolationTable(String interpolationMethod) {
        // construct interpolation LUT
        SimpleLUT lut = new SimpleLUT(interpolationMethod);
        lut.constructLUT();

        int kernelLength = lut.getKernelLength();

        // get LUT and cast it to float for JAI
        double[] lutArrayDoubles = lut.getKernelAsArray();
        float lutArrayFloats[] = new float[lutArrayDoubles.length];
        int i = 0;
        for (double lutElement : lutArrayDoubles) {
            lutArrayFloats[i++] = (float) lutElement;
        }

        // construct interpolation table for JAI resampling
        final int subsampleBits = 7;
        final int precisionBits = 32;
        int padding = kernelLength / 2 - 1;

        interpTable = new InterpolationTable(padding, kernelLength, subsampleBits, precisionBits, lutArrayFloats);
    }

    /**
     * Eliminate master and slave GCP pairs that have root mean square error
     * greater than given threshold.
     *
     * @param warpData Stores the warp information per band.
     * @param threshold Threshold for eliminating GCPs.
     * @return True if some GCPs are eliminated, false otherwise.
     */
    public static boolean eliminateGCPsBasedOnRMS(final WarpData warpData, final float threshold) {
        final List<Placemark> pinList = new ArrayList<>();
        if (warpData.slaveGCPList.size() < warpData.rms.length) {
            warpData.notEnoughGCPs = true;
            return true;
        }
        for (int i = 0; i < warpData.rms.length; i++) {
            if (warpData.rms[i] >= threshold) {
                pinList.add(warpData.slaveGCPList.get(i));
            }
        }

        for (Placemark aPin : pinList) {
            warpData.slaveGCPList.remove(aPin);
        }

        return !pinList.isEmpty();
    }

    /**
     * Compute warped GCPs.
     *
     * @param warpData The WARP polynomial.
     * @param warpPolynomialOrder The WARP polynomial order.
     * @param mX The x coordinate of master GCP.
     * @param mY The y coordinate of master GCP.
     * @param slavePos The warped GCP position.
     * @throws OperatorException The exceptions.
     */
    public static void getWarpedCoords(final WarpData warpData, final int warpPolynomialOrder,
            final double mX, final double mY, final PixelPos slavePos)
            throws OperatorException {
        final double[] xCoeffs = warpData.xCoef;
        final double[] yCoeffs = warpData.yCoef;
        if (xCoeffs.length != yCoeffs.length) {
            throw new OperatorException("WARP has different number of coefficients for X and Y");
        }

        final int numOfCoeffs = xCoeffs.length;
        switch (warpPolynomialOrder) {
            case 1: {
                if (numOfCoeffs != 3) {
                    throw new OperatorException("Number of WARP coefficients do not match WARP degree");
                }
                slavePos.x = (float) (xCoeffs[0] + xCoeffs[1] * mX + xCoeffs[2] * mY);

                slavePos.y = (float) (yCoeffs[0] + yCoeffs[1] * mX + yCoeffs[2] * mY);
                break;
            }
            case 2: {
                if (numOfCoeffs != 6) {
                    throw new OperatorException("Number of WARP coefficients do not match WARP degree");
                }
                final double mXmX = mX * mX;
                final double mXmY = mX * mY;
                final double mYmY = mY * mY;

                slavePos.x = (float) (xCoeffs[0] + xCoeffs[1] * mX + xCoeffs[2] * mY
                        + xCoeffs[3] * mXmX + xCoeffs[4] * mXmY + xCoeffs[5] * mYmY);

                slavePos.y = (float) (yCoeffs[0] + yCoeffs[1] * mX + yCoeffs[2] * mY
                        + yCoeffs[3] * mXmX + yCoeffs[4] * mXmY + yCoeffs[5] * mYmY);
                break;
            }
            case 3: {
                if (numOfCoeffs != 10) {
                    throw new OperatorException("Number of WARP coefficients do not match WARP degree");
                }
                final double mXmX = mX * mX;
                final double mXmY = mX * mY;
                final double mYmY = mY * mY;

                slavePos.x = (float) (xCoeffs[0] + xCoeffs[1] * mX + xCoeffs[2] * mY
                        + xCoeffs[3] * mXmX + xCoeffs[4] * mXmY + xCoeffs[5] * mYmY
                        + xCoeffs[6] * mXmX * mX + xCoeffs[7] * mX * mXmY + xCoeffs[8] * mXmY * mY + xCoeffs[9] * mYmY * mY);

                slavePos.y = (float) (yCoeffs[0] + yCoeffs[1] * mX + yCoeffs[2] * mY
                        + yCoeffs[3] * mXmX + yCoeffs[4] * mXmY + yCoeffs[5] * mYmY
                        + yCoeffs[6] * mXmX * mX + yCoeffs[7] * mX * mXmY + yCoeffs[8] * mXmY * mY + yCoeffs[9] * mYmY * mY);
                break;
            }
            default:
                throw new OperatorException("Incorrect WARP degree");
        }
    }

    /**
     * Output co-registration information to file.
     *
     * @param sourceProduct The source product.
     * @param warpPolynomialOrder The order of Warp polinomial.
     * @param warpData Stores the warp information per band.
     * @param appendFlag Boolean flag indicating if the information is output to
     * file in appending mode.
     * @param threshold The threshold for elinimating GCPs.
     * @param parseIndex Index for parsing GCPs.
     * @param bandName the band name
     * @throws OperatorException The exceptions.
     */
    public static void outputCoRegistrationInfo(final Product sourceProduct, final int warpPolynomialOrder,
            final WarpData warpData, final boolean appendFlag,
            final float threshold, final int parseIndex, final String bandName)
            throws OperatorException {
        final File residualFile = getResidualsFile(sourceProduct);
        PrintStream p = null; // declare a print stream object

        try {
            final FileOutputStream out = new FileOutputStream(residualFile.getAbsolutePath(), appendFlag);

            // Connect print stream to the output stream
            p = new PrintStream(out);

            p.println();

            if (!appendFlag) {
                p.println();
                p.format("Transformation degree = %d", warpPolynomialOrder);
                p.println();
            }

            p.println();
            p.print("------------------------ Band: " + bandName + " (Parse " + parseIndex + ") ------------------------");
            p.println();

            if (!warpData.notEnoughGCPs) {
                p.println();
                p.println("WARP coefficients:");
                for (double xCoeff : warpData.xCoef) {
                    p.print((float) xCoeff + ", ");
                }

                p.println();
                for (double yCoeff : warpData.yCoef) {
                    p.print((float) yCoeff + ", ");
                }
                p.println();
            }

            if (appendFlag) {
                p.println();
                p.format("RMS Threshold: %5.3f", threshold);
                p.println();
            }

            p.println();
            if (appendFlag) {
                p.print("Valid GCPs after parse " + parseIndex + " :");
            } else {
                p.print("Initial Valid GCPs:");
            }
            p.println();

            if (!warpData.notEnoughGCPs) {

                p.println();
                p.println("  No.  | Master GCP x | Master GCP y | Slave GCP x  | Slave GCP y  | Row Residual | Col Residual |        RMS        |");
                p.println("----------------------------------------------------------------------------------------------------------------------");
                for (int i = 0; i < warpData.rms.length; i++) {
                    p.format("%6d |%13.3f |%13.3f |%13.3f |%13.3f |%13.8f |%13.8f |%18.12f |",
                            i, warpData.masterGCPCoords[2 * i], warpData.masterGCPCoords[2 * i + 1],
                            warpData.slaveGCPCoords[2 * i], warpData.slaveGCPCoords[2 * i + 1],
                            warpData.rowResiduals[i], warpData.colResiduals[i], warpData.rms[i]);
                    p.println();
                }

                p.println();
                p.print("Row residual mean = " + warpData.rowResidualMean);
                p.println();
                p.print("Row residual std = " + warpData.rowResidualStd);
                p.println();

                p.println();
                p.print("Col residual mean = " + warpData.colResidualMean);
                p.println();
                p.print("Col residual std = " + warpData.colResidualStd);
                p.println();

                p.println();
                p.print("RMS mean = " + warpData.rmsMean);
                p.println();
                p.print("RMS std = " + warpData.rmsStd);
                p.println();

            } else {

                p.println();
                p.println("No. | Master GCP x | Master GCP y | Slave GCP x | Slave GCP y |");
                p.println("---------------------------------------------------------------");
                for (int i = 0; i < warpData.numValidGCPs; i++) {
                    p.format("%2d  |%13.3f |%13.3f |%12.3f |%12.3f |",
                            i, warpData.masterGCPCoords[2 * i], warpData.masterGCPCoords[2 * i + 1],
                            warpData.slaveGCPCoords[2 * i], warpData.slaveGCPCoords[2 * i + 1]);
                    p.println();
                }
            }
            p.println();
            p.println();

        } catch (IOException exc) {
            exc.printStackTrace();
            throw new OperatorException(exc);
        } finally {
            if (p != null) {
                p.close();
            }
        }
    }

    private static File getResidualsFile(final Product sourceProduct) {
        final String fileName = sourceProduct.getName() + "_residual.txt";
        return new File(ResourceUtils.getReportFolder(), fileName);
    }

    /**
     * Create warped image.
     *
     * @param warp The WARP polynomial.
     * @param srcImage The source image.
     * @return The warped image.
     */
    public RenderedOp createWarpImage(WarpPolynomial warp, final RenderedImage srcImage) {

        // reformat source image by casting pixel values from ushort to float
        final ParameterBlock pb1 = new ParameterBlock();
        pb1.addSource(srcImage);
        pb1.add(DataBuffer.TYPE_FLOAT);
        final RenderedImage srcImageFloat = JAI.create("format", pb1);

        // get warped image
        final ParameterBlock pb2 = new ParameterBlock();
        pb2.addSource(srcImageFloat);
        pb2.add(warp);

        if (interp != null) {
            pb2.add(interp);
        } else if (interpTable != null) {
            pb2.add(interpTable);
        }

        return JAI.create("warp", pb2);
    }

    public static class WarpData {
        public final List<Placemark> slaveGCPList = new ArrayList<>();
        public WarpPolynomial jaiWarp = null;
        public double[] xCoef = null;
        public double[] yCoef = null;

        public int numValidGCPs = 0;
        public boolean notEnoughGCPs = false;
        public float[] rms = null;
        public float[] rowResiduals = null;
        public float[] colResiduals = null;
        public float[] masterGCPCoords = null;
        public float[] slaveGCPCoords = null;

        public double rmsStd = 0;
        public double rmsMean = 0;
        public double rowResidualStd = 0;
        public double rowResidualMean = 0;
        public double colResidualStd = 0;
        public double colResidualMean = 0;

        public WarpData(ProductNodeGroup<Placemark> slaveGCPGroup) {
            for (int i = 0; i < slaveGCPGroup.getNodeCount(); ++i) {
                slaveGCPList.add(slaveGCPGroup.get(i));
            }
        }

        /**
         * Compute WARP function using master and slave GCPs.
         *
         * @param warpPolynomialOrder The WARP polynimal order.
         */
        public void computeWARP(final int warpPolynomialOrder) {

            // check if master and slave GCP coordinates are identical, if yes set the warp polynomial coefficients
            // directly, no need to compute them using JAI function because JAI produces incorrect result due to ill
            // conditioned matrix.
            float sum = 0.0f;
            for (int i = 0; i < slaveGCPCoords.length; i++) {
                sum += Math.abs(slaveGCPCoords[i] - masterGCPCoords[i]);
            }
            if (sum < 0.01) {
                switch (warpPolynomialOrder) {
                    case 1: {
                        xCoef = new double[3];
                        yCoef = new double[3];
                        xCoef[0] = 0;
                        xCoef[1] = 1;
                        xCoef[2] = 0;
                        yCoef[0] = 0;
                        yCoef[1] = 0;
                        yCoef[2] = 1;
                        break;
                    }
                    case 2: {
                        xCoef = new double[6];
                        yCoef = new double[6];
                        xCoef[0] = 0;
                        xCoef[1] = 1;
                        xCoef[2] = 0;
                        xCoef[3] = 0;
                        xCoef[4] = 0;
                        xCoef[5] = 0;
                        yCoef[0] = 0;
                        yCoef[1] = 0;
                        yCoef[2] = 1;
                        yCoef[3] = 0;
                        yCoef[4] = 0;
                        yCoef[5] = 0;
                        break;
                    }
                    case 3: {
                        xCoef = new double[10];
                        yCoef = new double[10];
                        xCoef[0] = 0;
                        xCoef[1] = 1;
                        xCoef[2] = 0;
                        xCoef[3] = 0;
                        xCoef[4] = 0;
                        xCoef[5] = 0;
                        xCoef[6] = 0;
                        xCoef[7] = 0;
                        xCoef[8] = 0;
                        xCoef[9] = 0;
                        yCoef[0] = 0;
                        yCoef[1] = 0;
                        yCoef[2] = 1;
                        yCoef[3] = 0;
                        yCoef[4] = 0;
                        yCoef[5] = 0;
                        yCoef[6] = 0;
                        yCoef[7] = 0;
                        yCoef[8] = 0;
                        yCoef[9] = 0;
                        break;
                    }
                    default:
                        throw new OperatorException("Incorrect WARP degree");
                }
                return;
            }

            jaiWarp = WarpPolynomial.createWarp(slaveGCPCoords, //source
                    0,
                    masterGCPCoords, // destination
                    0,
                    2 * numValidGCPs,
                    1.0F,
                    1.0F,
                    1.0F,
                    1.0F,
                    warpPolynomialOrder);

            final float[] jaiXCoefs = jaiWarp.getXCoeffs();
            final float[] jaiYCoefs = jaiWarp.getYCoeffs();
            final int size = jaiXCoefs.length;
            xCoef = new double[size];
            yCoef = new double[size];
            for (int i = 0; i < size; ++i) {
                xCoef[i] = jaiXCoefs[i];
                yCoef[i] = jaiYCoefs[i];
            }
        }
    }

    private void announceGCPWarning() {
        String msg = "";
        for (Band srcBand : sourceProduct.getBands()) {
            final WarpData warpData = warpDataMap.get(srcBand);
            if (warpData != null && warpData.notEnoughGCPs) {
                msg += srcBand.getName() + " does not have enough valid GCPs for the warp\n";
                openResidualsFile = true;
            }
        }
        if (!msg.isEmpty()) {
            System.out.println(msg);
        }
    }

    public float getRmsThreshold() {
        return rmsThreshold;
    }

    public void setRmsThreshold(float rmsThreshold) {
        this.rmsThreshold = rmsThreshold;
    }

    public int getWarpPolynomialOrder() {
        return warpPolynomialOrder;
    }

    public void setWarpPolynomialOrder(int warpPolynomialOrder) {
        this.warpPolynomialOrder = warpPolynomialOrder;
    }

    public String getInterpolationMethod() {
        return interpolationMethod;
    }

    public void setInterpolationMethod(String interpolationMethod) {
        this.interpolationMethod = interpolationMethod;
    }

    public boolean isExcludeMaster() {
        return excludeMaster;
    }

    public void setExcludeMaster(boolean excludeMaster) {
        this.excludeMaster = excludeMaster;
    }

    public Boolean getOpenResidualsFile() {
        return openResidualsFile;
    }

    public void setOpenResidualsFile(Boolean openResidualsFile) {
        this.openResidualsFile = openResidualsFile;
    }

    public Product getSourceProduct() {
        return sourceProduct;
    }

    public void setSourceProduct(Product sourceProduct) {
        this.sourceProduct = sourceProduct;
    }

	public Interpolation getInterp() {
		return interp;
	}

	public void setInterp(Interpolation interp) {
		this.interp = interp;
	}

	public InterpolationTable getInterpTable() {
		return interpTable;
	}

	public void setInterpTable(InterpolationTable interpTable) {
		this.interpTable = interpTable;
	}

	public Map<Band, Band> getComplexSrcMap() {
		return complexSrcMap;
	}

	public Map<Band, WarpData> getWarpDataMap() {
		return warpDataMap;
	}

	public Map<Band, Band> getSourceRasterMap() {
		return sourceRasterMap;
	}
}
