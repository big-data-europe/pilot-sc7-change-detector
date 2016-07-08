package eu.bde.sc7pilot.taskbased.calibration;

import java.io.File;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.VirtualBand;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.Tile;
import org.esa.snap.engine_utilities.datamodel.AbstractMetadata;
import org.esa.snap.engine_utilities.gpf.InputProductValidator;
import org.esa.snap.engine_utilities.gpf.OperatorUtils;
import org.esa.snap.engine_utilities.gpf.StackUtils;
import eu.bde.sc7pilot.taskbased.AbstractOperator;

public class MyCalibration extends AbstractOperator {

    private Product sourceProduct;
    private String[] sourceBandNames;
    private String auxFile = LATEST_AUX;
    private File externalAuxFile = null;
    private Boolean outputImageInComplex = false;
    private Boolean outputImageScaleInDb = false;
    private Boolean createGammaBand = false;
    private Boolean createBetaBand = false;
    private String[] selectedPolarisations;
    private Boolean outputSigmaBand = true;
    private Boolean outputGammaBand = false;
    private Boolean outputBetaBand = false;
    private Boolean outputDNBand = false;

    private MyCalibrator calibrator = null;

    public static final String PRODUCT_AUX = "Product Auxiliary File";
    public static final String LATEST_AUX = "Latest Auxiliary File";
    public static final String EXTERNAL_AUX = "External Auxiliary File";

    public MyCalibration(String auxFile, Boolean[] bParams, String[] selectedPolarisations) {
        if (auxFile != null) {
            this.externalAuxFile = new File(auxFile);
        }

        this.outputImageInComplex = bParams[0];
        this.outputImageScaleInDb = bParams[1];
        this.createGammaBand = bParams[2];
        this.createBetaBand = bParams[3];
        this.outputSigmaBand = bParams[4];
        this.outputGammaBand = bParams[5];
        this.outputBetaBand = bParams[6];
        this.outputDNBand = bParams[7];
    }

    @Override
    public void initialize() throws OperatorException {
        try {
            final InputProductValidator validator = new InputProductValidator(sourceProduct);
            validator.checkIfSARProduct();

            if (StackUtils.isCoregisteredStack(sourceProduct)) {
                throw new OperatorException("Cannot apply calibration to coregistered product.");
            }

            calibrator = MyCalibrationFactory.createCalibrator(sourceProduct);
            calibrator.setAuxFileFlag(auxFile);
            calibrator.setExternalAuxFile(externalAuxFile);
            calibrator.setOutputImageInComplex(outputImageInComplex);
            calibrator.setOutputImageIndB(outputImageScaleInDb);
            ((MyBaseCalibrator) calibrator).setOriginalImages(originalImages);
            if (calibrator instanceof MySentinel1Calibrator) {
                MySentinel1Calibrator cal = (MySentinel1Calibrator) calibrator;
                cal.setUserSelections(sourceProduct,
                        selectedPolarisations, outputSigmaBand, outputGammaBand, outputBetaBand, outputDNBand);
            }
            targetProduct = calibrator.createTargetProduct(sourceProduct, sourceBandNames);
            calibrator.initialize(this, sourceProduct, targetProduct, false, true);

            if (createGammaBand) {
                createGammaVirtualBand(targetProduct, outputImageScaleInDb);
            }

            if (createBetaBand) {
                createBetaVirtualBand(targetProduct, outputImageScaleInDb);
            }

            updateTargetProductMetadata();

        } catch (Throwable e) {
            OperatorUtils.catchOperatorException(getId(), e);
        }
    }

    /**
     * Update the metadata in the target product.
     */
    private void updateTargetProductMetadata() {

        final MetadataElement absRoot = AbstractMetadata.getAbstractedMetadata(targetProduct);
        absRoot.getAttribute(AbstractMetadata.abs_calibration_flag).getData().setElemBoolean(true);

        if (!outputImageInComplex) {
            absRoot.setAttributeString(AbstractMetadata.SAMPLE_TYPE, "DETECTED");
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
        try {
            calibrator.computeTile(targetBand, targetTile);
        } catch (Throwable e) {
            OperatorUtils.catchOperatorException(getId(), e);
        }
    }

    public static void createGammaVirtualBand(final Product trgProduct, final boolean outputImageScaleInDb) {

        int count = 1;
        final Band[] bands = trgProduct.getBands();
        for (Band trgBand : bands) {

            final String unit = trgBand.getUnit();
            if (trgBand instanceof VirtualBand || (unit != null && unit.contains("phase"))) {
                continue;
            }

            final String trgBandName = trgBand.getName();
            final String expression;
            if (outputImageScaleInDb) {
                expression = "(pow(10," + trgBandName + "/10.0)" + " / cos(incident_angle * PI/180.0)) "
                        + "==0 ? 0 : 10 * log10(abs("
                        + "(pow(10," + trgBandName + "/10.0)" + " / cos(incident_angle * PI/180.0))"
                        + "))";
            } else {
                expression = trgBandName + " / cos(incident_angle * PI/180.0)";
            }
            String gammeBandName = "Gamma0";

            if (bands.length > 1) {
                if (trgBandName.contains("_HH")) {
                    gammeBandName += "_HH";
                } else if (trgBandName.contains("_VV")) {
                    gammeBandName += "_VV";
                } else if (trgBandName.contains("_HV")) {
                    gammeBandName += "_HV";
                } else if (trgBandName.contains("_VH")) {
                    gammeBandName += "_VH";
                }
            }
            if (outputImageScaleInDb) {
                gammeBandName += "_dB";
            }

            while (trgProduct.getBand(gammeBandName) != null) {
                gammeBandName += "_" + ++count;
            }

            final VirtualBand band = new VirtualBand(gammeBandName,
                    ProductData.TYPE_FLOAT32,
                    trgBand.getRasterWidth(),
                    trgBand.getRasterHeight(),
                    expression);
            band.setUnit(unit);
            band.setDescription("Gamma0 image");
            trgProduct.addBand(band);
        }
    }

    public static void createBetaVirtualBand(final Product trgProduct, final boolean outputImageScaleInDb) {

        int count = 1;
        final Band[] bands = trgProduct.getBands();
        for (Band trgBand : bands) {

            final String unit = trgBand.getUnit();
            if (trgBand instanceof VirtualBand || (unit != null && unit.contains("phase"))) {
                continue;
            }

            final String trgBandName = trgBand.getName();
            final String expression;
            if (outputImageScaleInDb) {
                expression = "(pow(10," + trgBandName + "/10.0)" + " / sin(incident_angle * PI/180.0)) "
                        + "==0 ? 0 : 10 * log10(abs("
                        + "(pow(10," + trgBandName + "/10.0)" + " / sin(incident_angle * PI/180.0))"
                        + "))";
            } else {
                expression = trgBandName + " / sin(incident_angle * PI/180.0)";
            }
            String betaBandName = "Beta0";

            if (bands.length > 1) {
                if (trgBandName.contains("_HH")) {
                    betaBandName += "_HH";
                } else if (trgBandName.contains("_VV")) {
                    betaBandName += "_VV";
                } else if (trgBandName.contains("_HV")) {
                    betaBandName += "_HV";
                } else if (trgBandName.contains("_VH")) {
                    betaBandName += "_VH";
                }
            }
            if (outputImageScaleInDb) {
                betaBandName += "_dB";
            }

            while (trgProduct.getBand(betaBandName) != null) {
                betaBandName += "_" + ++count;
            }

            final VirtualBand band = new VirtualBand(betaBandName,
                    ProductData.TYPE_FLOAT32,
                    trgBand.getRasterWidth(),
                    trgBand.getRasterHeight(),
                    expression);
            band.setUnit(unit);
            band.setDescription("Beta0 image");
            trgProduct.addBand(band);
        }
    }

    public Product getSourceProduct() {
        return sourceProduct;
    }

    public void setSourceProduct(Product sourceProduct) {
        this.sourceProduct = sourceProduct;
    }

    public MyCalibrator getCalibrator() {
        return calibrator;
    }

    public void setCalibrator(MyCalibrator calibrator) {
        this.calibrator = calibrator;
    }
}
