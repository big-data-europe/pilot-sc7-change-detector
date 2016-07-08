package eu.bde.sc7pilot.taskbased.calibration;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.VirtualBand;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.engine_utilities.datamodel.AbstractMetadata;
import org.esa.snap.engine_utilities.eo.Constants;

public class MyCalibrationFactory {

    public static MyCalibrator createCalibrator(Product sourceProduct)
            throws OperatorException, IllegalArgumentException {

        final MetadataElement absRoot = AbstractMetadata.getAbstractedMetadata(sourceProduct);
        if (absRoot == null) {
            throw new OperatorException("AbstractMetadata is null");
        }
        final String mission = absRoot.getAttributeString(AbstractMetadata.MISSION);

        if (mission.equals("ENVISAT")) {
            return new MyASARCalibrator();
        } else if (mission.contains("ERS1") || mission.contains("ERS2")) {
            return new MyERSCalibrator();
        } else if (mission.equals("ALOS") || mission.equals("ALOS2")) {
            return new MyALOSCalibrator();
        } else if (mission.equals("RS2")) {
            return new MyRadarsat2Calibrator();
        } else if (mission.contains("TSX") || mission.contains("TDX")) {
            return new MyTerraSARXCalibrator();
        } else if (mission.contains("CSK")) {
            return new MyCosmoSkymedCalibrator();
        } else if (mission.contains("SENTINEL-1")) {
            return new MySentinel1Calibrator();
        } else {
            throw new OperatorException("Mission " + mission + " is currently not supported for calibration.");
        }
    }

    //================================== Create Sigma0, Gamma0 and Beta0 virtual bands ====================================
    /**
     * Create Sigma0 image as a virtual band using incidence angle from
     * ellipsoid.
     */
    public static void createSigmaNoughtVirtualBand(final Product targetProduct, final String incidenceAngleForSigma0) {

        if (incidenceAngleForSigma0.contains(Constants.USE_PROJECTED_INCIDENCE_ANGLE_FROM_DEM)) {
            return;
        }

        final Band[] bands = targetProduct.getBands();
        for (Band trgBand : bands) {

            final String trgBandName = trgBand.getName();
            if (trgBand instanceof VirtualBand || !trgBandName.contains("Sigma0")) {
                continue;
            }

            String expression = null;
            String sigmaNoughtVirtualBandName = null;
            String description = null;

            if (incidenceAngleForSigma0.contains(Constants.USE_INCIDENCE_ANGLE_FROM_ELLIPSOID)) {

                expression = trgBandName
                        + "==" + trgBand.getNoDataValue() + '?' + trgBand.getNoDataValue()
                        + ':' + trgBandName + " / sin(projectedLocalIncidenceAngle * PI/180.0)"
                        + " * sin(incidenceAngleFromEllipsoid * PI/180)";

                sigmaNoughtVirtualBandName = trgBandName + "_use_inci_angle_from_ellipsoid";

                description = "Sigma0 image created using incidence angle from ellipsoid";

            } else if (incidenceAngleForSigma0.contains(Constants.USE_LOCAL_INCIDENCE_ANGLE_FROM_DEM)) {

                expression = trgBandName
                        + "==" + trgBand.getNoDataValue() + '?' + trgBand.getNoDataValue()
                        + ':' + trgBandName + " / sin(projectedLocalIncidenceAngle * PI/180.0)"
                        + " * sin(localIncidenceAngle * PI/180)";

                sigmaNoughtVirtualBandName = trgBandName + "_use_local_inci_angle_from_dem";

                description = "Sigma0 image created using local incidence angle from DEM";
            }

            final VirtualBand band = new VirtualBand(sigmaNoughtVirtualBandName,
                    ProductData.TYPE_FLOAT32,
                    trgBand.getRasterWidth(),
                    trgBand.getRasterHeight(),
                    expression);
            band.setUnit(trgBand.getUnit());
            band.setDescription(description);
            band.setNoDataValueUsed(true);
            targetProduct.addBand(band);
        }
    }

    /**
     * Create Gamma0 image as a virtual band.
     */
    public static void createGammaNoughtVirtualBand(Product targetProduct, String incidenceAngleForGamma0) {

        final Band[] bands = targetProduct.getBands();
        for (Band trgBand : bands) {

            final String trgBandName = trgBand.getName();
            if (trgBand instanceof VirtualBand || !trgBandName.contains("Sigma0")) {
                continue;
            }

            final String incidenceAngle;
            if (incidenceAngleForGamma0.contains(Constants.USE_INCIDENCE_ANGLE_FROM_ELLIPSOID)) {
                incidenceAngle = "incidenceAngleFromEllipsoid";
            } else if (incidenceAngleForGamma0.contains(Constants.USE_LOCAL_INCIDENCE_ANGLE_FROM_DEM)) {
                incidenceAngle = "localIncidenceAngle";
            } else { // USE_PROJECTED_INCIDENCE_ANGLE_FROM_DEM
                incidenceAngle = "projectedLocalIncidenceAngle";
            }

            final String expression = trgBandName
                    + "==" + trgBand.getNoDataValue() + '?' + trgBand.getNoDataValue()
                    + ':' + trgBandName + " / sin(projectedLocalIncidenceAngle * PI/180.0)"
                    + " * sin(" + incidenceAngle + " * PI/180)" + " / cos(" + incidenceAngle + " * PI/180)";

            String gammaNoughtVirtualBandName;
            String description;
            if (incidenceAngleForGamma0.contains(Constants.USE_INCIDENCE_ANGLE_FROM_ELLIPSOID)) {
                gammaNoughtVirtualBandName = "_use_inci_angle_from_ellipsoid";
                description = "Gamma0 image created using incidence angle from ellipsoid";
            } else if (incidenceAngleForGamma0.contains(Constants.USE_LOCAL_INCIDENCE_ANGLE_FROM_DEM)) {
                gammaNoughtVirtualBandName = "_use_local_inci_angle_from_dem";
                description = "Gamma0 image created using local incidence angle from DEM";
            } else { // USE_PROJECTED_INCIDENCE_ANGLE_FROM_DEM
                gammaNoughtVirtualBandName = "_use_projected_local_inci_angle_from_dem";
                description = "Gamma0 image created using projected local incidence angle from dem";
            }

            if (trgBandName.contains("_HH")) {
                gammaNoughtVirtualBandName = "Gamma0_HH" + gammaNoughtVirtualBandName;
            } else if (trgBandName.contains("_VV")) {
                gammaNoughtVirtualBandName = "Gamma0_VV" + gammaNoughtVirtualBandName;
            } else if (trgBandName.contains("_HV")) {
                gammaNoughtVirtualBandName = "Gamma0_HV" + gammaNoughtVirtualBandName;
            } else if (trgBandName.contains("_VH")) {
                gammaNoughtVirtualBandName = "Gamma0_VH" + gammaNoughtVirtualBandName;
            } else {
                gammaNoughtVirtualBandName = "Gamma0" + gammaNoughtVirtualBandName;
            }

            final VirtualBand band = new VirtualBand(gammaNoughtVirtualBandName,
                    ProductData.TYPE_FLOAT32,
                    trgBand.getRasterWidth(),
                    trgBand.getRasterHeight(),
                    expression);
            band.setUnit(trgBand.getUnit());
            band.setDescription(description);
            band.setNoDataValueUsed(true);
            targetProduct.addBand(band);
        }
    }

    /**
     * Create Beta0 image as a virtual band.
     */
    public static void createBetaNoughtVirtualBand(final Product targetProduct) {

        final Band[] bands = targetProduct.getBands();
        for (Band trgBand : bands) {

            final String trgBandName = trgBand.getName();
            if (trgBand instanceof VirtualBand || !trgBandName.contains("Sigma0")) {
                continue;
            }

            final String expression = trgBandName
                    + "==" + trgBand.getNoDataValue() + '?' + trgBand.getNoDataValue()
                    + ':' + trgBandName + " / sin(projectedLocalIncidenceAngle * PI/180.0)";

            String betaNoughtVirtualBandName = trgBandName.replace("Sigma0", "Beta0");
            final VirtualBand band = new VirtualBand(betaNoughtVirtualBandName,
                    ProductData.TYPE_FLOAT32,
                    trgBand.getRasterWidth(),
                    trgBand.getRasterHeight(),
                    expression);
            band.setUnit(trgBand.getUnit());
            band.setDescription("Beta0 image");
            band.setNoDataValueUsed(true);
            targetProduct.addBand(band);
        }
    }
}
