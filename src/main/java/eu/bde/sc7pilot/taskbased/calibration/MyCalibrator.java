package eu.bde.sc7pilot.taskbased.calibration;

import java.io.File;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.Tile;
import org.esa.snap.engine_utilities.datamodel.Unit;
import eu.bde.sc7pilot.taskbased.AbstractOperator;

public interface MyCalibrator {

    void initialize(final AbstractOperator op, final Product sourceProduct, final Product targetProduct,
            final boolean mustPerformRetroCalibration, final boolean mustUpdateMetadata)
            throws OperatorException;

    void computeTile(final Band targetBand, final Tile targetTile) throws OperatorException;

    void setOutputImageInComplex(final boolean flag);

    void setOutputImageIndB(final boolean flag);

    void setIncidenceAngleForSigma0(final String incidenceAngleForSigma0);

    void setExternalAuxFile(final File file);

    void setAuxFileFlag(final String auxFile);

    double applyRetroCalibration(final int x, final int y, final double v, final String bandPolar,
            final Unit.UnitType bandUnit, final int[] subSwathIndex);

    double applyCalibration(
            final double v, final double rangeIndex, final double azimuthIndex, final double slantRange,
            final double satelliteHeight, final double sceneToEarthCentre, final double localIncidenceAngle,
            final String bandName, final String bandPolar, final Unit.UnitType bandUnit, int[] subSwathIndex);

    void removeFactorsForCurrentTile(final Band targetBand, final Tile targetTile, final String srcBandName);

    Product createTargetProduct(final Product sourceProduct, final String[] sourceBandNames);
}
