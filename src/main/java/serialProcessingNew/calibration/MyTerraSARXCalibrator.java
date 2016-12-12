/*
 * Copyright (C) 2014 by Array Systems Computing Inc. http://www.array.ca
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */
package serialProcessingNew.calibration;

import java.awt.Rectangle;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.util.FastMath;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.TiePointGrid;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.Tile;
import org.esa.snap.engine_utilities.datamodel.AbstractMetadata;
import org.esa.snap.engine_utilities.datamodel.Unit;
import org.esa.snap.engine_utilities.eo.Constants;
import org.esa.snap.engine_utilities.gpf.OperatorUtils;
import org.esa.snap.engine_utilities.gpf.ReaderUtils;
import org.esa.snap.engine_utilities.gpf.TileIndex;
import org.esa.snap.engine_utilities.util.Maths;
import serialProcessingNew.AbstractOperator;


/**
 * Calibration for TerraSAR-X data products.
 */

public class MyTerraSARXCalibrator extends MyBaseCalibrator implements MyCalibrator {

    private String productType = null;
    private String acquisitionMode = null;
    private boolean useIncidenceAngleFromGIM = false;
    private double firstLineUTC = 0.0; // in days
    private double lineTimeInterval = 0.0; // in days
    private int sourceImageWidth = 0;
    private TiePointGrid incidenceAngle = null;
    private TiePointGrid slantRangeTime = null;
    private final HashMap<String, Double> calibrationFactor = new HashMap<>(2);
    private final HashMap<String, NoiseRecord[]> noiseRecord = new HashMap<>(2);
    private final HashMap<String, int[]> rangeLineIndex = new HashMap<>(2); // y indices of noise records
    private final HashMap<String, double[][]> rangeLineNoise = new HashMap<>(2);
    private Product sourceGIMProduct = null;
    private boolean noiseCorrectedFlag = false;

    /**
     * Default constructor. The graph processing framework
     * requires that an operator has a default constructor.
     */
    public MyTerraSARXCalibrator() {
    }

    /**
     * Set external auxiliary file.
     */
    public void setExternalAuxFile(File file) throws OperatorException {
        if (file != null) {
            throw new OperatorException("TerraSARXCalibrator: No external auxiliary file should be selected for TerraSAT-X product");
        }
    }

    /**
     * Set auxiliary file flag.
     */
    public void setAuxFileFlag(String file) {
    }

    /**

     */
    public void initialize(final AbstractOperator op, final Product srcProduct, final Product tgtProduct,
                           final boolean mustPerformRetroCalibration, final boolean mustUpdateMetadata)
            throws OperatorException {
        try {
            calibrationOp = op;
            sourceProduct = srcProduct;
            targetProduct = tgtProduct;

            absRoot = AbstractMetadata.getAbstractedMetadata(sourceProduct);
            origMetadataRoot = AbstractMetadata.getOriginalProductMetadata(sourceProduct);

            sourceImageWidth = sourceProduct.getSceneRasterWidth();

            getMission();

            getAcquisitionMode();

            getProductType();

            getCalibrationFlag();

            getSampleType();

            getMetadata();

            getCalibrationFactor();

            getTiePointGridData();

            if (useIncidenceAngleFromGIM) {
                getGIMProduct();
            }

            getNoiseCorrectedFlag();

            if (!noiseCorrectedFlag) {
                getNoiseRecords();
                computeNoiseForRangeLines();
            }

            if (mustUpdateMetadata) {
                updateTargetProductMetadata();
            }

        } catch (Exception e) {
            throw new OperatorException("TerraSARXCalibrator: " + e);
        }
    }

    /**
     * Get mission.
     */
    private void getMission() {
        final String mission = absRoot.getAttributeString(AbstractMetadata.MISSION);
        if (!(mission.contains("TSX") || mission.contains("TDX")))
            throw new OperatorException("TerraSARXCalibrator: " + mission +
                    " is not a valid mission for TerraSAT-X Calibration");
    }

    /**
     * Get acquisition mode.
     */
    private void getAcquisitionMode() {
        acquisitionMode = absRoot.getAttributeString(AbstractMetadata.ACQUISITION_MODE);
        //if(acquisitionMode.contains("ScanSAR"))
        //    throw new OperatorException("ScanSAR calibration is currently not supported.");
    }

    /**
     * Get product type.
     */
    private void getProductType() {
        productType = absRoot.getAttributeString(AbstractMetadata.PRODUCT_TYPE);
        if (productType.contains("EEC")) {
            useIncidenceAngleFromGIM = true;
        }
    }

    /**
     * Get calibration factors for all polarizations.
     */
    private void getCalibrationFactor() {
        final MetadataElement level1Product = origMetadataRoot.getElement("level1Product");
        final MetadataElement calibrationElem = level1Product.getElement("calibration");
        final MetadataElement[] subElems = calibrationElem.getElements();
        for (MetadataElement ele : subElems) {
            if (ele.getName().contains("calibrationConstant")) {
                final String pol = ele.getAttributeString("polLayer").toUpperCase();
                final double factor = Double.parseDouble(ele.getAttributeString("calFactor"));
                calibrationFactor.put(pol, factor);
            }
        }
    }

    private void getMetadata() {
        firstLineUTC = absRoot.getAttributeUTC(AbstractMetadata.first_line_time).getMJD(); // in days
        lineTimeInterval = absRoot.getAttributeDouble(AbstractMetadata.line_time_interval) / Constants.secondsInDay; // s to day
        if (lineTimeInterval == 0.0) {
            throw new OperatorException("Invalid input for Line Time Interval: " + lineTimeInterval);
        }
    }

    private void getNoiseCorrectedFlag() {

        final MetadataElement level1Product = origMetadataRoot.getElement("level1Product");
        final MetadataElement processingFlagsElem = level1Product.getElement("processing").getElement("processingFlags");
        if (processingFlagsElem == null) {
            throw new OperatorException("Cannot find \"processingFlags\" element in level1Product metadata");
        }
        noiseCorrectedFlag = processingFlagsElem.getAttributeString("noiseCorrectedFlag").contains("true");

        if (acquisitionMode.contains("ScanSAR") && !noiseCorrectedFlag) {
            throw new OperatorException("Noise correction for ScanSAR is currently not supported.");
        }
    }

    /**
     * Get image noise records.
     */
    private void getNoiseRecords() {

        final MetadataElement level1Product = origMetadataRoot.getElement("level1Product");
        final MetadataElement[] subElems = level1Product.getElements();
        for (MetadataElement ele : subElems) {
            if (!ele.getName().contains("noise")) {
                continue;
            }

            final String pol = ele.getAttributeString("polLayer").toUpperCase();
            final int numOfNoiseRecords = Integer.parseInt(ele.getAttributeString("numberOfNoiseRecords"));
            final MetadataElement[] imageNoiseElem = ele.getElements();
            if (numOfNoiseRecords != imageNoiseElem.length) {
                throw new OperatorException(
                        "TerraSARXCalibrator: The number of noise records does not match the record number.");
            }

            NoiseRecord[] record = new NoiseRecord[numOfNoiseRecords];
            for (int i = 0; i < numOfNoiseRecords; ++i) {
                record[i] = new NoiseRecord();
                record[i].timeUTC = ReaderUtils.getTime(imageNoiseElem[i], "timeUTC", AbstractMetadata.dateFormat).getMJD();
                record[i].noiseEstimateConfidence = Double.parseDouble(imageNoiseElem[i].getAttributeString("noiseEstimateConfidence"));

                final MetadataElement noiseEstimate = imageNoiseElem[i].getElement("noiseEstimate");
                record[i].validityRangeMin = Double.parseDouble(noiseEstimate.getAttributeString("validityRangeMin"));
                record[i].validityRangeMax = Double.parseDouble(noiseEstimate.getAttributeString("validityRangeMax"));
                record[i].referencePoint = Double.parseDouble(noiseEstimate.getAttributeString("referencePoint"));
                record[i].polynomialDegree = Integer.parseInt(noiseEstimate.getAttributeString("polynomialDegree"));

                final MetadataElement[] coefficientElem = noiseEstimate.getElements();
                if (record[i].polynomialDegree + 1 != coefficientElem.length) {
                    throw new OperatorException(
                            "TerraSARXCalibrator: The number of coefficients does not match the polynomial degree.");
                }

                record[i].coefficient = new double[record[i].polynomialDegree + 1];
                for (int j = 0; j < coefficientElem.length; ++j) {
                    record[i].coefficient[j] = Double.parseDouble(coefficientElem[j].getAttributeString("coefficient"));
                }
            }

            noiseRecord.put(pol, record);
        }
    }

    /**
     * Compute noise for the whole range lines corresponding to the noise records for all polarizations.
     */
    private void computeNoiseForRangeLines() {

        Set<Map.Entry<String, NoiseRecord[]>> set = noiseRecord.entrySet();
        for (Map.Entry<String, NoiseRecord[]> elem : set) {
            final String pol = elem.getKey();
            final NoiseRecord[] record = elem.getValue();
            final int numOfNoiseRecords = record.length;
            double[][] noise = new double[numOfNoiseRecords][sourceImageWidth];
            int[] index = new int[numOfNoiseRecords];

            for (int i = 0; i < numOfNoiseRecords; ++i) {
                index[i] = (int) ((record[i].timeUTC - firstLineUTC) / lineTimeInterval + 0.5);
                for (int j = 0; j < sourceImageWidth; ++j) {
                    final double slantRgTime = slantRangeTime.getPixelDouble(j, index[i]) / 1.0e9; // ns to s
                    if (slantRgTime >= record[i].validityRangeMin && slantRgTime <= record[i].validityRangeMax) {
                        noise[i][j] = Maths.computePolynomialValue(
                                slantRgTime - record[i].referencePoint, record[i].coefficient);
                    } else {
                        noise[i][j] = 0.0;
                    }
                }
            }
            rangeLineIndex.put(pol, index);
            rangeLineNoise.put(pol, noise);
        }
    }

    /**
     * Get incidence angle and slant range time tie point grids.
     */
    private void getTiePointGridData() {
        incidenceAngle = OperatorUtils.getIncidenceAngle(sourceProduct);
        slantRangeTime = OperatorUtils.getSlantRangeTime(sourceProduct);
    }

    /**
     * Get GIM product.
     */
    private void getGIMProduct() {
        try {
            if(sourceProduct.getFileLocation() == null) {
                useIncidenceAngleFromGIM = false;
                return;
            }

            File sourceGIMFile =
                    new File(sourceProduct.getFileLocation().getParentFile(), "AUXRASTER" + File.separator + "GIM.tif");

            if (sourceGIMFile.exists()) {
                sourceGIMProduct = ProductIO.readProduct(sourceGIMFile);
            } else {
                useIncidenceAngleFromGIM = false;
            }

        } catch (Exception e) {
            throw new OperatorException("TerraSARXCalibrator: " + e);
        }
    }

    /**
     * Update the metadata in the target product.
     */
    private void updateTargetProductMetadata() {

        final MetadataElement abs = AbstractMetadata.getAbstractedMetadata(targetProduct);

        abs.getAttribute(AbstractMetadata.abs_calibration_flag).getData().setElemBoolean(true);
    }

    /**
     * Called by the framework in order to compute a tile for the given target band.
     * <p>The default implementation throws a runtime exception with the message "not implemented".</p>
     *
     * @param targetBand The target band.
     * @param targetTile The current tile associated with the target band to be computed.
     * @param pm         A progress monitor which should be used to determine computation cancelation requests.
     * @throws OperatorException If an error occurs during computation of the target raster.
     */
    public void computeTile(Band targetBand, Tile targetTile) throws OperatorException {

        final Rectangle targetTileRectangle = targetTile.getRectangle();
        final int x0 = targetTileRectangle.x;
        final int y0 = targetTileRectangle.y;
        final int w = targetTileRectangle.width;
        final int h = targetTileRectangle.height;
        //System.out.println("x0 = " + x0 + ", y0 = " + y0 + ", w = " + w + ", h = " + h);

        Tile sourceRaster1 = null;
        ProductData srcData1 = null;
        ProductData srcData2 = null;
        Band sourceBand1 = null;

        final String[] srcBandNames = targetBandNameToSourceBandName.get(targetBand.getName());
        if (srcBandNames.length == 1) {
            sourceBand1 = sourceProduct.getBand(srcBandNames[0]);
            sourceRaster1 = calibrationOp.getSourceTile(sourceBand1, targetTileRectangle);
            srcData1 = sourceRaster1.getDataBuffer();
        } else {
            sourceBand1 = sourceProduct.getBand(srcBandNames[0]);
            final Band sourceBand2 = sourceProduct.getBand(srcBandNames[1]);
            sourceRaster1 = calibrationOp.getSourceTile(sourceBand1, targetTileRectangle);
            final Tile sourceRaster2 = calibrationOp.getSourceTile(sourceBand2, targetTileRectangle);
            srcData1 = sourceRaster1.getDataBuffer();
            srcData2 = sourceRaster2.getDataBuffer();
        }

        Tile srcGIMTile = null;
        ProductData srcGIMData = null;
        if (useIncidenceAngleFromGIM) {
            srcGIMTile = calibrationOp.getSourceTile(sourceGIMProduct.getBand("band_1"), targetTileRectangle);
            srcGIMData = srcGIMTile.getDataBuffer();
        }

        final Unit.UnitType tgtBandUnit = Unit.getUnitType(targetBand);
        final Unit.UnitType srcBandUnit = Unit.getUnitType(sourceBand1);
        final double noDataValue = sourceBand1.getNoDataValue();

        // copy band if unit is phase
        if (tgtBandUnit == Unit.UnitType.PHASE) {
            targetTile.setRawSamples(sourceRaster1.getRawSamples());
            return;
        }

        final String pol = OperatorUtils.getBandPolarization(srcBandNames[0], absRoot).toUpperCase();
        double Ks = 0.0;
        if (pol != null) {
            Ks = calibrationFactor.get(pol);
        }

        double[][] tileNoise = null;
        if (!noiseCorrectedFlag) {
            tileNoise = new double[h][w];
            computeTileNoise(pol, x0, y0, w, h, tileNoise);
        }

        final ProductData trgData = targetTile.getDataBuffer();
        final TileIndex srcIndex = new TileIndex(sourceRaster1);
        final TileIndex tgtIndex = new TileIndex(targetTile);

        final int maxY = y0 + h;
        final int maxX = x0 + w;

        double sigma, dn, dn2, i, q, phaseTerm = 0.0;
        int srcIdx, tgtIdx;

        for (int y = y0; y < maxY; ++y) {
            srcIndex.calculateStride(y);
            tgtIndex.calculateStride(y);

            for (int x = x0; x < maxX; ++x) {
                srcIdx = srcIndex.getIndex(x);
                tgtIdx = tgtIndex.getIndex(x);

                if (srcBandUnit == Unit.UnitType.AMPLITUDE) {
                    dn = srcData1.getElemDoubleAt(srcIdx);
                    dn2 = dn * dn;
                } else if (srcBandUnit == Unit.UnitType.INTENSITY) {
                    dn2 = srcData1.getElemDoubleAt(srcIdx);
                } else if (srcBandUnit == Unit.UnitType.REAL) {
                    i = srcData1.getElemDoubleAt(srcIdx);
                    q = srcData2.getElemDoubleAt(srcIdx);
                    dn2 = i * i + q * q;
                    if (tgtBandUnit == Unit.UnitType.REAL) {
                        phaseTerm = i / Math.sqrt(dn2);
                    } else if (tgtBandUnit == Unit.UnitType.IMAGINARY) {
                        phaseTerm = q / Math.sqrt(dn2);
                    }
                } else if (srcBandUnit == Unit.UnitType.INTENSITY_DB) {
                    dn2 = FastMath.pow(10, srcData1.getElemDoubleAt(srcIdx) / 10.0); // convert dB to linear scale
                } else {
                    throw new OperatorException("TerraSAR-X Calibration: unhandled unit");
                }

                double inciAng;
                if (useIncidenceAngleFromGIM) {
                    final int gim = srcGIMData.getElemIntAt(srcIdx);
                    inciAng = (gim - (gim % 10)) / 100.0 * Constants.DTOR;
                } else {
                    inciAng = incidenceAngle.getPixelDouble(x, y) * Constants.DTOR;
                }

                if (noiseCorrectedFlag) {
                    sigma = Ks * dn2 * FastMath.sin(inciAng);
                } else {
                    sigma = Ks * (dn2 - tileNoise[y - y0][x - x0]) * FastMath.sin(inciAng);
                }

                if (isComplex && outputImageInComplex) {
                    sigma = Math.sqrt(sigma)*phaseTerm;
                }

                if (outputImageScaleInDb) { // convert calibration result to dB
                    if (sigma < underFlowFloat) {
                        sigma = -underFlowFloat;
                    } else {
                        sigma = 10.0 * Math.log10(sigma);
                    }
                }

                trgData.setElemDoubleAt(tgtIdx, sigma);
            }
        }
    }

    /**
     * Compute noise for each pixel in the tile.
     *
     * @param pol       Polarization string.
     * @param x0        X coordinate for pixel at the upper left corner of the tile.
     * @param y0        Y coordinate for pixel at the upper left corner of the tile.
     * @param w         Tile width.
     * @param h         Tile height.
     * @param tileNoise Array holding noise for the tile.
     */
    private void computeTileNoise(final String pol, final int x0, final int y0, final int w,
                                  final int h, double[][] tileNoise) {

        final int[] indexArray = rangeLineIndex.get(pol);
        final double[][] noise = rangeLineNoise.get(pol);

        int i1 = 0, i2 = 0;
        int y1 = 0, y2 = 0;
        for (int y = y0; y < y0 + h; ++y) {

            for (int i = 0; i < indexArray.length; ++i) {
                if (indexArray[i] <= y) {
                    i1 = i;
                    y1 = indexArray[i];
                } else {
                    i2 = i;
                    y2 = indexArray[i];
                    break;
                }
            }

            if (y1 == indexArray[indexArray.length - 1]) {
                y2 = y1;
                i2 = i1;
            } else if (y1 > y2) {
                throw new OperatorException("TerraSARXCalibrator: No noise is defined for pixel with y = " + y);
            }

            for (int x = x0; x < x0 + w; ++x) {
                final double n1 = noise[i1][x];
                final double n2 = noise[i2][x];
                double mu = 0.0;
                if (y1 != y2) {
                    mu = (double) (y - y1) / (double) (y2 - y1);
                }
                tileNoise[y - y0][x - x0] = Maths.interpolationLinear(n1, n2, mu);
            }
        }
    }

    public double applyCalibration(
            final double v, final double rangeIndex, final double azimuthIndex, final double slantRange,
            final double satelliteHeight, final double sceneToEarthCentre, final double localIncidenceAngle,
            final String bandName, final String bandPolar, final Unit.UnitType bandUnit, int[] subSwathIndex) {

        double sigma = 0.0;
        if (bandUnit == Unit.UnitType.AMPLITUDE) {
            sigma = v * v;
        } else if (bandUnit == Unit.UnitType.INTENSITY || bandUnit == Unit.UnitType.REAL || bandUnit == Unit.UnitType.IMAGINARY) {
            sigma = v;
        } else if (bandUnit == Unit.UnitType.INTENSITY_DB) {
            sigma = FastMath.pow(10, v / 10.0); // convert dB to linear scale
        } else {
            throw new OperatorException("TerraSARXCalibrator: Unknown band unit");
        }

        final double Ks = calibrationFactor.get(bandPolar.toUpperCase());
        sigma *= Ks * FastMath.sin(localIncidenceAngle * Constants.DTOR);
        return sigma;
    }

    public double applyRetroCalibration(
            int x, int y, double v, String bandPolar, final Unit.UnitType bandUnit, int[] subSwathIndex) {
        return v;
    }

    public void removeFactorsForCurrentTile(Band targetBand, Tile targetTile, String srcBandName)
            throws OperatorException {

        Band sourceBand = sourceProduct.getBand(targetBand.getName());
        Tile sourceTile = calibrationOp.getSourceTile(sourceBand, targetTile.getRectangle());
        targetTile.setRawSamples(sourceTile.getRawSamples());
    }

    private final static class NoiseRecord {
        public double timeUTC;
        public double noiseEstimateConfidence;
        public double validityRangeMin;
        public double validityRangeMax;
        public double referencePoint;
        public int polynomialDegree;
        public double[] coefficient;
    }
}
