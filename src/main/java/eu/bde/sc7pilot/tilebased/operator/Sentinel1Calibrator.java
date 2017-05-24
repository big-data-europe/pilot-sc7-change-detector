package eu.bde.sc7pilot.tilebased.operator;

import java.awt.Rectangle;
import java.util.HashMap;

import org.apache.commons.math3.util.FastMath;
import org.esa.s1tbx.insar.gpf.Sentinel1Utils;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.engine_utilities.datamodel.Unit;

import eu.bde.sc7pilot.taskbased.calibration.MyBaseCalibrator;
import eu.bde.sc7pilot.taskbased.calibration.MySentinel1Calibrator.CalibrationInfo;
import eu.bde.sc7pilot.tilebased.metadata.CalibrationMetadata;
import eu.bde.sc7pilot.tilebased.metadata.CalibrationMetadata.CALTYPE;
import eu.bde.sc7pilot.tilebased.model.MyTile;
import eu.bde.sc7pilot.tilebased.model.MyTileIndex;


public class Sentinel1Calibrator extends MyBaseCalibrator {
	private CalibrationMetadata calibrationMetadata;
	private HashMap<String, CalibrationInfo> targetBandToCalInfo = new HashMap<>(2);
    private Unit.UnitType tgtBandUnit;
    private Unit.UnitType srcBandUnit;
    private String name;
    
    public Sentinel1Calibrator(CalibrationMetadata calibrationMetadata) {
    	this.calibrationMetadata = calibrationMetadata;
    	this.targetBandToCalInfo = calibrationMetadata.getTargetBandToCalInfo();
    	this.outputImageInComplex = calibrationMetadata.getOutputImageInComplex();
    	this.outputImageScaleInDb = calibrationMetadata.getOutputImageScaleInDb();
    	this.srcBandUnit = calibrationMetadata.getSrcBandUnit();
    	this.tgtBandUnit = calibrationMetadata.getTgtBandUnit();
    }
    
    public void computeTile(MyTile sourceRaster1, MyTile sourceRaster2, MyTile targetTile, double noDataValue, String targetBandName) throws OperatorException {
    	final Rectangle targetTileRectangle = targetTile.getRectangle();
    	final int x0 = targetTileRectangle.x;
    	final int y0 = targetTileRectangle.y;
    	final int w = targetTileRectangle.width;
    	final int h = targetTileRectangle.height;
    	//System.out.println("x0 = " + x0 + ", y0 = " + y0 + ", w = " + w + ", h = " + h + ", target band = " + targetBand.getName());
    	ProductData srcData1 = null;
    	ProductData srcData2 = null;
    	srcData1 = sourceRaster1.getDataBuffer();
    	if(sourceRaster2 != null)
    		srcData2 = sourceRaster2.getDataBuffer();
    	
    	final ProductData tgtData = targetTile.getDataBuffer();
    	
    	//CHECK tgtData
    	//tgtData.
    	
    	final MyTileIndex srcIndex = new MyTileIndex(sourceRaster1);
    	final MyTileIndex trgIndex = new MyTileIndex(targetTile);
    	final int maxY = y0 + h;
    	final int maxX = x0 + w;
    	final CalibrationInfo calInfo = targetBandToCalInfo.get(targetBandName);
    	final CALTYPE calType = getCalibrationType(targetBandName);
    	double dn = 0.0, dn2, i, q, muX, lutVal, retroLutVal = 1.0, calValue, calibrationFactor, phaseTerm = 0.0;
    	int srcIdx, trgIdx;
    	int zerows = 0, prints = 0, total = 0, ifTimes = 0, xTimes = 0, yTimes = 0, continues = 0;		// Debugging variables!
    	double samplingPercentage = 0.00008;
    	System.out.println("x0 =\t" + x0 + "\t\ty0 =\t" + y0);
    	System.out.println("w =\t" + w + "\t\th =\t" + h);
    	System.out.println("maxX =\t" + maxX + "\t\tmaxY =\t" + maxY + "\n");
    	for (int y = y0; y < maxY; ++y) {
    		yTimes++;
    		srcIndex.calculateStride(y);
    		trgIndex.calculateStride(y);
    		final int calVecIdx = calInfo.getCalibrationVectorIndex(y);
    		final Sentinel1Utils.CalibrationVector vec0 = calInfo.getCalibrationVector(calVecIdx);
    		final Sentinel1Utils.CalibrationVector vec1 = calInfo.getCalibrationVector(calVecIdx + 1);
    		final float[] vec0LUT = getVector(calType, vec0);
    		final float[] vec1LUT = getVector(calType, vec1);
    		
    		float[] retroVec0LUT = null;
    		float[] retroVec1LUT = null;
    		CalibrationMetadata.CALTYPE dataType=calibrationMetadata.getDataType();
    		if (dataType != null) {
    			retroVec0LUT = getVector(dataType, vec0);
    			retroVec1LUT = getVector(dataType, vec1);
    		}
    		final double azTime = calInfo.firstLineTime + y * calInfo.lineTimeInterval;
    		final double muY = (azTime - vec0.timeMJD) / (vec1.timeMJD - vec0.timeMJD);
    		for (int x = x0; x < maxX; ++x) {
    			xTimes++;
    			// variable for sampling
    			double m = Math.random();
    			
    			srcIdx = srcIndex.getIndex(x);
    			trgIdx = trgIndex.getIndex(x);
    			if (srcData1.getElemDoubleAt(srcIdx) == noDataValue) {
    				continues++;
        			if(m < samplingPercentage) {
        				System.out.println("\nsrcData1.getElemDoubleAt(srcIdx) =\t" + srcData1.getElemDoubleAt(srcIdx) + "\tat srcIdx: " + srcIdx);
        				System.out.println("\t\t\tnoDataValue =\t" + noDataValue + "\n");
        			}
    				continue;
    				// Every time the pixels are Zerow, is because we enter in this black-hole here!!!
    			}
    			
    			final int pixelIdx = calInfo.getPixelIndex(x, calVecIdx);
    			muX = (x - vec0.pixels[pixelIdx]) / (double)(vec0.pixels[pixelIdx + 1] - vec0.pixels[pixelIdx]);
    			lutVal = (1 - muY) * ((1 - muX) * vec0LUT[pixelIdx] + muX * vec0LUT[pixelIdx + 1]) + muY * ((1 - muX) * vec1LUT[pixelIdx] + muX * vec1LUT[pixelIdx + 1]);
    			calibrationFactor = 1.0 / (lutVal*lutVal);
    			if (srcBandUnit == Unit.UnitType.AMPLITUDE) {
    				dn = srcData1.getElemDoubleAt(srcIdx);
    				dn2 = dn * dn;
    			} 
    			else if (srcBandUnit == Unit.UnitType.INTENSITY) {
    				if (dataType != null) {
    					retroLutVal = (1 - muY) * ((1 - muX) * retroVec0LUT[pixelIdx] + muX * retroVec0LUT[pixelIdx + 1]) +
            																		muY * ((1 - muX) * retroVec1LUT[pixelIdx] + muX * retroVec1LUT[pixelIdx + 1]);
    				}
    				dn2 = srcData1.getElemDoubleAt(srcIdx);
    				calibrationFactor *= retroLutVal;
    			}
    			else if (srcBandUnit == Unit.UnitType.REAL) {
    				i = srcData1.getElemDoubleAt(srcIdx);
    				q = srcData2.getElemDoubleAt(srcIdx);
    				dn2 = i * i + q * q;
    				if (tgtBandUnit == Unit.UnitType.REAL) {
    					phaseTerm = i / Math.sqrt(dn2);
    				} 
    				else if (tgtBandUnit == Unit.UnitType.IMAGINARY) {
    					phaseTerm = q / Math.sqrt(dn2);
    				}
    			} 
    			else if (srcBandUnit == Unit.UnitType.INTENSITY_DB) {
    				dn2 = FastMath.pow(10, srcData1.getElemDoubleAt(srcIdx) / 10.0); // convert dB to linear scale
    			} 
    			else {
    				throw new OperatorException("Sentinel-1 Calibration: unhandled unit");
    			}

    			calValue = dn2 * calibrationFactor;
    			if(m < samplingPercentage) {
    				System.out.println("\tcalValue = dn2 * calibrationFactor");
    				System.out.println("dn2 =\t\t\t" + dn2);
    				System.out.println("calibrationFactor =\t" + calibrationFactor);
    				System.out.println("calValue =\t\t" + calValue + "\n");
    			}

    			if (isComplex && outputImageInComplex) {
    				ifTimes++;
    				calValue = Math.sqrt(calValue)*phaseTerm;
        			if(m < samplingPercentage) {
        				System.out.println("\tcalValue = Math.sqrt(calValue)*phaseTerm");
        				System.out.println("phaseTerm =\t" + phaseTerm);
        				System.out.println("calValue =\t" + calValue + "\n");
        			}
    			}
            
    			tgtData.setElemDoubleAt(trgIdx, calValue);
    			
    			// Check how many calValue(s) are Zerow
    			// Print samples of trgIdx and calValue
    			total++;
    			if(calValue == 0.0)
    				zerows++;
    			if(m < samplingPercentage) {
    				prints++;
    				System.out.println("trgIdx: " + trgIdx + "\tHas calValue: " + calValue);
    				System.out.println("\t~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
    			}
    		}
    	}
    	System.out.println("\n" + xTimes + "\ttimes entered in X.");
    	System.out.println(yTimes + "\ttimes entered in Y.");
    	System.out.println(continues + "\ttimes Continued.\n\n");
    	System.out.println("RESULTS:");
    	System.out.println("\n" + prints + "\tsamples out of:\t" + total + "\tare printed");
    	System.out.println(ifTimes + "\ttimes entere in IF (isComplex && outputImageInComplex)");
    	System.out.println(zerows + "\tcalValue are ZEROW\n");
    }
    
    public static float[] getVector(final CalibrationMetadata.CALTYPE calType, final Sentinel1Utils.CalibrationVector vec) {
    	if (calType == null) {
    		return null;
    	}
    	else if (calType.equals(CALTYPE.SIGMA0)) {
    		return vec.sigmaNought;
    	}
    	else if (calType.equals(CALTYPE.BETA0)) {
    		return vec.betaNought;
    	}
    	else if (calType.equals(CALTYPE.GAMMA)) {
    		return vec.gamma;
    	} else {
    		return vec.dn;
    	}
    }
    
    public static CALTYPE getCalibrationType(final String bandName) {
    	CALTYPE calType;
    	if (bandName.contains("Beta")) {
    		calType = CALTYPE.BETA0;
    	}
    	else if (bandName.contains("Gamma")) {
    		calType = CALTYPE.GAMMA;
    	}
    	else if (bandName.contains("DN")) {
    		calType = CALTYPE.DN;
    	}
    	else {
    		calType = CALTYPE.SIGMA0;
    	}
    	return calType;
    }
    
    public String getName() {
    	return name;
    }
    
    public void setName(String name) {
    	this.name = name;
    }
    
}
