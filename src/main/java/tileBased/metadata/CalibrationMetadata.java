package tileBased.metadata;

import java.util.HashMap;

import org.esa.snap.engine_utilities.datamodel.Unit;

import serialProcessingNew.calibration.MySentinel1Calibrator.CalibrationInfo;


public class CalibrationMetadata {
    private Boolean outputImageInComplex = false;
    private Boolean outputImageScaleInDb = false;
    private CALTYPE dataType = null;
    private Unit.UnitType tgtBandUnit;
    private Unit.UnitType srcBandUnit;
    private HashMap<String, CalibrationInfo> targetBandToCalInfo;
    public enum CALTYPE {SIGMA0, BETA0, GAMMA, DN}
    
	public CalibrationMetadata(Boolean[] bParams,Unit.UnitType tgtBandUnit,Unit.UnitType srcBandUnit,HashMap<String, CalibrationInfo> targetBandToCalInfo) {
		this.srcBandUnit=srcBandUnit;
		this.tgtBandUnit=tgtBandUnit;
		this.outputImageInComplex=bParams[0];
    	this.outputImageScaleInDb=bParams[1];
    	this.targetBandToCalInfo=targetBandToCalInfo;
	}
	public Boolean getOutputImageInComplex() {
		return outputImageInComplex;
	}
	public void setOutputImageInComplex(Boolean outputImageInComplex) {
		this.outputImageInComplex = outputImageInComplex;
	}
	public Boolean getOutputImageScaleInDb() {
		return outputImageScaleInDb;
	}
	public void setOutputImageScaleInDb(Boolean outputImageScaleInDb) {
		this.outputImageScaleInDb = outputImageScaleInDb;
	}
	public CALTYPE getDataType() {
		return dataType;
	}
	public void setDataType(CALTYPE dataType) {
		this.dataType = dataType;
	}
	public Unit.UnitType getTgtBandUnit() {
		return tgtBandUnit;
	}
	public void setTgtBandUnit(Unit.UnitType tgtBandUnit) {
		this.tgtBandUnit = tgtBandUnit;
	}
	public Unit.UnitType getSrcBandUnit() {
		return srcBandUnit;
	}
	public void setSrcBandUnit(Unit.UnitType srcBandUnit) {
		this.srcBandUnit = srcBandUnit;
	}
	public HashMap<String, CalibrationInfo> getTargetBandToCalInfo() {
		return targetBandToCalInfo;
	}
	public void setTargetBandToCalInfo(HashMap<String, CalibrationInfo> targetBandToCalInfo) {
		this.targetBandToCalInfo = targetBandToCalInfo;
	}
}
