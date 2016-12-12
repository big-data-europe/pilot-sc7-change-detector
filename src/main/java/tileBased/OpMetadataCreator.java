package tileBased;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.TiePointGeoCoding;
import org.esa.snap.engine_utilities.datamodel.Unit;
import org.esa.snap.engine_utilities.gpf.StackUtils;

import serialProcessingNew.AbstractOperator;
import serialProcessingNew.MyChangeDetection;
import serialProcessingNew.MyCreateStack;
import serialProcessingNew.MyGCPSelection;
import serialProcessingNew.MyRead;
import serialProcessingNew.MyWarp;
import serialProcessingNew.calibration.MyCalibration;
import serialProcessingNew.calibration.MySentinel1Calibrator;
import tileBased.metadata.CalibrationMetadata;
import tileBased.metadata.ChangeDetectionMetadata;
import tileBased.metadata.ImageMetadata;
import tileBased.metadata.WarpMetadata;

public class OpMetadataCreator {

	public void createGCPMetadata() {
		
	}
	public WarpMetadata createWarpMetadata(MyWarp warpOp) {
		return new WarpMetadata(warpOp.getInterp(), warpOp.getInterpTable());
	}
	public void createOpImgMetadata(Map<String, ImageMetadata> imageMetadata,AbstractOperator operator){
		createOpImgMetadata(imageMetadata,operator,false);
	}
	public void createProdImgMetadata(Map<String, ImageMetadata> imageMetadata,Product targetProduct,String id){
		for (int i = 0; i < targetProduct.getNumBands(); i++) {
			Band targetBand = targetProduct.getBandAt(i);
			if(targetBand.getClass()!=Band.class)
				continue;
			ImageMetadata trgImageMetadata = getImageMetadata(targetBand,null);
			imageMetadata.put(targetBand.getName()+"_"+id, trgImageMetadata);
	}
	}
	public void createOpImgMetadata(Map<String, ImageMetadata> imageMetadata,AbstractOperator operator,boolean slaveOffsetIsNeeded){
		Product targetProduct = operator.getTargetProduct();
		String[] masterBandNames = StackUtils.getMasterBandNames(targetProduct);
		Map<Band, Band> sourceRasterMap = operator.getSourceRasterMap();
		Set<String> masterBands = new HashSet(Arrays.asList(masterBandNames));
		Map<Product, int[]> slaveOffsetMap =null;
		if(slaveOffsetIsNeeded)
			slaveOffsetMap = ((MyCreateStack) operator).getSlaveOffsettMap();
		for (int i = 0; i < targetProduct.getNumBands(); i++) {
			Band targetBand = targetProduct.getBandAt(i);
			if (masterBands.contains(targetBand.getName())) {
				continue;
			}
			ImageMetadata trgImageMetadata = getImageMetadata(targetBand,null);
			Band sourceBand = sourceRasterMap.get(targetBand);
			ImageMetadata srcImageMetadata =null;
			if(sourceBand!=null){
				 srcImageMetadata = getImageMetadata(sourceBand,targetBand.getName());
				 trgImageMetadata.setSourceBandName(sourceBand.getName());
			}
			String srcName = null;
			String trgName;
			if(operator.getClass()==MyGCPSelection.class||operator.getClass()==MyWarp.class){
				srcName=sourceBand.getName() +"_"+operator.getId()+ "_source";
				trgName=targetBand.getName() +"_"+operator.getId()+ "_target";
			}
			else{
				if(sourceBand!=null)
					srcName=sourceBand.getName()+"_"+operator.getId();
				trgName=targetBand.getName()+"_"+operator.getId();
			}
			if(sourceBand!=null)
				imageMetadata.put(srcName, srcImageMetadata);
			imageMetadata.put(trgName, trgImageMetadata);
//			if(sourceTargetMap!=null)
//				sourceTargetMap.put(sourceBand.getName()+"_"+operator.getId(), targetBand.getName());
			if(slaveOffsetIsNeeded)
				srcImageMetadata.setOffsetMap(slaveOffsetMap.get(sourceBand.getProduct()));
				
		}
	}
	public void createCalImgMetadata(Map<String, ImageMetadata> imageMetadata,MyCalibration myCalibration,Map<String, CalibrationMetadata> calMetadata,Boolean[] bParams1) {
		Product targetProduct = myCalibration.getTargetProduct();
		Product sourceProduct = myCalibration.getSourceProduct();
		for (int i = 0; i < targetProduct.getNumBands(); i++) {
			HashMap<String, String[]> targetBandNameToSourceBandName = ((MySentinel1Calibrator) myCalibration
					.getCalibrator()).getTargetBandNameToSourceBandName();
			Band targetBandCal = targetProduct.getBandAt(i);

			Band sourceBandCal = sourceProduct
					.getBand(targetBandNameToSourceBandName.get(targetBandCal.getName())[0]);
			ImageMetadata trgImageMetadataCal = getImageMetadata(targetBandCal,sourceBandCal.getName());
			ImageMetadata srcImageMetadataCal = getImageMetadata(sourceBandCal,targetBandCal.getName());

			CalibrationMetadata calibrationMetadata = new CalibrationMetadata(bParams1,
					Unit.getUnitType(targetBandCal), Unit.getUnitType(sourceBandCal),
					((MySentinel1Calibrator) myCalibration.getCalibrator()).getTargetBandToCalInfo());
			
			imageMetadata.put(sourceBandCal.getName()+"_"+myCalibration.getId(), srcImageMetadataCal);
			imageMetadata.put(targetBandCal.getName()+"_"+myCalibration.getId(), trgImageMetadataCal);
			calMetadata.put(sourceBandCal.getName()+"_"+myCalibration.getId(), calibrationMetadata);
		}
	}
	public ChangeDetectionMetadata createChangeDMetadata(MyChangeDetection mychangeDetection,boolean[] bParams3) {
		 double[] dParams2 = {mychangeDetection.getSourceProduct().getBandAt(0).getGeophysicalNoDataValue(),mychangeDetection.getSourceProduct().getBandAt(1).getGeophysicalNoDataValue() };
		 return new ChangeDetectionMetadata(bParams3, dParams2);

	}
	private ImageMetadata getImageMetadata(Band band,String sourceBandName) {
		//band.getSourceImage().ge
		return new ImageMetadata(band.getDataType(), band.getProduct().getSceneRasterWidth(),
				band.getProduct().getSceneRasterHeight(), (float) band.getGeophysicalNoDataValue(), 0, 0,
				((TiePointGeoCoding)band.getGeoCoding()).getLatGrid(),((TiePointGeoCoding)band.getGeoCoding()).getLonGrid(),band.getName(),sourceBandName);
	}
}
