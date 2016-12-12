package serialProcessingNew;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.gpf.common.BandMathsOp.BandDescriptor;

public class SnapS1Toolbox {
	private static final Logger log = Logger.getLogger(SnapS1Toolbox.class);
	
	private static Product runSnapOperator(String opName, Map<String,Object> parameters, Product prod) {
		GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
		Product retProd = GPF.createProduct(opName, parameters, prod);
		
		return retProd;
	}
	
	private static Product runSnapOperator(String opName, Map<String,Object> parameters, Product[] prods) {
		GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
		Product retProd = GPF.createProduct(opName, parameters, prods);
		
		return retProd;
	}
	
	public static Product readOperator(String imagePath) {
		Product prod = null;
		
		try {
			prod = ProductIO.readProduct(imagePath);
		} catch (IOException e) {
			log.error(e);
		}
		
		return prod;
	}
	
	public static void writeOperator(Product prod, String imagePath, String imgFormat) {
		try {
			ProductIO.writeProduct(prod, imagePath, imgFormat);
		} catch (IOException e) {
			log.error(e);
		}
	}
	
	public static Product subsetOperator(Product prod, String polygon) {
        Map<String,Object> parameters = new HashMap<String,Object>();
        parameters.put("geoRegion", polygon);
        parameters.put("copyMetadata", true);
		
		return runSnapOperator("Subset", parameters, prod);
	}
	
	public static Product applyOrbitFileOperator(Product prod) {
        Map<String,Object> parameters = new HashMap<String,Object>();
        parameters.put("continueOnFail", true);
        
        return runSnapOperator("Apply-Orbit-File", parameters, prod);
	}
	
	public static Product thermalNoiseRemovalOperator(Product prod) {
        Map<String,Object> parameters = new HashMap<String,Object>();
        
        return runSnapOperator("ThermalNoiseRemoval", parameters, prod);
	}
	
	public static Product calibrationOperator(Product prod, String polarization) {
        Map<String,Object> parameters = new HashMap<String,Object>();
        parameters.put("auxFile", "Product Auxiliary File");
        parameters.put("selectedPolarisations", polarization);
        
        return runSnapOperator("Calibration", parameters, prod);
	}
	
	public static Product createStackOperator(Product[] prods) {
        Map<String,Object> parameters = new HashMap<String,Object>();
        parameters.put("resamplingType", "BICUBIC_INTERPOLATION");
        parameters.put("extent", "Minimum");
        
        return runSnapOperator("CreateStack", parameters, prods);
	}
	
	public static Product crossCorrelationOperator(Product prod, int numGCP) {
        Map<String,Object> parameters = new HashMap<String,Object>();
        parameters.put("numGCPtoGenerate", numGCP);
        parameters.put("coarseRegistrationWindowWidth", "64");
        parameters.put("coarseRegistrationWindowHeight", "64");
        parameters.put("maxIteration", 2);
        parameters.put("applyFineRegistration", false);
        parameters.put("inSAROptimized", false);
        parameters.put("fineRegistrationWindowWidth", "64");
        parameters.put("fineRegistrationWindowHeight", "64");        
        
        return runSnapOperator("Cross-Correlation", parameters, prod);
	}
	
	public static Product warpOperator(Product prod) {
        Map<String,Object> parameters = new HashMap<String,Object>();
        parameters.put("rmsThreshold", 1.0);
        parameters.put("warpPolynomialOrder", 1);
        parameters.put("interpolationMethod", "Bilinear interpolation");
        
        return runSnapOperator("Warp", parameters, prod);
	}
	
	public static Product terrainCorrectionOperator(Product prod) {
        Map<String,Object> parameters = new HashMap<String,Object>();
        parameters.put("nodataValueAtSea", false);
        parameters.put("pixelSpacingInMeter", 10.01);
        parameters.put("pixelSpacingInDegree", 8.992135994036409E-5);
        parameters.put("mapProjection",
        			   "GEOGCS[\"WGS84(DD)\"," +
        			   "DATUM[\"WGS84\"," +                                       
                       "SPHEROID[\"WGS 84\", 6378137.0, 298.257223563, AUTHORITY[\"EPSG\",\"7030\"]]," +
                       "AUTHORITY[\"EPSG\",\"6326\"]]," +
                       "PRIMEM[\"Greenwich\", 0.0, AUTHORITY[\"EPSG\",\"8901\"]]," +
         		       "UNIT[\"degree\", 0.017453292519943295]," +
         		       "AXIS[\"Geodetic longitude\", EAST]," +
         		       "AXIS[\"Geodetic latitude\", NORTH]]"); 
        
        return runSnapOperator("Terrain-Correction", parameters, prod);
	}
	
	public static Product bandMathsChangeDetectionOperator(Product[] products) throws IOException {
        Product stack = SnapS1Toolbox.createStackOperator(products);
        String[] bNames = stack.getBandNames();
        BandDescriptor targetBand1 = new BandDescriptor();
        targetBand1.name = "Change Detection";
        targetBand1.type = "float32";
        targetBand1.expression = "(" + bNames[0] + " > 0.0001) && ( " + bNames[1] + " > 0.0001) &&(abs(log10(" + bNames[0] + " / " + bNames[1] + ")) > 1) && ( " + bNames[0] + " > 0.05 || " + bNames[1] + " > 0.05)";
        BandDescriptor[] targetBands = new BandDescriptor[] {targetBand1};
        Map<String,Object> parameters = new HashMap<String,Object>();
        parameters.put("targetBands", targetBands);
        
        return GPF.createProduct("BandMaths", parameters, stack);
    }
}
