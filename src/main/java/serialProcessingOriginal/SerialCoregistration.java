package serialProcessingOriginal;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.esa.s1tbx.insar.gpf.coregistration.GCPManager;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Placemark;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductNodeGroup;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.gpf.OperatorSpiRegistry;

import scala.Tuple2;

public class SerialCoregistration {

    public static void main(String[] args) {
        String filesPath = "/home/efi/SNAP/sentinel-images/";
        File targetFile = new File(filesPath, "ORIGINALALL-FINAL");
        File masterFile = new File(filesPath, "subset_0_of_S1A_IW_GRDH_1SDV_20151110T145915_20151110T145940_008544_00C1A6_F175.dim");
        File slaveFile = new File(filesPath, "subset_1_of_S1A_IW_GRDH_1SDV_20151029T145915_20151029T145940_008369_00BD0B_334C.dim");
        final OperatorSpiRegistry spiRegistry = GPF.getDefaultInstance().getOperatorSpiRegistry();
        spiRegistry.loadOperatorSpis();

        MyRead readOp1 = new MyRead(masterFile, spiRegistry);
        MyRead readOp2 = new MyRead(slaveFile, spiRegistry);
        
        Boolean[] bParams={false,false,false,false,true,false,false,false};	
        MyCalibration myCalibration1=new MyCalibration();
        myCalibration1.setSourceProduct(readOp1.targetProduct);	
        myCalibration1.setParameters(null,bParams,null);

        MyCalibration myCalibration2=new MyCalibration();	
        myCalibration2.setSourceProduct(readOp2.targetProduct);	
        myCalibration2.setParameters(null,bParams,null);
        
        
        Product[] sourcesForCreateStack = new Product[2];
        sourcesForCreateStack[0] = myCalibration1.targetProduct;
        sourcesForCreateStack[1] = myCalibration2.targetProduct;
        

        String[] parameters = {"NONE", "Master", "Orbit"};

        MyCreateStack createStackOp = new MyCreateStack();
        createStackOp.setSourceProducts(sourcesForCreateStack);
        createStackOp.setParameters(parameters[0], parameters[1], parameters[2]);
        Product sourceForGCP = createStackOp.targetProduct;

        MyGCPSelection GCPSelectionOp = new MyGCPSelection();
        GCPSelectionOp.setSourceProduct(sourceForGCP);
        GCPSelectionOp.setParameters(1000, "128", "128", "4", "4", 10, 0.25, false, "32", "32", 3, 0.6, false, false, false);
        Product sourceForWarp = GCPSelectionOp.targetProduct;

        MyWarp warpOp = new MyWarp();
        warpOp.setSourceProduct(sourceForWarp);
        warpOp.setParameters(0.05f, 1, "Bilinear interpolation", false, false);
        Product sourceForChangeDetection = warpOp.targetProduct;

        boolean[] bParams2 = {false, false};
        float[] fParams = {2.0f, -2.0f};
        MyChangeDetection myChangeDetection = new MyChangeDetection();
        myChangeDetection.setSourceProduct(sourceForChangeDetection);
        myChangeDetection.setParameters(fParams[0], fParams[1], bParams2[0], bParams2[1]);
        Product sourceForWrite = myChangeDetection.targetProduct;
        
        MyWrite writeOp = new MyWrite();
        writeOp.setParameters(targetFile, "BEAM-DIMAP");
        writeOp.setSourceProduct(sourceForWrite);
        writeOp.computeProduct();
    }
    public static void  testGCPs(Map<String, Map<Integer, Placemark>> gcpsMap) {
		Map<String, Map<PixelPos,Placemark>> gcpPosMap=new HashMap<String, Map<PixelPos,Placemark>>();
		for(String name: gcpsMap.keySet()){
			Map<Integer, Placemark> map=gcpsMap.get(name);
			 Map<PixelPos,Placemark> pmap=new HashMap<PixelPos,Placemark>();
			 for(Placemark entry: map.values())
				 pmap.put(entry.getPixelPos(), entry);
			 gcpPosMap.put(name, pmap);
		}
    	String filesPath = "/home/efi/SNAP/sentinel-images/";
        File targetFile = new File(filesPath, "serial-original-warp");
//         File masterFile = new File(filesPath, "subset_0_of_S1A_IW_GRDH_1SDV_20151110T145915_20151110T145940_008544_00C1A6_F175.dim");
//        File slaveFile = new File(filesPath, "subset_1_of_S1A_IW_GRDH_1SDV_20151029T145915_20151029T145940_008369_00BD0B_334C.dim");
//        File masterFile = new File(filesPath,
//				"subset3_of_S1A_IW_GRDH_1SSV_20141225T142407_20141225T142436_003877_004A54_040F.dim");
//		File slaveFile = new File(filesPath,
//				"subset3_of_S1A_IW_GRDH_1SSV_20150518T142409_20150518T142438_005977_007B49_AF76.dim");
        File masterFile = new File(filesPath, "subset_0_of_S1A_IW_GRDH_1SDV_20151110T145915_20151110T145940_008544_00C1A6_F175.dim");
        File slaveFile = new File(filesPath, "subset_1_of_S1A_IW_GRDH_1SDV_20151029T145915_20151029T145940_008369_00BD0B_334C.dim");
        final OperatorSpiRegistry spiRegistry = GPF.getDefaultInstance().getOperatorSpiRegistry();
        spiRegistry.loadOperatorSpis();

        MyRead readOp1 = new MyRead(masterFile, spiRegistry);
        MyRead readOp2 = new MyRead(slaveFile, spiRegistry);
        
        Boolean[] bParams={false,false,false,false,true,false,false,false};	
        MyCalibration myCalibration1=new MyCalibration();
        myCalibration1.setSourceProduct(readOp1.targetProduct);	
        myCalibration1.setParameters(null,bParams,null);

        MyCalibration myCalibration2=new MyCalibration();	
        myCalibration2.setSourceProduct(readOp2.targetProduct);	
        myCalibration2.setParameters(null,bParams,null);
        
        
        Product[] sourcesForCreateStack = new Product[2];
        sourcesForCreateStack[0] = myCalibration1.targetProduct;
        sourcesForCreateStack[1] = myCalibration2.targetProduct;
        

        String[] parameters = {"NONE", "Master", "Orbit"};

        MyCreateStack createStackOp = new MyCreateStack();
        createStackOp.setSourceProducts(sourcesForCreateStack);
        createStackOp.setParameters(parameters[0], parameters[1], parameters[2]);
        Product sourceForGCP = createStackOp.targetProduct;

        MyGCPSelection GCPSelectionOp = new MyGCPSelection();
        GCPSelectionOp.setSourceProduct(sourceForGCP);
        GCPSelectionOp.setParameters(1000, "128", "128", "4", "4", 10, 0.25, false, "32", "32", 3, 0.6, false, false, false);
        GCPSelectionOp.computeProduct();
        
        for(int i=0;i<GCPSelectionOp.targetProduct.getNumBands();i++){
    	final ProductNodeGroup<Placemark> targetGCPGroup = GCPManager.instance()
				.getGcpGroup(GCPSelectionOp.targetProduct.getBandAt(i));
    	Map<PixelPos,Placemark> pmap=gcpPosMap.get(GCPSelectionOp.targetProduct.getBandAt(i).getName());
		
		if(pmap==null)
			continue;
		System.out.println("init size for gcps of band "+GCPSelectionOp.targetProduct.getBandAt(i).getName()+" "+pmap.size());
		System.out.println("size for correct gcps of band "+GCPSelectionOp.targetProduct.getBandAt(i).getName()+" "+targetGCPGroup.getNodeCount());
		for(int j=0;j<targetGCPGroup.getNodeCount();j++){
			if(pmap.containsKey(targetGCPGroup.get(j).getPixelPos()))
				pmap.remove(targetGCPGroup.get(j).getPixelPos());
			else
				System.out.println(targetGCPGroup.get(j).getPixelPos());
			
		}
		System.out.println("final size for gcps of band "+GCPSelectionOp.targetProduct.getBandAt(i).getName()+" "+pmap.size());
	}
    }
    
}
