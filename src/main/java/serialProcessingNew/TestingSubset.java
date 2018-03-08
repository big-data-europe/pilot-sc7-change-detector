package serialProcessingNew;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.media.jai.PlanarImage;
import javax.media.jai.TiledImage;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.RasterDataNode;
import org.esa.snap.core.gpf.Tile;
import org.esa.snap.core.image.ImageManager;
import org.esa.snap.core.util.ImageUtils;
import org.esa.snap.engine_utilities.gpf.OperatorUtils;

public class TestingSubset {

	public static void main(String[] args) throws IOException {
		
		// args[0] : path
		// args[1] : source_file
		// args[2] : polygon
		
		
		if(args.length<=2) {
			throw new IOException("Error:Invalid Args");
		}
		
		String sourceFilePath = args[0] + args[1];
		String zipName = "";
		String[] parts = args[1].split("\\.");
		if (parts[1].equals("zip")) {
			zipName = parts[0];
		}
		else {
			System.out.println("Cannot recognize file type");
		}
		String outputFilePath = args[0] + "xlSub_of_" + zipName;
		File sourceFile = new File(sourceFilePath);
		File targetFile = new File(outputFilePath);
		
		SerialProcessor processor = new SerialProcessor();
		//String[] selectedPolarisations = null;
		//String[] selectedPolarisations = {"VH"};
		String[] selectedPolarisations= {"VV"};
		
		MyRead myRead = new MyRead(sourceFile, "read");
		processor.getBufferedImage(myRead,selectedPolarisations);
		Product readProduct = myRead.getTargetProduct();
		
		String polygon = args[2];
		for (int i=3; i<args.length; i++)
		{
			polygon+=" "+args[i];
		}
		System.out.println(polygon);
        
		long startTime = System.currentTimeMillis();
		
		MySubset test = new MySubset(readProduct,polygon,"subset");
		getBufferedImage(test, selectedPolarisations);
		
		long stopTime = System.currentTimeMillis();
		long executionTime = stopTime - startTime;
		System.out.println(executionTime + " ms, to calculate subset");
	
		MyWrite writeOp = new MyWrite(test.getTargetProduct(), targetFile, "BEAM-DIMAP");
        writeOp.setId("write");
        processor.initOperatorForMultipleBands(writeOp);
        //processor.storeResult(writeOp);
        processor.myStoreResult(writeOp, selectedPolarisations);
        
        MyWrite writeOp2 = new MyWrite(test.getTargetProduct(), targetFile, "GeoTIFF");
        writeOp2.setId("write");
        processor.initOperatorForMultipleBands(writeOp2);
        //processor.storeResult(writeOp2);
        processor.myStoreResult(writeOp2, selectedPolarisations);
        
        //Deleting input and unwanted output
//        if (sourceFile.exists()) {
//        	sourceFile.delete();
//        	System.out.println("Input-Image deleted succesfully!");
//        }
//        else {
//        	System.out.println("Cannot delete Input-Image");
//        }
        
	}
	
	// getBufferedImage needs to take AbstractOperator instead of my Read as first parameter
	// possible change in SerialProcessor won't break existing functionality (presumably)
	public static void getBufferedImage(AbstractOperator op,String[] selectedPolarisations) {
		Product targetProduct = op.getTargetProduct();
		LoopLimits limits = new LoopLimits(targetProduct);
		int noOfBands = targetProduct.getNumBands();
		for (int i = 0; i < noOfBands; i++) {
			Band band = targetProduct.getBandAt(i);

			if (selectedPolarisations != null) {
				Set<String> selectedPols = new HashSet(Arrays.asList(Arrays.stream(selectedPolarisations).map(s -> s.toLowerCase()).toArray(String[]::new)));
				String pol = OperatorUtils.getPolarizationFromBandName(band.getName());
				if (!selectedPols.contains(pol.toLowerCase()))
					continue;
			}
			//System.out.println(band.getClass().getName());
			if (band.getClass() == Band.class) {
				TiledImage image = createSourceImages(band);
//				System.out.println("MySubset - Band:" + i + " " + band.getName());
//				System.out.println("MySubset - BandType:" + band.getDataType());
//				System.out.println("MySubset - NumXTiles: " + limits.getNumXTiles() + " NumYTiles: " + limits.getNumYTiles());
//				System.out.println("MySubset - Number of elements: " + band.getNumDataElems());
				
				for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
					for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
						GetTile getTile = new GetTile(band, op);
						Tile tile = getTile.computeTile(tileX, tileY);
						
//						if(tileX==0 && tileY==0) {
//							System.out.println("Subset:" + Arrays.toString(tile.getSamplesDouble()).substring(0,50));
//						}
						
						WritableRaster raster = createWritableRaster(tile.getRectangle(), band);
						raster.setDataElements(tile.getMinX(), tile.getMinY(), tile.getWidth(), tile.getHeight(),
								tile.getRawSamples().getElems());
						image.setData(raster);
					}
				}
				band.setSourceImage(image);
				op.addTargetImage(band, image);
			}
		}
	}
	
	private static TiledImage createSourceImages(Band band) {
		Product product = band.getProduct();
		int width = product.getSceneRasterWidth();
		int height = product.getSceneRasterHeight();
		int bufferType = ImageManager.getDataBufferType(band.getDataType());
		final SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(bufferType,
				product.getPreferredTileSize().width, product.getPreferredTileSize().height);
		final ColorModel cm = PlanarImage.createColorModel(sampleModel);
		return new TiledImage(0, 0, width, height, 0, 0, sampleModel, cm);
	}
	
	private static WritableRaster createWritableRaster(Rectangle rectangle, RasterDataNode band) {
		final int dataBufferType = ImageManager.getDataBufferType(band.getDataType());
		SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(dataBufferType, rectangle.width,
				rectangle.height);
		final Point location = new Point(rectangle.x, rectangle.y);
		return Raster.createWritableRaster(sampleModel, sampleModel.createDataBuffer(), location);
	}
	
}
