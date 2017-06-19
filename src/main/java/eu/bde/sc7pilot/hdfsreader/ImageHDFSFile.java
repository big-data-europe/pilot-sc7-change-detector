package eu.bde.sc7pilot.hdfsreader;

import java.awt.Rectangle;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.io.IOException;

import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;

import org.esa.snap.core.datamodel.ProductData;

public class ImageHDFSFile {

    private ImageReader reader;

    public void readImageIORasterBand(final int sourceOffsetX, final int sourceOffsetY, final int sourceStepX,
            final int sourceStepY, final ProductData destBuffer, final int destOffsetX, final int destOffsetY,
            final int destWidth, final int destHeight, final int imageID, final int bandSampleOffset)
            throws IOException {

        final ImageReadParam param = reader.getDefaultReadParam();
        param.setSourceSubsampling(sourceStepX, sourceStepY, sourceOffsetX % sourceStepX, sourceOffsetY % sourceStepY);
        final Raster data = getData(param, destOffsetX, destOffsetY, destWidth, destHeight);
        final DataBuffer dataBuffer = data.getDataBuffer();
        final SampleModel sampleModel = data.getSampleModel();
        final int dataBufferType = dataBuffer.getDataType();
        //Checking sampleOffset
        final int sampleOffset = imageID + bandSampleOffset;	// Den kserw ti kanei, pantws dhmiourgei provlima kai DEN trexei h sampleModel.getSamples
        final Object dest = destBuffer.getElems();
//        String devsMSG = "[Dev's MSG]\t";		// Activate this string and the corresponding messages to see the implementation 

        try {
            if (dest instanceof int[] && (dataBufferType == DataBuffer.TYPE_USHORT || dataBufferType == DataBuffer.TYPE_SHORT || dataBufferType == DataBuffer.TYPE_INT)) {
                sampleModel.getSamples(0, 0, destWidth, destHeight, sampleOffset, (int[]) dest, dataBuffer);
            }
            else if (dataBufferType == DataBuffer.TYPE_FLOAT && dest instanceof float[]) {
                sampleModel.getSamples(0, 0, destWidth, destHeight, sampleOffset, (float[]) dest, dataBuffer);
            }
            else if (dataBufferType == DataBuffer.TYPE_DOUBLE && dest instanceof double[]) {
                sampleModel.getSamples(0, 0, destWidth, destHeight, sampleOffset, (double[]) dest, dataBuffer);
            }
            else {
                final double[] dArray = new double[destWidth * destHeight];
                if(sampleOffset == 0) {
                    // dArray will take pixel values from dataBuffer as it used to do
//                	System.out.println("\n" + devsMSG + "sampleOffset = " + sampleOffset + "\tReading Single-Tiff Image's pixels.");
                	sampleModel.getSamples(0, 0, data.getWidth(), data.getHeight(), sampleOffset, dArray, dataBuffer);
                    int i = 0;
                    for (double value : dArray) {
                    	// Tis dinei sto destBuffer!
                        destBuffer.setElemDoubleAt(i++, value);
                    }
                }
                else {
                	// dArray will take pixel values probably from the dataBuffer.getElemDouble (probably) with indeces = 0, 2, 4, 6, 8... till the end
//                	System.out.println("\n" + devsMSG + "sampleOffset = " + sampleOffset + "\tReading Multi-Tiff Image's pixels.");
                	int mySasmpleOffset = 1;
                	sampleModel.getSamples(0, 0, data.getWidth(), data.getHeight(), mySasmpleOffset, dArray, dataBuffer);
                    int i = 0;
                    for (double value : dArray) {
                    	// Tis dinei sto destBuffer!
                        destBuffer.setElemDoubleAt(i++, value);
                    }
                }
            }
        } 
        catch (Exception e) {
            try {
                // dArray will take pixel values from dataBuffer.getElemDouble(0) till the half of dataBuffer.getElemDouble(i)
            	// If any problem occurs, switch between "Handling Exception vol. I" and "Handling Exception vol. II".
//            	System.out.println("\n" + devsMSG + "Handling Exception vol. I: Reading pixels sequentially from the start of dataBuffer.");
                final double[] dArray = new double[destWidth * destHeight];
                for(int i = 0; i < dArray.length; i++) {
                	dArray[i] = dataBuffer.getElemDouble(i);
            	}
            } 
            catch (Exception e2) {
//            	System.out.println("\n" + devsMSG + "Handling Exception vol. II: Reading pixels sequentially from the middle of dataBuffer.");
                final double[] dArray = new double[destWidth * destHeight];
                int halfDataBuffer = dataBuffer.getSize() / 2;
                for (int i = 0; i < dArray.length; i++) {
                	int j = halfDataBuffer + i;
                	dArray[i] = dataBuffer.getElemDouble(j);
                    destBuffer.setElemDoubleAt(i++, 0);
                }
            }
        }
    }

    private synchronized Raster getData(final ImageReadParam param,
            final int destOffsetX, final int destOffsetY,
            final int destWidth, final int destHeight) throws IOException {
        try {
            final RenderedImage image = reader.readAsRenderedImage(0, param);
            return image.getData(new Rectangle(destOffsetX, destOffsetY, destWidth, destHeight));
        } catch (Exception e) {
//			if (ZipUtils.isZip(productInputFile) && !ZipUtils.isValid(productInputFile)) {
//				throw new IOException("Zip file is corrupt " + productInputFile.getName());
//			}
            throw e;
        }
    }

    public void setImageReader(ImageReader imageReader) {
        reader = imageReader;
    }

}
