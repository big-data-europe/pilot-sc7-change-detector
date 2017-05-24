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
        System.out.println("\n\timageID \t\t= " + imageID);
        System.out.println("\tbandSampleOffset \t= " + bandSampleOffset);
        System.out.println("\tsampleOffset \t\t= " + sampleOffset + "\n");
        // final int sampleOffset = 1;
        final Object dest = destBuffer.getElems();

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
            	int n = 0;
                final double[] dArray = new double[destWidth * destHeight];
                //*** Checking dArray before getting values
                System.out.println("\ndArrayBeforeSIZE =\t" + dArray.length);
                System.out.println("dataBufferSIZE =\t" + dataBuffer.getSize());
                System.out.println("dataBufferBANKS =\t" + dataBuffer.getNumBanks() + "\n");
                for(int i = 0; i < 10; i++) {
                	System.out.println("dArrayBefore[" + i + "] =\t" + dArray[i]);
                	System.out.println("dataBufferFLOAT[" + i + "] =\t" + dataBuffer.getElemFloat(i) + "\n");
                }
                System.out.println("\n");
                //~~~END OF Checking dArray before getting values
                
//                sampleModel.getSamples(0, 0, data.getWidth(), data.getHeight(), sampleOffset, dArray, dataBuffer);	// "Original" implementation
                
                //*** My implementation
                if(sampleOffset == 0) {
                	System.out.println("\n\n\tI THINK WE ARE IN SINGLE-TIFF IMAGE\n\n");
                	sampleModel.getSamples(0, 0, data.getWidth(), data.getHeight(), sampleOffset, dArray, dataBuffer);
                }
                else {
                	System.out.println("\n\n\tI THINK WE ARE IN MULTI-TIFF IMAGE\n\n");
                	for(int i = 0; i < dArray.length; i++) {
                		dArray[i] = dataBuffer.getElemDouble(i);
                	}
                }
                //~~~ END OF My Implementation
                
                //*** Checking dArray after getting values
                System.out.println("\n\tdArrayAfterSIZE \t= " + dArray.length);
                System.out.println("\tdataBufferSIZE \t\t= " + dataBuffer.getSize());
                System.out.println("\tdataBufferBANKS \t= " + dataBuffer.getNumBanks() + "\n");
                for(int i = 0; i < 10; i++) {
                	System.out.println("dArrayAfter[" + i + "] =\t" + dArray[i]);
                	System.out.println("dataBufferFLOAT[" + i + "] =\t" + dataBuffer.getElemFloat(i) + "\n");
                }
                System.out.println("\n");
                //~~~END OF Checking dArray after getting values
                
                int i = 0;
                System.out.println("destBufferElemSizeBefore =\t" + destBuffer.getElemSize());
                System.out.println("destBufferNumElemsBefore =\t" + destBuffer.getNumElems());
                for (double value : dArray) {
                	// Tis dinei sto destBuffer!
                    destBuffer.setElemDoubleAt(i++, value);
                }
                System.out.println("");
                for(int j = 0; j < 10; j++) {
                	System.out.println("destBuffer[" + j + "] =\t" + destBuffer.getElemDoubleAt(j));
                }
                System.out.println("\ndestBufferElemSizeAfter =\t" + destBuffer.getElemSize());
                System.out.println("destBufferNumElemsAfter =\t" + destBuffer.getNumElems() + "\n");
                for(int j = 0; j < destBuffer.getNumElems(); j++) {
                	if (destBuffer.getElemDoubleAt(j) == dataBuffer.getElemDouble(j)) {
                		n++;
                	}
                	
                }
                double success = (n / destBuffer.getNumElems()) * 100;
                System.out.println("\n\n");
                System.out.println("\tdataBufferBANKS \t= " + dataBuffer.getNumBanks());
                System.out.println("\tdataBufferSIZE \t\t= " + dataBuffer.getSize());
                System.out.println("\tdestBufferNumElems \t= " + destBuffer.getNumElems());
                System.out.println("\tdestBufferElemSize \t= " + destBuffer.getElemSize());
                System.out.println("The first\t" + n + "\tpixels from dataBuffer transfered to destBuffer correctly");
                System.out.println("We have\t\t" + success + "%\tsuccess!");
                System.out.println("\n\n");
            }
        } 
        catch (Exception e) {
            try {
                final double[] dArray = new double[destWidth * destHeight];
                sampleModel.getSamples(0, 0, data.getWidth(), data.getHeight(), sampleOffset, dArray, dataBuffer);
                int i = 0;
                for (double value : dArray) {
                    destBuffer.setElemDoubleAt(i++, value);
                }
            } 
            catch (Exception e2) {
                int size = destWidth * destHeight;
                for (int i = 0; i < size; ++i) {
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
