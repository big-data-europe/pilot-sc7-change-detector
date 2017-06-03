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
        System.out.println("TREXEI readImageIORasterBand");
        System.out.println("\n\timageID \t\t= " + imageID);
        System.out.println("\tbandSampleOffset \t= " + bandSampleOffset);
        System.out.println("\tsampleOffset \t\t= " + sampleOffset + "\n");
        // final int sampleOffset = 1;
        final Object dest = destBuffer.getElems();

        try {
        	System.out.println("TRY IF-ELSE-IF IN ImageHDFSFile.readImageIORasterBand");
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
            	System.out.println("TRY ELSE IN ImageHDFSFile.readImageIORasterBand");
            	int n = 0;
            	// dArray will take pixel values from dataBuffer.getElemDouble(0) till dataBuffer.getElemDouble(halfDataBuffer)
                final double[] dArray = new double[destWidth * destHeight];
                // dArray2 will take pixel values from dataBuffer.getElemDouble(halfDataBuffer) till the end of dataBuffer.getElemDouble(i)
                final double[] dArray2 = new double[destWidth * destHeight];
                // dArray3 will take pixel values probably from the dataBuffer.getElemDouble with indeces = 0, 2, 4, 6, 8... till the end
                final double[] dArray3 = new double[destWidth * destHeight];
                int halfDataBuffer = dataBuffer.getSize() / 2;
                //*** Checking dArray before getting values
                System.out.println("\ndArrayBeforeSIZE =\t" + dArray.length);
                System.out.println("dataBufferSIZE =\t" + dataBuffer.getSize());
                System.out.println("halfDataBufferSIZE =\t" + halfDataBuffer);
                System.out.println("dataBufferBANKS =\t" + dataBuffer.getNumBanks() + "\n");
                for(int i = 0; i < 10; i++) {
                	int j = halfDataBuffer + i;
                	System.out.println("dArrayBefore[" + i + "] =\t" + dArray[i]);
//                	System.out.println("dataBufferFLOAT[" + i + "] =\t" + dataBuffer.getElemFloat(i));
//                	System.out.println("\t~~~~~~~~~~~~~~~~~~~~~~");
                	System.out.println("@@@ dArray2Before[" + i + "] =\t" + dArray2[i] + "\n");
//                	System.out.println("dataBufferFLOAT_HALF[" + j + "] =\t" + dataBuffer.getElemFloat(j) + "\n");
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
                	int mySasmpleOffset = 1;
                	sampleModel.getSamples(0, 0, data.getWidth(), data.getHeight(), mySasmpleOffset, dArray3, dataBuffer);
                	System.out.println("halfDataBuffer =\t" + halfDataBuffer);
                	System.out.println("\n\n\tI THINK WE ARE IN MULTI-TIFF IMAGE\n\n");
                	for(int i = 0; i < dArray.length; i++) {
                		int j = halfDataBuffer + i;
                		dArray[i] = dataBuffer.getElemDouble(i);
                		dArray2[i] = dataBuffer.getElemDouble(j);
                	}
                	for(int i = 0; i < 10; i++) {
                		System.out.println("&&& dArray3After[" + i + "] =\t" + dArray3[i]);                		
                	}
                	System.out.println("\n");
                	for(int i = 0; i < 10; i++) {
                		System.out.println("dataBufferFLOAT[" + i + "] =\t" + dataBuffer.getElemFloat(i));                		
                	}
                	System.out.println("\n");
                	for(int i = 0; i < 10; i++) {
                		System.out.println("dArrayAfter[" + i + "] =\t" + dArray[i]);                		
                	}
                	System.out.println("\n");
                	for(int i = 0; i < 10; i++) {
                		System.out.println("@@@ dArray2After[" + i + "] =\t" + dArray2[i]);                		
                	}
                	System.out.println("\n");
                	for(int i = 0; i < 10; i++) {
                		int j = halfDataBuffer + i;
                		System.out.println("dataBufferFLOAT_HALF[" + j + "] =\t" + dataBuffer.getElemFloat(j));        
                	}
                	System.out.println("\n");
                }
                //~~~ END OF My Implementation
                
                //*** Checking dArray after getting values
                System.out.println("\n\tdArrayAfterSIZE \t= " + dArray.length);
                System.out.println("\tdataBufferSIZE \t\t= " + dataBuffer.getSize());
                System.out.println("\tdataBufferBANKS \t= " + dataBuffer.getNumBanks() + "\n");
//                for(int i = 0; i < 10; i++) {
//                	int j = halfDataBuffer + i;
//                	System.out.println("dArrayAfter[" + i + "] =\t" + dArray[i]);
//                	System.out.println("dataBufferFLOAT[" + i + "] =\t" + dataBuffer.getElemFloat(i));
//                	System.out.println("\t~~~~~~~~~~~~~~~~~~~~~~");
//                	System.out.println("@@@ dArray2After[" + i + "] =\t" + dArray2[i]);
//                	System.out.println("dataBufferFLOAT_HALF[" + j + "] =\t" + dataBuffer.getElemFloat(j));
//                	System.out.println("\t~~~~~~~~~~~~~~~~~~~~~~");
//                	System.out.println("%%% dArray3After[" + i + "] =\t" + dArray3[i] + "\n");
//                }
                System.out.println("\n");
                //~~~END OF Checking dArray after getting values
                
                int i = 0;
                System.out.println("destBufferElemSizeBefore =\t" + destBuffer.getElemSize());
                System.out.println("destBufferNumElemsBefore =\t" + destBuffer.getNumElems());
                System.out.println("PASSING PIXEL-VALUES FROM dArray3 to destBuffer");
                for (double value : dArray3) {
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
            	System.out.println("HANDLING EXCEPTION1 IN ImageHDFSFile.readImageIORasterBand");
            	//MY EXCEPTION HANDLING!
            	int n = 0;
                final double[] dArray = new double[destWidth * destHeight];
                //final double[] dArray2 = new double[destWidth * destHeight];
                final double[] dArray3 = new double[destWidth * destHeight];
                int halfDataBuffer = dataBuffer.getSize() / 2;
                //*** Checking dArray before getting values
                System.out.println("\ndArrayBeforeSIZE =\t" + dArray.length);
                System.out.println("dataBufferSIZE =\t" + dataBuffer.getSize());
                System.out.println("@@@ halfDataBufferSIZE =\t" + halfDataBuffer);
                System.out.println("dataBufferBANKS =\t" + dataBuffer.getNumBanks() + "\n");
                for(int i = 0; i < 10; i++) {
                	int j = halfDataBuffer + i;
                	System.out.println("dArrayBefore[" + i + "] =\t" + dArray[i]);
                	System.out.println("dataBufferFLOAT[" + i + "] =\t" + dataBuffer.getElemFloat(i));
                	System.out.println("\t~~~~~~~~~~~~~~~~~~~~~~");
                	//System.out.println("@@@ dArray2Before[" + i + "] =\t" + dArray2[i]);
                	System.out.println("dataBufferFLOAT_HALF[" + j + "] =\t" + dataBuffer.getElemFloat(j) + "\n");
                }
                System.out.println("\n");
                //~~~END OF Checking dArray before getting values
                
//                sampleModel.getSamples(0, 0, data.getWidth(), data.getHeight(), sampleOffset, dArray, dataBuffer);	// "Original" implementation
                
                //*** My implementation
                if(sampleOffset == 0) {
                	System.out.println("\n\n\tI THINK WE ARE IN SINGLE-TIFF IMAGE [run in Exceptio mode! LOL]\n\n");
                	sampleModel.getSamples(0, 0, data.getWidth(), data.getHeight(), sampleOffset, dArray, dataBuffer);
                }
                else {
                	int mySampleOffset = 0;
                	sampleModel.getSamples(0, 0, data.getWidth(), data.getHeight(), mySampleOffset, dArray3, dataBuffer);
                	System.out.println("halfDataBuffer =\t" + halfDataBuffer);
                	System.out.println("\n\n\tI THINK WE ARE IN MULTI-TIFF IMAGE [run in Exceptio mode! LOL]\n\n");
                	int p = 0;
                	for(int i = 0; i < dArray.length; i++) {
//                		int j = halfDataBuffer + i;
                		dArray[i] = dataBuffer.getElemDouble(i);
                		p++;
//                		dArray2[i] = dataBuffer.getElemDouble(j);
                	}
                	System.out.println("WTF???, I ran: " + p + "fucking times!!!");
                	for(int i = 0; i < 10; i++) {
                		System.out.println("dataBufferFLOAT[" + i + "] =\t" + dataBuffer.getElemFloat(i));                		
                	}
                	System.out.println("\n");
                	for(int i = 0; i < 10; i++) {
                		System.out.println("dArrayAfter[" + i + "] =\t" + dArray[i]);                		
                	}
                	System.out.println("\n");
//                	for(int i = 0; i < 10; i++) {
//                		System.out.println("@@@ dArray2After[" + i + "] =\t" + dArray2[i]);                		
//                	}
                	System.out.println("\n");
                	for(int i = 0; i < 10; i++) {
                		System.out.println("&&& dArray3After[" + i + "] =\t" + dArray3[i]);                		
                	}
                	System.out.println("\n");
                	for(int i = 0; i < 10; i++) {
                		int j = halfDataBuffer + i;
                		System.out.println("dataBufferFLOAT_HALF[" + j + "] =\t" + dataBuffer.getElemFloat(j));        
                	}
                	System.out.println("\n");
                }
                //~~~ END OF My Implementation
                
                //*** Checking dArray after getting values
                System.out.println("\n\tdArrayAfterSIZE \t= " + dArray.length);
                System.out.println("\tdataBufferSIZE \t\t= " + dataBuffer.getSize());
                System.out.println("\tdataBufferBANKS \t= " + dataBuffer.getNumBanks() + "\n");
//                for(int i = 0; i < 10; i++) {
//                	int j = halfDataBuffer + i;
//                	System.out.println("dArrayAfter[" + i + "] =\t" + dArray[i]);
//                	System.out.println("dataBufferFLOAT[" + i + "] =\t" + dataBuffer.getElemFloat(i));
//                	System.out.println("\t~~~~~~~~~~~~~~~~~~~~~~");
//                	System.out.println("@@@ dArray2After[" + i + "] =\t" + dArray2[i]);
//                	System.out.println("dataBufferFLOAT_HALF[" + j + "] =\t" + dataBuffer.getElemFloat(j));
//                	System.out.println("\t~~~~~~~~~~~~~~~~~~~~~~");
//                	System.out.println("%%% dArray3After[" + i + "] =\t" + dArray3[i] + "\n");
//                }
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
                
            	// EFI'S EXCEPTION HANDLING
//                final double[] dArray = new double[destWidth * destHeight];
//                sampleModel.getSamples(0, 0, data.getWidth(), data.getHeight(), sampleOffset, dArray, dataBuffer);
//                int i = 0;
//                for (double value : dArray) {
//                    destBuffer.setElemDoubleAt(i++, value);
//                }
            } 
            catch (Exception e2) {
            	System.out.println("HANDLING EXCEPTION2 IN ImageHDFSFile.readImageIORasterBand");
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
