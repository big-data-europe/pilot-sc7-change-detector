package readerHDFS;

import java.awt.Rectangle;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.io.IOException;

import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;

import org.esa.s1tbx.io.imageio.ImageIOFile;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.engine_utilities.datamodel.Unit;

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
		final int sampleOffset = imageID + bandSampleOffset;
		final Object dest = destBuffer.getElems();

		try {
			if (dest instanceof int[] && (dataBufferType == DataBuffer.TYPE_USHORT
					|| dataBufferType == DataBuffer.TYPE_SHORT || dataBufferType == DataBuffer.TYPE_INT)) {
				sampleModel.getSamples(0, 0, destWidth, destHeight, sampleOffset, (int[]) dest, dataBuffer);
			} else if (dataBufferType == DataBuffer.TYPE_FLOAT && dest instanceof float[]) {
				sampleModel.getSamples(0, 0, destWidth, destHeight, sampleOffset, (float[]) dest, dataBuffer);
			} else if (dataBufferType == DataBuffer.TYPE_DOUBLE && dest instanceof double[]) {
				sampleModel.getSamples(0, 0, destWidth, destHeight, sampleOffset, (double[]) dest, dataBuffer);
			} else {
				final double[] dArray = new double[destWidth * destHeight];
				sampleModel.getSamples(0, 0, data.getWidth(), data.getHeight(), sampleOffset, dArray, dataBuffer);
				int i = 0;
				for (double value : dArray) {
					destBuffer.setElemDoubleAt(i++, value);
				}
			}
		} catch (Exception e) {
			try {
				final double[] dArray = new double[destWidth * destHeight];
				sampleModel.getSamples(0, 0, data.getWidth(), data.getHeight(), sampleOffset, dArray, dataBuffer);
				int i = 0;
				for (double value : dArray) {
					destBuffer.setElemDoubleAt(i++, value);
				}
			} catch (Exception e2) {
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
		} 
		catch (Exception e) {
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
