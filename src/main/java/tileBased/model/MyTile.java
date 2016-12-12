package tileBased.model;

import java.awt.Rectangle;
import java.awt.image.ComponentSampleModel;
import java.awt.image.DataBuffer;
import java.awt.image.WritableRaster;
import java.io.Serializable;

import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.util.ImageUtils;

public class MyTile implements Serializable {
	private final int minX;
	private final int minY;
	private final int maxX;
	private final int maxY;
	private final int width;
	private final int height;
	private final int type;
	private final int scanlineOffset;
	private final int scanlineStride;
	private ProductData rawSamples;
	private WritableRaster writableRaster;
    private final byte[] dataBufferByte;
    private final short[] dataBufferShort;
    private final int[] dataBufferInt;
    private final float[] dataBufferFloat;
    private final double[] dataBufferDouble;
	private ProductData dataBuffer;

	public MyTile(WritableRaster writableRaster, Rectangle rectangle, int type) {
		this.minX = rectangle.x;
		this.minY = rectangle.y;
		this.maxX = rectangle.x + rectangle.width - 1;
		this.maxY = rectangle.y + rectangle.height - 1;
		this.width = rectangle.width;
		this.height = rectangle.height;
		this.type = type;
		this.writableRaster = writableRaster;

		DataBuffer db = writableRaster.getDataBuffer();
		ComponentSampleModel sm = (ComponentSampleModel) writableRaster.getSampleModel();
		int smX0 = rectangle.x - writableRaster.getSampleModelTranslateX();
		int smY0 = rectangle.y - writableRaster.getSampleModelTranslateY();
		int dbI0 = db.getOffset();
		this.scanlineStride = sm.getScanlineStride();
		this.scanlineOffset = smY0 * scanlineStride + smX0 + dbI0;

		Object primitiveArray = ImageUtils.getPrimitiveArray(db);
		this.dataBufferByte = (primitiveArray instanceof byte[]) ? (byte[]) primitiveArray : null;
		this.dataBufferShort = (primitiveArray instanceof short[]) ? (short[]) primitiveArray : null;
		this.dataBufferInt = (primitiveArray instanceof int[]) ? (int[]) primitiveArray : null;
		this.dataBufferFloat = (primitiveArray instanceof float[]) ? (float[]) primitiveArray : null;
		this.dataBufferDouble = (primitiveArray instanceof double[]) ? (double[]) primitiveArray : null;
		// this.rawSamples=ProductData.createInstance(type,this.width*this.height);
		// writableRaster.getDataElements(minX, minY, width, height,
		// rawSamples.getElems());
	}

	// public void setRawSamples(ProductData rawSamples) {
	// this.rawSamples = rawSamples;
	// writableRaster.setDataElements(minX, minY, width, height,
	// rawSamples.getElems());
	// }
	public void setRawSamples(ProductData rawSamples) {
		if (rawSamples != this.rawSamples) {
			writableRaster.setDataElements(minX, minY, width, height, rawSamples.getElems());
		}
	}

	public ProductData getDataBuffer() {
		if (dataBuffer == null) {
			dataBuffer = ProductData.createInstance(type, ImageUtils.getPrimitiveArray(writableRaster.getDataBuffer()));
		}
		return dataBuffer;
	}

	public ProductData getRawSamples() {
		if (rawSamples == null) {
			ProductData dataBuffer = getDataBuffer();
			if (width * height == dataBuffer.getNumElems()) {
				rawSamples = dataBuffer;
			}
		}
		if (rawSamples == null) {
			rawSamples = ProductData.createInstance(type, this.width * this.height);

			writableRaster.getDataElements(minX, minY, width, height, rawSamples.getElems());
		}
		return rawSamples;
	}

	public int getMinX() {
		return minX;
	}

	public int getMinY() {
		return minY;
	}

	public int getMaxX() {
		return maxX;
	}

	public int getMaxY() {
		return maxY;
	}

	public int getWidth() {
		return width;
	}

	public int getHeight() {
		return height;
	}

	public int getType() {
		return type;
	}

	public final Rectangle getRectangle() {
		return new Rectangle(minX, minY, width, height);
	}

	public int getScanlineOffset() {
		return scanlineOffset;
	}

	public int getScanlineStride() {
		return scanlineStride;
	}

	public WritableRaster getWritableRaster() {
		return writableRaster;
	}

	public void setWritableRaster(WritableRaster writableRaster) {
		this.writableRaster = writableRaster;
	}

	public byte[] getDataBufferByte() {
		return dataBufferByte;
	}

	public short[] getDataBufferShort() {
		return dataBufferShort;
	}

	public int[] getDataBufferInt() {
		return dataBufferInt;
	}

	public float[] getDataBufferFloat() {
		return dataBufferFloat;
	}

	public double[] getDataBufferDouble() {
		return dataBufferDouble;
	}

	public void setDataBuffer(ProductData dataBuffer) {
		this.dataBuffer = dataBuffer;
	}



}
