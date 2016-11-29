package tileBased.metadata;

import java.awt.Dimension;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;

import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.TiePointGeoCoding;
import org.esa.snap.core.datamodel.TiePointGrid;
import org.esa.snap.core.util.ImageUtils;
import org.esa.snap.core.util.jai.JAIUtils;

public class ImageMetadata {
	private final int minX = 0;
	private final int minY = 0;
	private final int tileGridXOffset = 0;
	private final Dimension tileSize;
	private final int imageWidth;
	private final int imageHeight;
	private final Rectangle bounds;
	private int dataType;
	private float noDataValue;
	private int tileScanlineOffset;
	private int tileScanlineStride;
	private GeoCoding geoCoding;
	private String bandName;
	private String bandPairName;
	private TiePointGrid latGrid;
	private TiePointGrid lonGrid;
	private int[] offsetMap;
	/**
	 * The Y coordinate of tile (0, 0).
	 */
	private final int tileGridYOffset = 0;
	public ImageMetadata(int dataType,int imageWidth,int imageHeight,float noDataValue,int tileScanlineOffset,int tileScanlineStride,GeoCoding geoCoding) {
		this(dataType,imageWidth,imageHeight,noDataValue,tileScanlineOffset,tileScanlineStride,geoCoding,null,null,null,null,null);
	}
	public ImageMetadata(int dataType,int imageWidth,int imageHeight,float noDataValue,int tileScanlineOffset,int tileScanlineStride,GeoCoding geoCoding,String bandName) {
		this(dataType,imageWidth,imageHeight,noDataValue,tileScanlineOffset,tileScanlineStride,geoCoding,null,null,bandName,null,null);
	}
	public ImageMetadata(int dataType,int imageWidth,int imageHeight,float noDataValue,int tileScanlineOffset,int tileScanlineStride,TiePointGrid latGrid,TiePointGrid lonGrid,String bandName,String sourceBandName) {
		this(dataType,imageWidth,imageHeight,noDataValue,tileScanlineOffset,tileScanlineStride,null,latGrid,lonGrid,bandName,sourceBandName,null);
	}
	public ImageMetadata(int dataType,int imageWidth,int imageHeight,float noDataValue,int tileScanlineOffset,int tileScanlineStride,GeoCoding geoCoding,TiePointGrid latGrid,TiePointGrid lonGrid,String bandName,String sourceBandName,int[] offsetMap) {
		this.geoCoding=geoCoding;
		this.noDataValue=noDataValue;
		this.dataType=dataType;
		this.tileSize = getPreferredTileSize(imageWidth,imageHeight);
		this.imageHeight=imageHeight;
		this.imageWidth=imageWidth;
		this.tileScanlineOffset=tileScanlineOffset;
		this.tileScanlineStride=tileScanlineStride;
		this.bandName=bandName;
		this.bandPairName=sourceBandName;
		this.latGrid=latGrid;
		this.lonGrid=lonGrid;
		bounds = new Rectangle();
		bounds.setBounds(minX, minY, imageWidth, imageHeight);
		this.offsetMap=offsetMap;
	}
	public Rectangle getRectangle(int tileX, int tileY) {
		Point org = new Point(tileXToX(tileX), tileYToY(tileY));

		int bufferType = getDataBufferType(dataType);
		final SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(bufferType, tileSize.width,
				tileSize.height);
		Rectangle rect = new Rectangle(org.x, org.y, sampleModel.getWidth(), sampleModel.getHeight());
		Rectangle destRect = rect.intersection(bounds);
		return destRect;
	}
	public WritableRaster getWritableRaster(int tileX, int tileY) {
		Point org = new Point(tileXToX(tileX), tileYToY(tileY));

		int bufferType = getDataBufferType(dataType);
		final SampleModel sampleModel = ImageUtils.createSingleBandedSampleModel(bufferType, tileSize.width,
				tileSize.height);
		final WritableRaster dest = createWritableRaster(sampleModel, org);
		return dest;
	}

	public Point getTileIndices(int x,int y) {
		
		return new Point(XToTileX(x),YToTileY(y));
	}
	private int tileXToX(int tx) {
		return tx * tileSize.width + tileGridXOffset;
	}

	/**
	 * Converts a vertical tile index into the Y coordinate of its upper left
	 * pixel relative to a given tile grid layout specified by its Y offset and
	 * tile height.
	 */
	private int tileYToY(int ty) {
		return ty * tileSize.height + tileGridYOffset;
	}
	
	private int XToTileX(int x) {
		return (x- tileGridXOffset) / tileSize.width ;
	}
	private int YToTileY(int y) {
		return (y - tileGridYOffset)/ tileSize.height ;
	}
	private static Dimension getPreferredTileSize(int width,int height) {
		return JAIUtils.computePreferredTileSize(width, height, 4);
	}
	private static int getDataBufferType(int productDataType) {
		switch (productDataType) {
		case ProductData.TYPE_INT8:
		case ProductData.TYPE_UINT8:
			return DataBuffer.TYPE_BYTE;
		case ProductData.TYPE_INT16:
			return DataBuffer.TYPE_SHORT;
		case ProductData.TYPE_UINT16:
			return DataBuffer.TYPE_USHORT;
		case ProductData.TYPE_INT32:
		case ProductData.TYPE_UINT32:
			return DataBuffer.TYPE_INT;
		case ProductData.TYPE_FLOAT32:
			return DataBuffer.TYPE_FLOAT;
		case ProductData.TYPE_FLOAT64:
			return DataBuffer.TYPE_DOUBLE;
		default:
			throw new IllegalArgumentException("productDataType");
		}
	}
	protected final WritableRaster createWritableRaster(SampleModel sampleModel, Point location) {
		if (sampleModel == null) {
			throw new IllegalArgumentException("sampleModel == null!");
		}


		if (location == null) {
			location = new Point(0, 0);
		}

		return Raster.createWritableRaster(sampleModel, sampleModel.createDataBuffer(), location);
	}
	public int getDataType() {
		return dataType;
	}
	public void setDataType(int dataType) {
		this.dataType = dataType;
	}
	public int getMinX() {
		return minX;
	}
	public int getMinY() {
		return minY;
	}
	public Dimension getTileSize() {
		return tileSize;
	}
	public int getImageWidth() {
		return imageWidth;
	}
	public int getImageHeight() {
		return imageHeight;
	}
	public float getNoDataValue() {
		return noDataValue;
	}
	public void setNoDataValue(float noDataValue) {
		this.noDataValue = noDataValue;
	}
	public int getTileScanlineOffset() {
		return tileScanlineOffset;
	}
	public void setTileScanlineOffset(int tileScanlineOffset) {
		this.tileScanlineOffset = tileScanlineOffset;
	}
	public int getTileScanlineStride() {
		return tileScanlineStride;
	}
	public void setTileScanlineStride(int tileScanlineStride) {
		this.tileScanlineStride = tileScanlineStride;
	}
	public GeoCoding getGeoCoding() {
		return geoCoding;
	}
	public void setGeoCoding(GeoCoding geoCoding) {
		this.geoCoding = geoCoding;
	}
	public String getBandName() {
		return bandName;
	}
	public void setBandName(String bandName) {
		this.bandName = bandName;
	}
	public TiePointGrid getLatGrid() {
		return latGrid;
	}
	public void setLatGrid(TiePointGrid latGrid) {
		this.latGrid = latGrid;
	}
	public TiePointGrid getLonGrid() {
		return lonGrid;
	}
	public void setLonGrid(TiePointGrid lonGrid) {
		this.lonGrid = lonGrid;
	}
	public String getBandPairName() {
		return bandPairName;
	}
	public void setSourceBandName(String sourceBandName) {
		this.bandPairName = sourceBandName;
	}
	public int[] getOffsetMap() {
		return offsetMap;
	}
	public void setOffsetMap(int[] offsetMap) {
		this.offsetMap = offsetMap;
	}
}
