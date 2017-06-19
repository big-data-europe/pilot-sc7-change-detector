package eu.bde.sc7pilot.tilebased.operator;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.awt.image.renderable.ParameterBlock;
import java.util.Hashtable;

import javax.media.jai.BorderExtender;
import javax.media.jai.DataBufferDouble;
import javax.media.jai.JAI;
import javax.media.jai.PlanarImage;
import javax.media.jai.RasterFactory;
import javax.media.jai.operator.DFTDescriptor;

import org.esa.snap.core.datamodel.GcpDescriptor;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Placemark;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.dataop.dem.ElevationModel;
import org.esa.snap.core.dataop.dem.ElevationModelDescriptor;
import org.esa.snap.core.dataop.dem.ElevationModelRegistry;
import org.esa.snap.core.dataop.resamp.ResamplingFactory;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.Tile;
import org.esa.snap.core.util.ImageUtils;
import org.esa.snap.core.util.math.MathUtils;
import org.esa.snap.engine_utilities.gpf.OperatorUtils;

import eu.bde.sc7pilot.taskbased.Utils;
import eu.bde.sc7pilot.tilebased.model.MyTile;
import eu.bde.sc7pilot.tilebased.model.MyTileIndex;

public class GCPSelection {
	private String name;
	private int cWindowWidth = 0;
	private int cWindowHeight = 0;
	private double gcpTolerance = 0.5;
	private int maxIteration = 10;
	private int cHalfWindowWidth;
	private int cHalfWindowHeight;
	private int rowUpSamplingFactor = 0;
	private int colUpSamplingFactor = 0;
	private final static double MaxInvalidPixelPercentage = 0.66;
	private boolean complexCoregistration = false;
	private MyTile masterTile;
	private MyTile slaveTile;
	private double masterNoDataValue;
	private double slaveNoDataValue;
	private GeoCoding tgtGeoCoding;
	private boolean onlyGCPsOnLand = false;
    private int sourceImageWidth;
    private int sourceImageHeight;
	private ElevationModel dem = null;

	public GCPSelection(int[] iParams,double[] dParams,GeoCoding tgtGeoCoding,MyTile masterTile,MyTile slaveTile) {
		this.masterTile=masterTile;
		this.slaveTile=slaveTile;
		this.cWindowWidth=iParams[0];
		this.cWindowHeight=iParams[1];
		this.maxIteration = iParams[2];
		this.rowUpSamplingFactor= iParams[3];
		this.colUpSamplingFactor= iParams[4];
		this.sourceImageWidth = iParams[5];
		this.sourceImageHeight = iParams[6];
		this.cHalfWindowWidth= this.cWindowWidth/2;
		this.cHalfWindowHeight= this.cWindowHeight/2;
		this.gcpTolerance = dParams[0];
		this.masterNoDataValue = dParams[1];
		this.slaveNoDataValue = dParams[2];
		this.tgtGeoCoding=tgtGeoCoding;
		if (onlyGCPsOnLand && dem == null) {
			createDEM();
		}
		
	}
	
	private synchronized void createDEM() {
		if (dem != null) {
			return;
		}
		final ElevationModelRegistry elevationModelRegistry = ElevationModelRegistry.getInstance();
		final ElevationModelDescriptor demDescriptor = elevationModelRegistry.getDescriptor("SRTM 3Sec");
		dem = demDescriptor.createDem(ResamplingFactory.createResampling(ResamplingFactory.NEAREST_NEIGHBOUR_NAME));
	}

	public Placemark computeSlaveGCP(Placemark mPin, PixelPos sGCPPixelPos) {
		final GeoPos mGCPGeoPos = mPin.getGeoPos();
		final PixelPos mGCPPixelPos = mPin.getPixelPos();
		Boolean getSlaveGCP = getCoarseSlaveGCPPosition(mGCPPixelPos, sGCPPixelPos);
		Placemark sPin = null;
		if (!getSlaveGCP) {
			sPin = null;
//			System.out.println("getSlaveGCP is FALSE");
		}		
		else {
			sPin = Placemark.createPointPlacemark(GcpDescriptor.getInstance(),
												mPin.getName(),
												mPin.getLabel(),
												mPin.getDescription(),
												sGCPPixelPos,
												mGCPGeoPos,
												tgtGeoCoding);
			if (!checkSlaveGCPValidity(sGCPPixelPos)) {
	            //System.out.println("GCP(" + i + ") is outside slave image.");
				sPin = null;
//				System.out.println("checkSlaveGCPValidity is FALSE");
	        }
			
		}
		return sPin;
//			System.out.println("gcp "+sPin.getPixelPos()+" out of bounds!!!");
	
	}

	private Boolean getCoarseSlaveGCPPosition(final PixelPos mGCPPixelPos, final PixelPos sGCPPixelPos) {

		try {
			final double[] mI = new double[cWindowWidth * cWindowHeight];
			final double[] sI = new double[cWindowWidth * cWindowHeight];

			final Boolean getMISuccess = getMasterImagette(mGCPPixelPos, mI);
			if(getMISuccess==null)
				return null;
			else if (!getMISuccess) {
				//System.out.println("getMISuccess is FALSE");
				return false;
			}
			// System.out.println("Master imagette:");
			// outputRealImage(mI);

			double rowShift = gcpTolerance + 1;
			double colShift = gcpTolerance + 1;
			int numIter = 0;

			while (Math.abs(rowShift) >= gcpTolerance || Math.abs(colShift) >= gcpTolerance) {
			//	System.out.println("slave gcp " + sGCPPixelPos.x + " " + sGCPPixelPos.y);
				if (numIter >= maxIteration) {
					return false;
				}
				 if (!checkSlaveGCPValidity(sGCPPixelPos)) {
	                    return false;
	                }
				final Boolean getSISuccess = getSlaveImagette(sGCPPixelPos, sI);
				if(getSISuccess==null)
					return null;
				else if (!getSISuccess) {
					return false;
				}
				// System.out.println("Slave imagette:");
				// outputRealImage(sI);

				final double[] shift = { 0, 0 };
				if (!getSlaveGCPShift(shift, mI, sI)) {
					return false;
				}

				rowShift = shift[0];
				colShift = shift[1];
				sGCPPixelPos.x += (float) colShift;
				sGCPPixelPos.y += (float) rowShift;
				numIter++;
			}

			return true;
		} catch (Throwable e) {
			// OperatorUtils.catchOperatorException(getId() + "
			// getCoarseSlaveGCPPosition ", e);
		}
		return false;
	}

	private Boolean getMasterImagette(final PixelPos gcpPixelPos, final double[] mI) throws OperatorException {

		final int x0 = (int) gcpPixelPos.x;
		final int y0 = (int) gcpPixelPos.y;
		final int xul = x0 - cHalfWindowWidth + 1;
		final int yul = y0 - cHalfWindowHeight + 1;
		final Rectangle masterImagetteRectangle = new Rectangle(xul, yul, cWindowWidth, cWindowHeight);
		if(!masterTile.getRectangle().contains(masterImagetteRectangle)) {
//			System.out.println(gcpPixelPos + " is out of bounds -- master");
			return false;
		}
		try {
			
			WritableRaster masterRaster = Utils.createWritableRaster(masterImagetteRectangle, masterTile.getType());
			Object dataBuffer = ImageUtils.createDataBufferArray(masterTile.getWritableRaster().getTransferType(),
															(int) masterImagetteRectangle.getWidth() * (int) masterImagetteRectangle.getHeight());
			masterTile.getWritableRaster().getDataElements(masterImagetteRectangle.x,
															masterImagetteRectangle.y,
															masterImagetteRectangle.width,
															masterImagetteRectangle.height,
															dataBuffer);
			masterRaster.setDataElements((int) masterImagetteRectangle.getMinX(),
										(int) masterImagetteRectangle.getMinY(),
										(int) masterImagetteRectangle.getWidth(),
										(int) masterImagetteRectangle.getHeight(),
										dataBuffer);
			final MyTile masterImagetteRaster1 = new MyTile(masterRaster, masterImagetteRectangle, masterTile.getType());
			final ProductData masterData1 = masterImagetteRaster1.getDataBuffer();

			ProductData masterData2 = null;
			double noDataValue2 = 0.0;

			final MyTileIndex mstIndex = new MyTileIndex(masterImagetteRaster1);

			int k = 0;
			//int valueTimes = 0;
			//System.out.println("masterNoDataValue is: " + "\n" + masterNoDataValue);
			int numInvalidPixels = 0;
			for (int j = 0; j < cWindowHeight; j++) {
				final int offset = mstIndex.calculateStride(yul + j);
				for (int i = 0; i < cWindowWidth; i++) {
					final int index = xul + i - offset;
					if (complexCoregistration) {	// To complexCoregistration exei tethei false, ara sth sygkekrimenh ylopoihsh pame sto else
						final double v1 = masterData1.getElemDoubleAt(index);
						final double v2 = masterData2.getElemDoubleAt(index);
						if (v1 == masterNoDataValue && v2 == noDataValue2) {
							numInvalidPixels++;
						}
						mI[k++] = v1 * v1 + v2 * v2;
					}
					else {
						final double v = masterData1.getElemDoubleAt(index);
						if (v == masterNoDataValue) {
							//valueTimes++;
							numInvalidPixels++;
						}
						mI[k++] = v;
					}
				}
			}

			masterData1.dispose();
			if (masterData2 != null) {
				masterData2.dispose();
			}

			if (numInvalidPixels > MaxInvalidPixelPercentage * cWindowHeight * cWindowWidth) {
				return false;
			}
			return true;

		} catch (Throwable e) {
			OperatorUtils.catchOperatorException("getMasterImagette", e);
		}
		return false;
	}

	private Boolean getSlaveImagette(final PixelPos gcpPixelPos, final double[] sI) throws OperatorException {

		final double xx = gcpPixelPos.x;
		final double yy = gcpPixelPos.y;
		final int xul = (int) xx - cHalfWindowWidth;
		final int yul = (int) yy - cHalfWindowHeight;
		 Rectangle slaveImagetteRectangle = new Rectangle(xul, yul, cWindowWidth + 3, cWindowHeight + 3);
		Rectangle slaveRect=slaveTile.getRectangle();
		Rectangle bounds=new Rectangle((int)slaveRect.getMinX(),(int)slaveRect.getMinY(),(int)slaveRect.getWidth(),(int)slaveRect.getHeight());
//		if(!(bounds).contains(slaveImagetteRectangle))
//		{
//			System.out.println(gcpPixelPos+ " is out of bounds -- slave");
//			return false;
//		}
		slaveImagetteRectangle=slaveImagetteRectangle.intersection(bounds);
		int k = 0;

		try {

			WritableRaster slaveRaster = Utils.createWritableRaster(slaveImagetteRectangle, slaveTile.getType());
			Object dataBuffer = ImageUtils.createDataBufferArray(slaveTile.getWritableRaster().getTransferType(),
					(int) slaveImagetteRectangle.getWidth() * (int) slaveImagetteRectangle.getHeight());
			slaveTile.getWritableRaster().getDataElements(slaveImagetteRectangle.x, slaveImagetteRectangle.y,
					slaveImagetteRectangle.width, slaveImagetteRectangle.height, dataBuffer);
			slaveRaster.setDataElements((int) slaveImagetteRectangle.getMinX(), (int) slaveImagetteRectangle.getMinY(),
					(int) slaveImagetteRectangle.getWidth(), (int) slaveImagetteRectangle.getHeight(), dataBuffer);
			final MyTile slaveImagetteRaster1 = new MyTile(slaveRaster, slaveImagetteRectangle, slaveTile.getType());
			final ProductData slaveData1 = slaveImagetteRaster1.getDataBuffer();

			Tile slaveImagetteRaster2 = null;
			ProductData slaveData2 = null;
			double noDataValue2 = 0.0;
			// if (complexCoregistration) {
			// slaveImagetteRaster2 = getSourceTile(slaveBand2,
			// slaveImagetteRectangle);
			// slaveData2 = slaveImagetteRaster2.getDataBuffer();
			// noDataValue2 = slaveBand2.getNoDataValue();
			// }

			final MyTileIndex index0 = new MyTileIndex(slaveImagetteRaster1);
			final MyTileIndex index1 = new MyTileIndex(slaveImagetteRaster1);

			int numInvalidPixels = 0;
			for (int j = 0; j < cWindowHeight; j++) {
				final double y = yy - cHalfWindowHeight + j + 1;
				final int y0 = (int) y;
				final int y1 = y0 + 1;
				final int offset0 = index0.calculateStride(y0);
				final int offset1 = index1.calculateStride(y1);
				final double wy = y - y0;
				for (int i = 0; i < cWindowWidth; i++) {
					final double x = xx - cHalfWindowWidth + i + 1;
					final int x0 = (int) x;
					final int x1 = x0 + 1;
					final double wx = x - x0;

					final int x00 = x0 - offset0;
					final int x01 = x0 - offset1;
					final int x10 = x1 - offset0;
					final int x11 = x1 - offset1;

					if (complexCoregistration) {

						final double v1 = MathUtils.interpolate2D(wy, wx, slaveData1.getElemDoubleAt(x00),
								slaveData1.getElemDoubleAt(x01), slaveData1.getElemDoubleAt(x10),
								slaveData1.getElemDoubleAt(x11));

						final double v2 = MathUtils.interpolate2D(wy, wx, slaveData2.getElemDoubleAt(x00),
								slaveData2.getElemDoubleAt(x01), slaveData2.getElemDoubleAt(x10),
								slaveData2.getElemDoubleAt(x11));

						if (v1 == slaveNoDataValue && v2 == noDataValue2) {
							numInvalidPixels++;
						}
						sI[k] = v1 * v1 + v2 * v2;
					} else {

						final double v = MathUtils.interpolate2D(wy, wx, slaveData1.getElemDoubleAt(x00),
								slaveData1.getElemDoubleAt(x01), slaveData1.getElemDoubleAt(x10),
								slaveData1.getElemDoubleAt(x11));

						if (v == slaveNoDataValue) {
							numInvalidPixels++;
						}
						sI[k] = v;
					}
					++k;
				}
			}
			slaveData1.dispose();
			if (slaveData2 != null) {
				slaveData2.dispose();
			}

			if (numInvalidPixels > MaxInvalidPixelPercentage * cWindowHeight * cWindowWidth) {
				return false;
			}
			return true;

		} catch (Throwable e) {
			OperatorUtils.catchOperatorException("getSlaveImagette", e);
		}
		return false;
	}

	private boolean getSlaveGCPShift(final double[] shift, final double[] mI, final double[] sI) {
		try {
			// perform cross correlation
			final PlanarImage crossCorrelatedImage = computeCrossCorrelatedImage(mI, sI);

			// check peak validity
			/*
			 * final double mean = getMean(crossCorrelatedImage); if
			 * (Double.compare(mean, 0.0) == 0) { return false; }
			 * 
			 * double max = getMax(crossCorrelatedImage); double qualityParam =
			 * max / mean; if (qualityParam <= qualityThreshold) { return false;
			 * }
			 */
			// get peak shift: row and col
			final int w = crossCorrelatedImage.getWidth();
			final int h = crossCorrelatedImage.getHeight();
			final Raster idftData = crossCorrelatedImage.getData();
			final double[] real = idftData.getSamples(0, 0, w, h, 0, (double[]) null);
			// System.out.println("Cross correlated imagette:");
			// outputRealImage(real);

			int peakRow = 0;
			int peakCol = 0;
			double peak = real[0];
			for (int r = 0; r < h; r++) {
				for (int c = 0; c < w; c++) {
					final int k = r * w + c;
					if (real[k] > peak) {
						peak = real[k];
						peakRow = r;
						peakCol = c;
					}
				}
			}
			// System.out.println("peak = " + peak + " at (" + peakRow + ", " +
			// peakCol + ")");

			if (peakRow <= h / 2) {
				shift[0] = (double) (-peakRow) / (double) rowUpSamplingFactor;
			} else {
				shift[0] = (double) (h - peakRow) / (double) rowUpSamplingFactor;
			}

			if (peakCol <= w / 2) {
				shift[1] = (double) (-peakCol) / (double) colUpSamplingFactor;
			} else {
				shift[1] = (double) (w - peakCol) / (double) colUpSamplingFactor;
			}

			return true;
		} catch (Throwable t) {
			System.out.println("getSlaveGCPShift failed " + t.getMessage());
			return false;
		}
	}

	private PlanarImage computeCrossCorrelatedImage(final double[] mI, final double[] sI) {

		// get master imagette spectrum
		final RenderedImage masterImage = createRenderedImage(mI, cWindowWidth, cWindowHeight);
		final PlanarImage masterSpectrum = dft(masterImage);
		// System.out.println("Master spectrum:");
		// outputComplexImage(masterSpectrum);

		// get slave imagette spectrum
		final RenderedImage slaveImage = createRenderedImage(sI, cWindowWidth, cWindowHeight);
		final PlanarImage slaveSpectrum = dft(slaveImage);
		// System.out.println("Slave spectrum:");
		// outputComplexImage(slaveSpectrum);

		// get conjugate slave spectrum
		final PlanarImage conjugateSlaveSpectrum = conjugate(slaveSpectrum);
		// System.out.println("Conjugate slave spectrum:");
		// outputComplexImage(conjugateSlaveSpectrum);

		// multiply master spectrum and conjugate slave spectrum
		final PlanarImage crossSpectrum = multiplyComplex(masterSpectrum, conjugateSlaveSpectrum);
		// System.out.println("Cross spectrum:");
		// outputComplexImage(crossSpectrum);

		// upsampling cross spectrum
		final RenderedImage upsampledCrossSpectrum = upsampling(crossSpectrum);

		// perform IDF on the cross spectrum
		final PlanarImage correlatedImage = idft(upsampledCrossSpectrum);
		// System.out.println("Correlated image:");
		// outputComplexImage(correlatedImage);

		// compute the magnitode of the cross correlated image
		return magnitude(correlatedImage);
	}

	private static RenderedImage createRenderedImage(final double[] array, final int w, final int h) {

		// create rendered image with demension being width by height
		final SampleModel sampleModel = RasterFactory.createBandedSampleModel(DataBuffer.TYPE_DOUBLE, w, h, 1);
		final ColorModel colourModel = PlanarImage.createColorModel(sampleModel);
		final DataBufferDouble dataBuffer = new DataBufferDouble(array, array.length);
		final WritableRaster raster = RasterFactory.createWritableRaster(sampleModel, dataBuffer, new Point(0, 0));

		return new BufferedImage(colourModel, raster, false, new Hashtable());
	}

	private static PlanarImage dft(final RenderedImage image) {

		final ParameterBlock pb = new ParameterBlock();
		pb.addSource(image);
		pb.add(DFTDescriptor.SCALING_NONE);
		pb.add(DFTDescriptor.REAL_TO_COMPLEX);
		return JAI.create("dft", pb, null);
	}

	private static PlanarImage idft(final RenderedImage image) {

		final ParameterBlock pb = new ParameterBlock();
		pb.addSource(image);
		pb.add(DFTDescriptor.SCALING_DIMENSIONS);
		pb.add(DFTDescriptor.COMPLEX_TO_COMPLEX);
		return JAI.create("idft", pb, null);
	}

	private static PlanarImage conjugate(final PlanarImage image) {

		final ParameterBlock pb = new ParameterBlock();
		pb.addSource(image);
		return JAI.create("conjugate", pb, null);
	}

	private static PlanarImage multiplyComplex(final PlanarImage image1, final PlanarImage image2) {

		final ParameterBlock pb = new ParameterBlock();
		pb.addSource(image1);
		pb.addSource(image2);
		return JAI.create("MultiplyComplex", pb, null);
	}

	private RenderedImage upsampling(final PlanarImage image) {

		// System.out.println("Source image:");
		// outputComplexImage(image);
		final int w = image.getWidth(); // w is power of 2
		final int h = image.getHeight(); // h is power of 2
		final int newWidth = rowUpSamplingFactor * w; // rowInterpFactor should
														// be power of 2 to
														// avoid zero padding in
														// idft
		final int newHeight = colUpSamplingFactor * h; // colInterpFactor should
														// be power of 2 to
														// avoid zero padding in
														// idft

		// create shifted image
		final ParameterBlock pb1 = new ParameterBlock();
		pb1.addSource(image);
		pb1.add(w / 2);
		pb1.add(h / 2);
		PlanarImage shiftedImage = JAI.create("PeriodicShift", pb1, null);
		// System.out.println("Shifted image:");
		// outputComplexImage(shiftedImage);

		// create zero padded image
		final ParameterBlock pb2 = new ParameterBlock();
		final int leftPad = (newWidth - w) / 2;
		final int rightPad = leftPad;
		final int topPad = (newHeight - h) / 2;
		final int bottomPad = topPad;
		pb2.addSource(shiftedImage);
		pb2.add(leftPad);
		pb2.add(rightPad);
		pb2.add(topPad);
		pb2.add(bottomPad);
		pb2.add(BorderExtender.createInstance(BorderExtender.BORDER_ZERO));
		final PlanarImage zeroPaddedImage = JAI.create("border", pb2);

		// reposition zero padded image so the image origin is back at (0,0)
		final ParameterBlock pb3 = new ParameterBlock();
		pb3.addSource(zeroPaddedImage);
		pb3.add(1.0f * leftPad);
		pb3.add(1.0f * topPad);
		final PlanarImage zeroBorderedImage = JAI.create("translate", pb3, null);
		// System.out.println("Zero padded image:");
		// outputComplexImage(zeroBorderedImage);

		// shift the zero padded image
		final ParameterBlock pb4 = new ParameterBlock();
		pb4.addSource(zeroBorderedImage);
		pb4.add(newWidth / 2);
		pb4.add(newHeight / 2);
		final PlanarImage shiftedZeroPaddedImage = JAI.create("PeriodicShift", pb4, null);
		// System.out.println("Shifted zero padded image:");
		// outputComplexImage(shiftedZeroPaddedImage);

		return shiftedZeroPaddedImage;
	}

	private static PlanarImage magnitude(final PlanarImage image) {

		final ParameterBlock pb = new ParameterBlock();
		pb.addSource(image);
		return JAI.create("magnitude", pb, null);
	}

	public boolean checkMasterGCPValidity(final Placemark mPin) throws Exception {
        final PixelPos pixelPos = mPin.getPixelPos();
        if (onlyGCPsOnLand) {
            double alt = dem.getElevation(mPin.getGeoPos());
            if (alt == dem.getDescriptor().getNoDataValue()) {
                return false;
            }
        }
        return (pixelPos.x - cHalfWindowWidth + 1 >= 0 && pixelPos.x + cHalfWindowWidth <= sourceImageWidth - 1)
                && (pixelPos.y - cHalfWindowHeight + 1 >= 0 && pixelPos.y + cHalfWindowHeight <= sourceImageHeight - 1);
    }
	public boolean checkSlaveGCPValidity(final PixelPos pixelPos) {

        return (pixelPos.x - cHalfWindowWidth + 1 >= 0 && pixelPos.x + cHalfWindowWidth <= sourceImageWidth - 1)
                && (pixelPos.y - cHalfWindowHeight + 1 >= 0 && pixelPos.y + cHalfWindowHeight <= sourceImageHeight - 1);
    }
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
}
