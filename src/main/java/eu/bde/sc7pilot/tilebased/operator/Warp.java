package eu.bde.sc7pilot.tilebased.operator;

import java.awt.Rectangle;
import java.awt.image.DataBuffer;
import java.awt.image.RenderedImage;
import java.awt.image.renderable.ParameterBlock;

import javax.media.jai.Interpolation;
import javax.media.jai.InterpolationTable;
import javax.media.jai.JAI;
import javax.media.jai.RenderedOp;
import javax.media.jai.WarpPolynomial;

import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.OperatorException;

import eu.bde.sc7pilot.taskbased.MyWarp.WarpData;
import eu.bde.sc7pilot.tilebased.model.MyTile;
public class Warp {
	private String name;
	private Interpolation interp = null;
	private WarpData warpData;
	private InterpolationTable interpTable = null;
	
	public Warp(Interpolation interp,WarpData warpData,InterpolationTable interpTable) {
		this.warpData=warpData;
		this.interp=interp;
		this.interpTable=interpTable;
	}
	public void computeTile(MyTile targetTile,RenderedImage warpedImage) throws OperatorException {
        final Rectangle targetRectangle = targetTile.getRectangle();
        final int x0 = targetRectangle.x;
        final int y0 = targetRectangle.y;
        final int w = targetRectangle.width;
        final int h = targetRectangle.height;
        try {
            // create source image
           // final Tile sourceRaster = getSourceTile(srcBand, targetRectangle);

            if (warpData.notEnoughGCPs) {
                return;
            }

           // final RenderedImage srcImage = sourceRaster.getRasterDataNode().getSourceImage();
            // get warped image
            
            
//            WarpOpImage wimg=(WarpOpImage)warpedImage.getRendering();
//            wimg.mapDestRect(destRect, sourceIndex)
            // copy warped image data to target
            final float[] dataArray = warpedImage.getData(targetRectangle).getSamples(x0, y0, w, h, 0, (float[]) null);

            targetTile.setRawSamples(ProductData.createInstance(dataArray));

        } catch (Throwable e) {
            e.printStackTrace();
        } 
    }
	 public RenderedOp createWarpImage(WarpPolynomial warp, final RenderedImage srcImage) {

	        // reformat source image by casting pixel values from ushort to float
	        final ParameterBlock pb1 = new ParameterBlock();
	        pb1.addSource(srcImage);
	        pb1.add(DataBuffer.TYPE_FLOAT);
	        final RenderedImage srcImageFloat = JAI.create("format", pb1);

	        // get warped image
	        final ParameterBlock pb2 = new ParameterBlock();
	        pb2.addSource(srcImageFloat);
	        pb2.add(warp);

	        if (interp != null) {
	            pb2.add(interp);
	        } else if (interpTable != null) {
	            pb2.add(interpTable);
	        }
	        return JAI.create("warp", pb2);
	    }
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public WarpData getWarpData() {
		return warpData;
	}
	public void setWarpData(WarpData warpData) {
		this.warpData = warpData;
	}
}
