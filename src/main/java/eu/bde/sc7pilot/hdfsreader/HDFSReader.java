package eu.bde.sc7pilot.hdfsreader;

import java.io.IOException;

import javax.imageio.ImageReader;

import org.esa.snap.core.dataio.ProductSubsetDef;
import org.esa.snap.core.datamodel.ProductData;

import com.bc.ceres.core.ProgressMonitor;

public class HDFSReader {

    /**
     * The spectral and spatial subset definition used to read from the original
     * data source.
     */
    private ProductSubsetDef subsetDef;
    private ImageReader tiffImageReader;

    public HDFSReader(ImageReader reader) {
        tiffImageReader = reader;
    }

    public void readBandRasterData(BandInfo bandInfo, int destOffsetX,
            int destOffsetY,
            int destWidth,
            int destHeight,
            ProductData destBuffer, ProgressMonitor pm) throws IOException {

        if (destBuffer.getNumElems() < destWidth * destHeight) {
            throw new IllegalArgumentException("destination buffer too small");
        }
        if (destBuffer.getNumElems() > destWidth * destHeight) {
            throw new IllegalArgumentException("destination buffer too big");
        }

        int sourceOffsetX = 0;
        int sourceOffsetY = 0;
        int sourceStepX = 1;
        int sourceStepY = 1;
        //*** SOS subsetDef from WHERE???
        if (getSubsetDef() != null) {
            sourceStepX = getSubsetDef().getSubSamplingX();
            sourceStepY = getSubsetDef().getSubSamplingY();
            if (getSubsetDef().getRegion() != null) {
                sourceOffsetX = getSubsetDef().getRegion().x;
                sourceOffsetY = getSubsetDef().getRegion().y;
            }
        }
        //~~~ SOS subsetDef from WHERE???
        sourceOffsetX += sourceStepX * destOffsetX;
        sourceOffsetY += sourceStepY * destOffsetY;
        int sourceWidth = sourceStepX * (destWidth - 1) + 1;
        int sourceHeight = sourceStepY * (destHeight - 1) + 1;

        //*** Implementation of readBandRasterDataImpl()
//        bandInfo.img.readImageIORasterBand(sourceOffsetX, sourceOffsetY, sourceStepX, sourceStepY,
//                                                   destBuffer, destOffsetX, destOffsetY, destWidth, destHeight,
//                                                   bandInfo.imageID, bandInfo.bandSampleOffset);
        ImageHDFSFile imgHDFS = new ImageHDFSFile();
        imgHDFS.setImageReader(tiffImageReader);
        //Suppose that:

        //SOS imageID & bandSampleOffset are taken from BandInfo bandinfo!!! Einai int(s) kai xrhsimopoiountai se arketa shmeia sthn readImageIORasterBand
        imgHDFS.readImageIORasterBand(sourceOffsetX, sourceOffsetY, sourceStepX, sourceStepY, destBuffer, destOffsetX, destOffsetY, destWidth, destHeight, bandInfo.imageID, bandInfo.bandSampleOffset);
        //~~~ Implementation of readBandRasterDataImpl()
    }

    /**
     * Returns the subset information with which this data product is read from
     * its physical source.
     *
     * @return the subset information, can be <code>null</code>
     */
    public ProductSubsetDef getSubsetDef() {
        return subsetDef;
    }

}
