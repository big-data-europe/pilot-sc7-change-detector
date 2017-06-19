package eu.bde.sc7pilot.hdfsreader;

import java.awt.Rectangle;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.esa.snap.core.datamodel.ProductData;

import eu.bde.sc7pilot.tilebased.model.MyTile;

public class ReadHDFSTile {

    private HDFSReader reader;

    public ReadHDFSTile(String path) {
        Path fileHDFSPath = new Path(path);
        Configuration conf = new Configuration();

        FSDataInputStream instream;
        ImageInputStream imgInStream = null;
        try {
            FileSystem fs = FileSystem.get(URI.create(path), conf);
            instream = fs.open(fileHDFSPath);
            imgInStream = ImageIO.createImageInputStream(instream);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Iterator<ImageReader> imgIt = ImageIO.getImageReaders(imgInStream);
        ImageReader imgReader = imgIt.next();
        imgReader.setInput(imgInStream);

        reader = new HDFSReader(imgReader);
    }

    public void readTile(MyTile tile, BandInfo bandInfo) {

        ProductData dataBuffer = tile.getRawSamples();
        Rectangle rectangle = tile.getRectangle();
        try {
            reader.readBandRasterData(bandInfo, rectangle.x, rectangle.y, rectangle.width, rectangle.height, dataBuffer,null);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        tile.setRawSamples(dataBuffer);
    }
}
