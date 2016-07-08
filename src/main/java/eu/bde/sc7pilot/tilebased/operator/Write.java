package eu.bde.sc7pilot.tilebased.operator;

import java.awt.Dimension;
import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.dataio.dimap.DimapProductWriter;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.util.jai.JAIUtils;
import org.esa.snap.core.util.math.MathUtils;

import com.bc.ceres.core.ProgressMonitor;

import eu.bde.sc7pilot.tilebased.model.MyTile;


public class Write {
	private Product sourceProduct;
	private Product targetProduct;
    private File file;
    private boolean requiresAllBands;
    private String formatName;
    private boolean deleteOutputOnFailure = true;
    private boolean writeEntireTileRows=true;

    private boolean[][][] tilesWritten;
    private final Map<Row, MyTile[]> writeCache = new HashMap<>();
    private Dimension[] tileSizes;
    private int[] tileCountsX;

    private ProductWriter productWriter;
    private List<Band> writableBands;

    private boolean outputFileExists = false;
    private boolean incremental = false;

    public Write(Product sourceProduct, File file, String formatName) {
        setRequiresAllBands(true);
        this.sourceProduct = sourceProduct;
        this.file = file;
        this.formatName = formatName;
        targetProduct = sourceProduct;
        outputFileExists = targetProduct.getFileLocation() != null && targetProduct.getFileLocation().exists();
        productWriter = ProductIO.getProductWriter(formatName);
        if (productWriter == null) {
            throw new OperatorException("No data product writer for the '" + formatName + "' format available");
        }
        productWriter.setIncrementalMode(incremental);
        targetProduct.setProductWriter(productWriter);

        final Band[] bands = targetProduct.getBands();
        writableBands = new ArrayList<>(bands.length);
        for (final Band band : bands) {
            band.getSourceImage(); // trigger source image creation
            if (productWriter.shouldWrite(band)) {
                writableBands.add(band);
            }
        }
        tileSizes = new Dimension[writableBands.size()];
        tileCountsX = new int[writableBands.size()];
        tilesWritten = new boolean[writableBands.size()][][];
        for (int i = 0; i < writableBands.size(); i++) {
            Band writableBand = writableBands.get(i);
            Dimension tileSize = determineTileSize(writableBand);
            tileSizes[i] = tileSize;
            int tileCountX = MathUtils.ceilInt(writableBand.getRasterWidth() / (double) tileSize.width);
            tileCountsX[i] = tileCountX;
            int tileCountY = MathUtils.ceilInt(writableBand.getRasterHeight() / (double) tileSize.height);
            tilesWritten[i] = new boolean[tileCountY][tileCountX];

            if (writeEntireTileRows && i > 0 && !tileSize.equals(tileSizes[0])) {
                writeEntireTileRows = false;        // don't writeEntireTileRows for multisize bands
            }
        }
        //(efi) the next line comes from the old doExecute()
        try {
            productWriter.writeProductNodes(targetProduct, file);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public String getFormatName() {
        return formatName;
    }

    public void setIncremental(boolean incremental) {
        this.incremental = incremental;
    }

    public void setFormatName(String formatName) {
        this.formatName = formatName;
    }

    public boolean isDeleteOutputOnFailure() {
        return deleteOutputOnFailure;
    }

    public void setDeleteOutputOnFailure(boolean deleteOutputOnFailure) {
        this.deleteOutputOnFailure = deleteOutputOnFailure;
    }

    public boolean isWriteEntireTileRows() {
        return writeEntireTileRows;
    }

    public void setWriteEntireTileRows(boolean writeEntireTileRows) {
        this.writeEntireTileRows = writeEntireTileRows;
    }

    private Dimension determineTileSize(Band band) {
        Dimension tileSize = null;
        if (band.getRasterWidth() == targetProduct.getSceneRasterWidth()
                && band.getRasterHeight() == targetProduct.getSceneRasterHeight()) {
            tileSize = targetProduct.getPreferredTileSize();
        }
        if (tileSize == null) {
            tileSize = JAIUtils.computePreferredTileSize(band.getRasterWidth(),
                    band.getRasterHeight(), 1);
        }
        return tileSize;
    }

    public void storeTile(Band targetBand, MyTile targetTile) throws OperatorException {
        int bandIndex = writableBands.indexOf(targetBand);
        if (bandIndex == -1) {
            return;
        }

        try {
            final Rectangle rect = targetTile.getRectangle();
            final Dimension tileSize = tileSizes[bandIndex];
            int tileCountX = tileCountsX[bandIndex];
            int tileX = MathUtils.floorInt(targetTile.getMinX() / (double) tileSize.width);
            int tileY = MathUtils.floorInt(targetTile.getMinY() / (double) tileSize.height);
            if (writeEntireTileRows) {
                Row row = new Row(targetBand, tileY);
                MyTile[] tileRowToWrite = updateTileRow(row, tileX, targetTile, tileCountX);
                if (tileRowToWrite != null) {
                    writeTileRow(targetBand, tileRowToWrite);
                }
                markTileAsHandled(targetBand, tileX, tileY);
            } else {
                final ProductData rawSamples = targetTile.getRawSamples();
                synchronized (productWriter) {
                    productWriter.writeBandRasterData(targetBand, rect.x, rect.y, rect.width, rect.height, rawSamples, ProgressMonitor.NULL);
                }
                markTileAsHandled(targetBand, tileX, tileY);
            }
            if (productWriter instanceof DimapProductWriter && isProductWrittenCompletely()) {
                // If we get here all tiles are written
                // we can update the header only for DIMAP, so rewrite it, to handle intermediate changes
                synchronized (productWriter) {
                    productWriter.writeProductNodes(targetProduct, file);
                }
            }
        } catch (Exception e) {
            if (deleteOutputOnFailure && !outputFileExists) {
                try {
                    productWriter.deleteOutput();
                } catch (IOException ignored) {
                }
            }
            if (e instanceof OperatorException) {
                throw (OperatorException) e;
            } else {
                throw new OperatorException("Not able to write product file: '" + file.getAbsolutePath() + "'", e);
            }
        }
    }

    private MyTile[] updateTileRow(Row key, int tileX, MyTile currentTile, int tileCountX) {
        synchronized (writeCache) {
        	MyTile[] tileRow;
            if (writeCache.containsKey(key)) {
                tileRow = writeCache.get(key);
            } else {
                tileRow = new MyTile[tileCountX];
                writeCache.put(key, tileRow);
            }
            tileRow[tileX] = currentTile;
            for (MyTile tile : tileRow) {
                if (tile == null) {
                    return null;
                }
            }
            writeCache.remove(key);
            return tileRow;
        }
    }

    private void writeTileRow(Band band, MyTile[] cacheLine) throws IOException {
    	MyTile firstTile = cacheLine[0];
        int sceneWidth = band.getRasterWidth();
        Rectangle lineBounds = new Rectangle(0, firstTile.getMinY(), sceneWidth, firstTile.getHeight());
        ProductData[] rawSampleOFLine = new ProductData[cacheLine.length];
        int[] tileWidth = new int[cacheLine.length];
        for (int tileX = 0; tileX < cacheLine.length; tileX++) {
        	MyTile tile = cacheLine[tileX];
            rawSampleOFLine[tileX] = tile.getRawSamples();
            tileWidth[tileX] = tile.getRectangle().width;
        }
        ProductData sampleLine = ProductData.createInstance(rawSampleOFLine[0].getType(), sceneWidth);
        synchronized (productWriter) {
            for (int y = lineBounds.y; y < lineBounds.y + lineBounds.height; y++) {
                int targetPos = 0;
                for (int tileX = 0; tileX < cacheLine.length; tileX++) {
                    Object rawSamples = rawSampleOFLine[tileX].getElems();
                    int width = tileWidth[tileX];
                    int srcPos = (y - lineBounds.y) * width;
                    System.arraycopy(rawSamples, srcPos, sampleLine.getElems(), targetPos, width);
                    targetPos += width;
                }

                productWriter.writeBandRasterData(band, 0, y, sceneWidth, 1, sampleLine, ProgressMonitor.NULL);
            }
        }
    }

    private void markTileAsHandled(Band targetBand, int tileX, int tileY) {
        int bandIndex = writableBands.indexOf(targetBand);
        tilesWritten[bandIndex][tileY][tileX] = true;
    }

    private boolean isProductWrittenCompletely() {
        for (int bandIndex = 0; bandIndex < writableBands.size(); bandIndex++) {
            for (boolean[] aXTileWritten : tilesWritten[bandIndex]) {
                for (boolean aYTileWritten : aXTileWritten) {
                    if (!aYTileWritten) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private static class Row {

        private final Band band;
        private final int tileY;

        private Row(Band band, int tileY) {
            this.band = band;
            this.tileY = tileY;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + band.hashCode();
            result = prime * result + tileY;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            Row other = (Row) obj;
            if (!band.equals(other.band)) {
                return false;
            }
            return tileY == other.tileY;
        }
    }

	public boolean isRequiresAllBands() {
		return requiresAllBands;
	}

	public void setRequiresAllBands(boolean requiresAllBands) {
		this.requiresAllBands = requiresAllBands;
	}
}
