package eu.bde.sc7pilot.tilebased.model;

public final class MyTileIndex {

    private final int tileOffset;
    private final int tileStride;
    private final int tileMinX;
    private final int tileMinY;

    private int offset = 0;

    public MyTileIndex(final MyTile tile) {
        tileOffset = tile.getScanlineOffset();
        tileStride = tile.getScanlineStride();
        tileMinX = tile.getMinX();
        tileMinY = tile.getMinY();
    }

    /**
     * calculates offset
     *
     * @param ty y pos
     * @return offset
     */
    public int calculateStride(final int ty) {
        offset = tileMinX - (((ty - tileMinY) * tileStride) + tileOffset);
        return offset;
    }

    public int getOffset() {
        return offset;
    }

    public int getIndex(final int tx) {
        return tx - offset;
    }
}
