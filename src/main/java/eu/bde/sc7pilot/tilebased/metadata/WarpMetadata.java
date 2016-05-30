package eu.bde.sc7pilot.tilebased.metadata;

import javax.media.jai.Interpolation;
import javax.media.jai.InterpolationTable;

public class WarpMetadata {

    private Interpolation interp = null;
    private InterpolationTable interpTable = null;

    public WarpMetadata(Interpolation interp, InterpolationTable interpTable) {
        this.interp = interp;
        this.interpTable = interpTable;
    }

    public Interpolation getInterp() {
        return interp;
    }

    public void setInterp(Interpolation interp) {
        this.interp = interp;
    }

    public InterpolationTable getInterpTable() {
        return interpTable;
    }

    public void setInterpTable(InterpolationTable interpTable) {
        this.interpTable = interpTable;
    }
}
