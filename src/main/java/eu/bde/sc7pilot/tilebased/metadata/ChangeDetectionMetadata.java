package eu.bde.sc7pilot.tilebased.metadata;

public class ChangeDetectionMetadata {

    private boolean outputLogRatio = false;
    private double noDataValueN;
    private double noDataValueD;

    public ChangeDetectionMetadata(boolean[] bParams, double[] dParams) {
        this.outputLogRatio = bParams[0];
        this.noDataValueD = dParams[1];
        this.noDataValueN = dParams[0];
    }

    public boolean isOutputLogRatio() {
        return outputLogRatio;
    }

    public void setOutputLogRatio(boolean outputLogRatio) {
        this.outputLogRatio = outputLogRatio;
    }

    public double getNoDataValueN() {
        return noDataValueN;
    }

    public void setNoDataValueN(double noDataValueN) {
        this.noDataValueN = noDataValueN;
    }

    public double getNoDataValueD() {
        return noDataValueD;
    }

    public void setNoDataValueD(double noDataValueD) {
        this.noDataValueD = noDataValueD;
    }

}
