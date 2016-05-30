package eu.bde.sc7pilot.tilebased.metadata;

public class GCPMetadata {

    private int numGCPtoGenerate = 200;

    private String coarseRegistrationWindowWidth = "128";

    private String coarseRegistrationWindowHeight = "128";
    private String rowInterpFactor = "2";

    private String columnInterpFactor = "2";
    private int maxIteration = 10;
    private double gcpTolerance = 0.5;

    // ==================== input parameters used for complex co-registration ==================
    private boolean applyFineRegistration = true;

    private String fineRegistrationWindowWidth = "32";
    private String fineRegistrationWindowHeight = "32";
    private int coherenceWindowSize = 3;
    private double coherenceThreshold = 0.6;
    private Boolean useSlidingWindow = false;

    //    @Parameter(description = "The coherence function tolerance", interval = "(0, *)", defaultValue = "1.e-6",
//	                label="Coherence Function Tolerance")
    private static final double coherenceFuncToler = 1.e-5;
    //    @Parameter(description = "The coherence value tolerance", interval = "(0, *)", defaultValue = "1.e-3",
//	                label="Coherence Value Tolerance")
    private static final double coherenceValueToler = 1.e-2;
    // =========================================================================================
    private boolean computeOffset = false;
    private boolean onlyGCPsOnLand = false;

    public GCPMetadata(boolean[] bParams, double[] dParams, int[] iParams, String[] sParams) {
        this.numGCPtoGenerate = iParams[0];
        this.coarseRegistrationWindowWidth = sParams[0];
        this.coarseRegistrationWindowHeight = sParams[1];
        this.rowInterpFactor = sParams[2];
        this.columnInterpFactor = sParams[3];
        this.fineRegistrationWindowWidth = sParams[4];
        this.fineRegistrationWindowHeight = sParams[5];
        this.maxIteration = iParams[1];
        this.gcpTolerance = dParams[0];
        this.applyFineRegistration = bParams[0];
        this.coherenceWindowSize = iParams[2];
        this.coherenceThreshold = dParams[1];
        this.useSlidingWindow = bParams[1];
        this.computeOffset = bParams[2];
        this.onlyGCPsOnLand = bParams[3];
    }

    public int getNumGCPtoGenerate() {
        return numGCPtoGenerate;
    }

    public void setNumGCPtoGenerate(int numGCPtoGenerate) {
        this.numGCPtoGenerate = numGCPtoGenerate;
    }

    public String getCoarseRegistrationWindowWidth() {
        return coarseRegistrationWindowWidth;
    }

    public void setCoarseRegistrationWindowWidth(String coarseRegistrationWindowWidth) {
        this.coarseRegistrationWindowWidth = coarseRegistrationWindowWidth;
    }

    public String getCoarseRegistrationWindowHeight() {
        return coarseRegistrationWindowHeight;
    }

    public void setCoarseRegistrationWindowHeight(String coarseRegistrationWindowHeight) {
        this.coarseRegistrationWindowHeight = coarseRegistrationWindowHeight;
    }

    public String getRowInterpFactor() {
        return rowInterpFactor;
    }

    public void setRowInterpFactor(String rowInterpFactor) {
        this.rowInterpFactor = rowInterpFactor;
    }

    public String getColumnInterpFactor() {
        return columnInterpFactor;
    }

    public void setColumnInterpFactor(String columnInterpFactor) {
        this.columnInterpFactor = columnInterpFactor;
    }

    public int getMaxIteration() {
        return maxIteration;
    }

    public void setMaxIteration(int maxIteration) {
        this.maxIteration = maxIteration;
    }

    public double getGcpTolerance() {
        return gcpTolerance;
    }

    public void setGcpTolerance(double gcpTolerance) {
        this.gcpTolerance = gcpTolerance;
    }

    public boolean isApplyFineRegistration() {
        return applyFineRegistration;
    }

    public void setApplyFineRegistration(boolean applyFineRegistration) {
        this.applyFineRegistration = applyFineRegistration;
    }

    public String getFineRegistrationWindowWidth() {
        return fineRegistrationWindowWidth;
    }

    public void setFineRegistrationWindowWidth(String fineRegistrationWindowWidth) {
        this.fineRegistrationWindowWidth = fineRegistrationWindowWidth;
    }

    public String getFineRegistrationWindowHeight() {
        return fineRegistrationWindowHeight;
    }

    public void setFineRegistrationWindowHeight(String fineRegistrationWindowHeight) {
        this.fineRegistrationWindowHeight = fineRegistrationWindowHeight;
    }

    public int getCoherenceWindowSize() {
        return coherenceWindowSize;
    }

    public void setCoherenceWindowSize(int coherenceWindowSize) {
        this.coherenceWindowSize = coherenceWindowSize;
    }

    public double getCoherenceThreshold() {
        return coherenceThreshold;
    }

    public void setCoherenceThreshold(double coherenceThreshold) {
        this.coherenceThreshold = coherenceThreshold;
    }

    public Boolean getUseSlidingWindow() {
        return useSlidingWindow;
    }

    public void setUseSlidingWindow(Boolean useSlidingWindow) {
        this.useSlidingWindow = useSlidingWindow;
    }

    public boolean isComputeOffset() {
        return computeOffset;
    }

    public void setComputeOffset(boolean computeOffset) {
        this.computeOffset = computeOffset;
    }

    public boolean isOnlyGCPsOnLand() {
        return onlyGCPsOnLand;
    }

    public void setOnlyGCPsOnLand(boolean onlyGCPsOnLand) {
        this.onlyGCPsOnLand = onlyGCPsOnLand;
    }

    public static double getCoherencefunctoler() {
        return coherenceFuncToler;
    }

    public static double getCoherencevaluetoler() {
        return coherenceValueToler;
    }

}
