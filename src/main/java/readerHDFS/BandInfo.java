package readerHDFS;

public class BandInfo {
    public int imageID;
    public int bandSampleOffset;
	private String hdfsPath;

    public BandInfo(String hdfsPath, int id, int offset) {
    	this.hdfsPath = hdfsPath;
        this.imageID = id;
        this.bandSampleOffset = offset;
    }

	public int getImageID() {
		return imageID;
	}

	public int getBandSampleOffset() {
		return bandSampleOffset;
	}

	public String getHdfsPath() {
		return hdfsPath;
	}

	public void setImageID(int imageID) {
		this.imageID = imageID;
	}

	public void setBandSampleOffset(int bandSampleOffset) {
		this.bandSampleOffset = bandSampleOffset;
	}

	public void setHdfsPath(String hdfsPath) {
		this.hdfsPath = hdfsPath;
	}
}
