package eu.bde.sc7pilot.tilebased.operator;

import java.awt.Rectangle;
import org.esa.snap.core.datamodel.ProductData;
import org.jlinda.core.Constants;
import eu.bde.sc7pilot.tilebased.model.MyTile;
import eu.bde.sc7pilot.tilebased.model.MyTileIndex;

public class ChangeDetection {

    private boolean outputLogRatio = false;
    private double noDataValueN;
    private double noDataValueD;
    private String name;

    public ChangeDetection(boolean outputLogRatio, double noDataValueN, double noDataValueD) {
        this.outputLogRatio = outputLogRatio;
        this.noDataValueD = noDataValueD;
        this.noDataValueN = noDataValueN;
    }

    public void computeTile(MyTile nominatorTile, MyTile denominatorTile, MyTile targetRatioTile, Rectangle targetRectangle) {

        final int tx0 = targetRectangle.x;
        final int ty0 = targetRectangle.y;
        final int tw = targetRectangle.width;
        final int th = targetRectangle.height;
        //System.out.println("tx0 = " + tx0 + ", ty0 = " + ty0 + ", tw = " + tw + ", th = " + th);

        final ProductData nominatorData = nominatorTile.getDataBuffer();
        final ProductData denominatorData = denominatorTile.getDataBuffer();

        final ProductData ratioData = targetRatioTile.getDataBuffer();

        final MyTileIndex trgIndex = new MyTileIndex(targetRatioTile);
        final MyTileIndex srcIndex = new MyTileIndex(nominatorTile);

        final int maxy = ty0 + th;
        final int maxx = tx0 + tw;

        double vRatio;
        for (int ty = ty0; ty < maxy; ty++) {
            trgIndex.calculateStride(ty);
            srcIndex.calculateStride(ty);
            for (int tx = tx0; tx < maxx; tx++) {
                final int trgIdx = trgIndex.getIndex(tx);
                final int srcIdx = srcIndex.getIndex(tx);

                final double vN = nominatorData.getElemDoubleAt(srcIdx);
                final double vD = denominatorData.getElemDoubleAt(srcIdx);
                if (vN == noDataValueN || vD == noDataValueD || vN <= 0.0 || vD <= 0.0) {
                    ratioData.setElemFloatAt(trgIdx, 0.0f);
                    continue;
                }

                vRatio = vN / vD;
                if (outputLogRatio) {
                    vRatio = Math.log(Math.max(vRatio, Constants.EPS));
                }
//                 if()
//                	 System.out.println(vRatio);
                ratioData.setElemFloatAt(trgIdx, (float) vRatio);
            }
        }

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
