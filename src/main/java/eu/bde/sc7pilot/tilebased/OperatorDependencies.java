package eu.bde.sc7pilot.tilebased;

import java.awt.Rectangle;

public class OperatorDependencies {

    public static Rectangle createStack(Rectangle targetRectangle, int[] offset, int srcImageWidth, int srcImageHeight) {
//		System.out.println(offset);
//		System.out.println(srcImageWidth);
//		System.out.println(srcImageHeight);
        final int tx0 = targetRectangle.x;
        final int ty0 = targetRectangle.y;
        final int tw = targetRectangle.width;
        final int th = targetRectangle.height;

        final int sx0 = Math.min(Math.max(0, tx0 + offset[0]), srcImageWidth - 1);
        final int sy0 = Math.min(Math.max(0, ty0 + offset[1]), srcImageHeight - 1);
        final int sw = Math.min(sx0 + tw - 1, srcImageWidth - 1) - sx0 + 1;
        final int sh = Math.min(sy0 + th - 1, srcImageHeight - 1) - sy0 + 1;
        final Rectangle srcRectangle = new Rectangle(sx0, sy0, sw, sh);
        return srcRectangle;
    }

    public static Rectangle gcpSelection(Rectangle targetRectangle, int[] offset, int srcImageWidth, int srcImageHeight) {

        final int tx0 = targetRectangle.x;
        final int ty0 = targetRectangle.y;
        final int tw = targetRectangle.width;
        final int th = targetRectangle.height;

        final int sx0 = Math.min(3 * tx0, srcImageWidth - 1);
        final int sy0 = Math.min(3 * ty0, srcImageHeight - 1);
        final int sw = Math.min(sx0 + tw - 1, srcImageWidth - 1) - sx0 + 1;
        final int sh = Math.min(sy0 + th - 1, srcImageHeight - 1) - sy0 + 1;
        final Rectangle srcRectangle = new Rectangle(sx0, sy0, sw, sh);
        return srcRectangle;
    }
}
