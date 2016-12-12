package tileBased.operator;

import java.awt.Rectangle;

import org.esa.snap.core.datamodel.ProductData;

import tileBased.model.MyTile;
import tileBased.model.MyTileIndex;

public class CreateStack {
	private String name;
public void computeTile(MyTile targetTile,MyTile sourceTile,int[] offset,int srcImageWidth,int srcImageHeight,float noDataValue) throws Exception{
	try {
		final Rectangle targetRectangle = targetTile.getRectangle();
		final ProductData trgData = targetTile.getDataBuffer();
		final int tx0 = targetRectangle.x;
		final int ty0 = targetRectangle.y;
		final int tw = targetRectangle.width;
		final int th = targetRectangle.height;
		final int maxX = tx0 + tw;
		final int maxY = ty0 + th;

		final MyTile srcTile = sourceTile;
		if (srcTile == null) {
		}
		final ProductData srcData = srcTile.getDataBuffer();

		final MyTileIndex trgIndex = new MyTileIndex(targetTile);
		final MyTileIndex srcIndex = new MyTileIndex(srcTile);

		boolean isInt = false;
		final int trgDataType = trgData.getType();
		if (trgDataType == srcData.getType()
				&& (trgDataType == ProductData.TYPE_INT16 || trgDataType == ProductData.TYPE_INT32)) {
			isInt = true;
		}

		for (int ty = ty0; ty < maxY; ++ty) {
			final int sy = ty + offset[1];
			final int trgOffset = trgIndex.calculateStride(ty);
			if (sy < 0 || sy >= srcImageHeight) {
				for (int tx = tx0; tx < maxX; ++tx) {
					trgData.setElemDoubleAt(tx - trgOffset, noDataValue);
				}
				continue;
			}
			final int srcOffset = srcIndex.calculateStride(sy);
			for (int tx = tx0; tx < maxX; ++tx) {
				final int sx = tx + offset[0];

				if (sx < 0 || sx >= srcImageWidth) {
					trgData.setElemDoubleAt(tx - trgOffset, noDataValue);
				} else {
					if (isInt) {
						trgData.setElemIntAt(tx - trgOffset, srcData.getElemIntAt(sx - srcOffset));
					} else {
						trgData.setElemDoubleAt(tx - trgOffset, srcData.getElemDoubleAt(sx - srcOffset));
					}
				}
			}

		}
		
	} catch (Throwable e) {
		throw e;
}
}
public String getName() {
	return name;
}
public void setName(String name) {
	this.name = name;
}
}
