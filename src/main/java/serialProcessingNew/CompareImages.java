/*

 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package serialProcessingNew;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.Tile;

public class CompareImages {

	public static void main(String[] args) throws IOException {
		
//		String filesPath1 = "/home/gvastakis/Desktop/sentinel-images-subsets/";
//		File file1 = new File(filesPath1,
//				"snapsubset.dim");
//
//		String filesPath2 = "/home/gvastakis/Desktop/sentinel-images-subsets/";
//		File file2 = new File(filesPath2, "subset.dim");
		
//		String filesPath1 = "/home/gvastakis/Desktop/new-images/4/";
//		File file1 = new File(filesPath1,
//				"snapsubset.dim");
//
//		String filesPath2 = "/home/gvastakis/Desktop/new-images/4/";
//		File file2 = new File(filesPath2, "subset.dim");
		
//		String filesPath1 = "/home/gvastakis/Desktop/new-images/4/";
//		File file1 = new File(filesPath1,
//				"snapsubset2.dim");
//
//		String filesPath2 = "/home/gvastakis/Desktop/new-images/4/";
//		File file2 = new File(filesPath2, "subset2.dim");
		
//		String filesPath1 = "/home/gvastakis/Desktop/new-images/4/";
//		File file1 = new File(filesPath1,
//				"snapsubset3.dim");
//
//		String filesPath2 = "/home/gvastakis/Desktop/new-images/4/";
//		File file2 = new File(filesPath2, "subset3.dim");
		
//		String filesPath1 = "/home/gvastakis/Desktop/new-images/4/";
//		File file1 = new File(filesPath1,
//				"snapsubset4.dim");
//
//		String filesPath2 = "/home/gvastakis/Desktop/new-images/4/";
//		File file2 = new File(filesPath2, "subset4.dim");
		
//		String filesPath1 = "/home/gvastakis/Desktop/new-images/5/";
//		File file1 = new File(filesPath1,
//				"snapsubset.dim");
//
//		String filesPath2 = "/home/gvastakis/Desktop/new-images/5/";
//		File file2 = new File(filesPath2, "subset.dim");
		
		String filesPath1 = "/home/ethanos/Desktop/BDEimages/VHold/";
		File file1 = new File(filesPath1,
				"changeD-tile-based-tiledImageWORKING3fromSubsets.dim");

		String filesPath2 = "/home/ethanos/Desktop/BDEimages/VHold/";
		File file2 = new File(filesPath2, "subset_of_changeD-tile-based-tiledImageWORKING3.dim");
		
//		String filesPath3 = "/home/gvastakis/Desktop/new-images/5/";
//		File file3 = new File(filesPath2, "S1A_S6_GRDH_1SDV_20160815T214331_20160815T214400_012616_013C9D_2495.zip");
//		MyRead myRead3 = new MyRead(file3,"read");
//		Product targetProduct3 = myRead3.getTargetProduct();
//		LoopLimits limits = new LoopLimits(targetProduct3);
//		int noOfBands = targetProduct3.getNumBands();
//		for (int i = 0; i < noOfBands; i++) {
//			Band band1 = targetProduct3.getBandAt(i);
//			if (band1.getClass() == Band.class ) {
//					GetTile getTile1 = new GetTile(band1, myRead3);
//					Tile tile1 = getTile1.computeTile(0,0);
//					System.out.println(Arrays.toString(tile1.getDataBufferInt()).substring(0, 100));
//					throw new IOException("foobar");
//			}
//		}
		

		MyRead myRead1 = null;
		MyRead myRead2 = null;
		try {
			myRead1 = new MyRead(file1, "read");
			myRead2 = new MyRead(file2, "read");
			compareImagesTiles(myRead1, myRead2);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void compareImagesTiles(MyRead myRead1, MyRead myRead2) {
		Product targetProduct1 = myRead1.getTargetProduct();
		Product targetProduct2 = myRead2.getTargetProduct();
		LoopLimits limits = new LoopLimits(targetProduct1);
		LoopLimits limits2 = new LoopLimits(targetProduct2);

		int nonZeroPoints = 0;
		int ctrue = 0;
		int cfalse = 0;
		int sum = 0;
		int nOfPixels = 0;
		int nOfTiles = 0;
		int noOfBands = targetProduct1.getNumBands();
		System.out.println("NumOfBands:" + noOfBands);
		
		System.out.println("Prod1 - Height  :" + targetProduct1.getSceneRasterHeight());
		System.out.println("Prod1 - Width   :" + targetProduct1.getSceneRasterWidth());
		System.out.println("Prod2 - Height  :" + targetProduct2.getSceneRasterHeight());
		System.out.println("Prod2 - Width   :" + targetProduct2.getSceneRasterWidth());
		
		System.out.println("Prod1 - getNumXTiles  :" + limits.getNumXTiles());
		System.out.println("Prod1 - getNumYTiles  :" + limits.getNumYTiles());
		System.out.println("Prod2 - getNumXTiles  :" + limits2.getNumXTiles());
		System.out.println("Prod2 - getNumYTiles  :" + limits2.getNumYTiles());
		System.out.println("Prod1 - tileSizeWidth :" + targetProduct1.getPreferredTileSize().width);
		System.out.println("Prod1 - tileSizeHeight:" + targetProduct1.getPreferredTileSize().height);
		System.out.println("Prod2 - tileSizeWidth :" + targetProduct2.getPreferredTileSize().width);
		System.out.println("Prod2 - tileSizeHeight:" + targetProduct2.getPreferredTileSize().height);
		
		for (int i = 0; i < noOfBands; i++) {
			Band band1 = targetProduct1.getBandAt(i);
			Band band2 = targetProduct2.getBandAt(i);
			nOfTiles = limits.getNumXTiles() * limits.getNumYTiles();
			nOfPixels = targetProduct1.getPreferredTileSize().width * targetProduct1.getPreferredTileSize().height
					* nOfTiles;

			if (band1.getClass() == Band.class && band2.getClass() == Band.class) {
				System.out.println(band1.getClass().getName() + "-" + band2.getClass().getName());
				for (int tileY = 0; tileY < limits.getNumYTiles(); tileY++) {
					for (int tileX = 0; tileX < limits.getNumXTiles(); tileX++) {
						GetTile getTile1 = new GetTile(band1, myRead1);
						Tile tile1 = getTile1.computeTile(tileX, tileY);

						GetTile getTile2 = new GetTile(band2, myRead2);
						Tile tile2 = getTile2.computeTile(tileX, tileY);

						if (tileX == 0 && tileY == 0) {
							if (tile1.getDataBufferDouble() != null) {
								System.out.println("Double Buffer!!!");
							}
							if (tile1.getDataBufferFloat() != null) {
								System.out.println("Float Buffer!!!");
							}
							if (tile1.getDataBufferInt() != null) {
								System.out.println("Int Buffer!!!");
							}
							if (tile1.getDataBufferShort() != null) {
								System.out.println("Short Buffer!!!");
							}
						}
//						System.out.println(Arrays.toString(tile1.getDataBufferInt()).substring(0, 100));
//						System.out.println(Arrays.toString(tile2.getDataBufferInt()).substring(0, 100));
						System.out.println(Arrays.toString(tile1.getDataBufferFloat()).substring(0, 100));
						System.out.println(Arrays.toString(tile2.getDataBufferFloat()).substring(0, 100));

						float[] localTile = tile1.getDataBufferFloat();
						float[] clusterTile = tile2.getDataBufferFloat();
						boolean equal = Arrays.equals(tile1.getDataBufferFloat(), tile2.getDataBufferFloat());
						int diffs = 0;
						double averageDiff = 0;
						if (!equal) {
							cfalse++;
							for (int j = 0; j < tile1.getDataBufferFloat().length; j++) {
								averageDiff += Math.abs(tile1.getDataBufferFloat()[j] - tile2.getDataBufferFloat()[j]);
								if (tile1.getDataBufferFloat()[j] != tile2.getDataBufferFloat()[j]) {
									diffs++;
								}
							}
							averageDiff /= diffs;
						} else {
							ctrue++;
						}
						sum += diffs;
						System.out.println(tileX + " " + tileY + " ---> " + equal);
						if (!equal) {
							System.out.println(diffs + " " + "out of " + tile1.getDataBufferFloat().length
									+ " pixels are different");
							System.out.println("Average difference\t:\t" + averageDiff);
						}
					}
				}
			}
		}
		System.out.println("Non-zero points : " + nonZeroPoints);
		System.out.println(cfalse + " tiles are different!");
		System.out.println(ctrue + " tiles are the same!");
		System.out.println(sum + " out of " + nOfPixels + " pixels are different!");
		if (cfalse != 0)
			System.out.println(sum / cfalse + " the average number of different pixels per tile!");
	}
}
