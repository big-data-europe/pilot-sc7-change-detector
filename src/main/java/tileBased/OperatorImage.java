package tileBased;

import javax.media.jai.ImageLayout;
import javax.media.jai.SourcelessOpImage;

public class OperatorImage extends SourcelessOpImage {
	 private OperatorImage(ImageLayout imageLayout) {
	        super(imageLayout,
	              null,
	              imageLayout.getSampleModel(null),
	              imageLayout.getMinX(null),
	              imageLayout.getMinY(null),
	              imageLayout.getWidth(null),
	              imageLayout.getHeight(null));
	    }
}
