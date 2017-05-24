package eu.bde.sc7pilot.taskbased;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.VirtualBand;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.util.ProductUtils;
import org.esa.snap.engine_utilities.gpf.OperatorUtils;

public class MyBandSelect extends AbstractOperator {
	
    private Product sourceProduct;
    private String[] selectedPolarisations;
    private String[] sourceBandNames;
    private String bandNamePattern;

  public MyBandSelect(String[] selectedPolarisations, String[] sourceBandNames) {
	  
	this.selectedPolarisations = selectedPolarisations;
	this.sourceBandNames = sourceBandNames;
  }
  
    @Override
    public void initialize() throws OperatorException {

        try {
            targetProduct = new Product(sourceProduct.getName(), 
            							sourceProduct.getProductType(),
            							sourceProduct.getSceneRasterWidth(),
            							sourceProduct.getSceneRasterHeight());
            ProductUtils.copyProductNodes(sourceProduct, targetProduct);
            addSelectedBands();
        } 
        catch (Throwable e) {
            OperatorUtils.catchOperatorException(getId(), e);
        }
    }

    private void addSelectedBands() throws OperatorException {

        final Band[] sourceBands = OperatorUtils.getSourceBands(sourceProduct, sourceBandNames, true);

        for (Band srcBand : sourceBands) {
            // check first if polarisation is found
            if (selectedPolarisations != null && selectedPolarisations.length > 0) {
                String pol = OperatorUtils.getPolarizationFromBandName(srcBand.getName());
                boolean foundPol = false;
                for(String selPol : selectedPolarisations) {
                    if(pol.equalsIgnoreCase(selPol)) {
                        foundPol = true;
                    }
                }
                if(!foundPol) {
                    continue;
                }
            }

            if(bandNamePattern != null && !bandNamePattern.isEmpty()) {
                // check regular expression such as contain mst "^.*mst.*$"

                Pattern pattern = Pattern.compile(bandNamePattern);
                Matcher matcher = pattern.matcher(srcBand.getName());
                if (!matcher.matches()) {
                    continue;
                }
            }

            if (srcBand instanceof VirtualBand) {
                final VirtualBand sourceBand = (VirtualBand) srcBand;
                final VirtualBand targetBand = new VirtualBand(sourceBand.getName(),
                        sourceBand.getDataType(),
                        sourceBand.getRasterWidth(),
                        sourceBand.getRasterHeight(),
                        sourceBand.getExpression());
                ProductUtils.copyRasterDataNodeProperties(sourceBand, targetBand);
                targetProduct.addBand(targetBand);
            } else {
                ProductUtils.copyBand(srcBand.getName(), sourceProduct, targetProduct, true);
            }
        }

        if(targetProduct.getNumBands() == 0) {
            throw new OperatorException("No valid bands found in target product");
        }
    }

	public Product getSourceProduct() {
		return sourceProduct;
	}

	public void setSourceProduct(Product sourceProduct) {
		this.sourceProduct = sourceProduct;
	}

	public String[] getSelectedPolarisations() {
		return selectedPolarisations;
	}

	public void setSelectedPolarisations(String[] selectedPolarisations) {
		this.selectedPolarisations = selectedPolarisations;
	}
}
