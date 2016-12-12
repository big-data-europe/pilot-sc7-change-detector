package tileBased;

import java.awt.geom.AffineTransform;
import java.awt.image.DataBuffer;
import java.awt.image.WritableRaster;
import java.lang.reflect.InvocationHandler;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.spark.serializer.KryoRegistrator;
import org.esa.snap.core.datamodel.GcpDescriptor;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Placemark;
import org.esa.snap.core.datamodel.PlacemarkDescriptor;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.ProductNode;
import org.esa.snap.core.datamodel.ProductNodeGroup;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.engine_utilities.datamodel.Unit;
import org.geotools.feature.simple.SimpleFeatureTypeImpl;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.datum.DefaultGeodeticDatum;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.opengis.feature.simple.SimpleFeature;

import com.esotericsoftware.kryo.Kryo;
import com.vividsolutions.jts.geom.Coordinate;

import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyMapSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptySetSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonListSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonMapSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonSetSerializer;
import de.javakaffee.kryoserializers.EnumSetSerializer;
import de.javakaffee.kryoserializers.GregorianCalendarSerializer;
import de.javakaffee.kryoserializers.JdkProxySerializer;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.cglib.CGLibProxySerializer;
import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSetSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaLocalDateSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaLocalDateTimeSerializer;
import readerHDFS.BandInfo;
import serialProcessingNew.LoopLimits;
import serialProcessingNew.MyWarp.WarpData;
import serialProcessingNew.calibration.MySentinel1Calibrator.CalibrationInfo;
import tileBased.metadata.ImageMetadata;
import tileBased.metadata.WarpMetadata;
import tileBased.model.MyTile;

public class MyRegistrator implements KryoRegistrator{

	@Override
	public void registerClasses(Kryo kryo) {
		kryo.register(BandInfo.class);
		kryo.register(Arrays.class);
		kryo.register(Unit.UnitType.class);
		kryo.register(CalibrationInfo.class);
		kryo.register(LoopLimits.class);
		kryo.register(WarpMetadata.class);
		kryo.register(WarpData.class);
		kryo.register(serialProcessingNew.MyWarp.WarpData.class);
		kryo.register(ProductNode.class);
		kryo.register(ProductNodeGroup.class);
		kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
		kryo.register(Product.class);
		kryo.register(PlacemarkDescriptor.class);
		kryo.register(SimpleFeature.class);
		kryo.register(PixelPos.class);
		kryo.register(GeoPos.class);
		kryo.register(AffineTransform.class);
		kryo.register(Coordinate.class);
		kryo.register(Coordinate.class);
		kryo.register(SimpleFeatureTypeImpl.class);
		kryo.register(GcpDescriptor.class);
		kryo.register(GeoCoding.class);
		kryo.register(DefaultGeographicCRS.class);
		kryo.register(DefaultGeodeticDatum.class);
		kryo.register(Placemark.class);
		kryo.register(ImageMetadata.class);
		kryo.register(OperatorException.class);
		kryo.register(List.class);
		kryo.register(MyTile.class);
		kryo.register(WritableRaster.class);
		kryo.register(ProductData.class);
		kryo.register(DataBuffer.class);
		kryo.register( Arrays.asList( "" ).getClass(), new ArraysAsListSerializer() );
		kryo.register( Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer() );
		kryo.register( Collections.EMPTY_MAP.getClass(), new CollectionsEmptyMapSerializer() );
		kryo.register( Collections.EMPTY_SET.getClass(), new CollectionsEmptySetSerializer() );
		kryo.register( Collections.singletonList( "" ).getClass(), new CollectionsSingletonListSerializer() );
		kryo.register( Collections.singleton( "" ).getClass(), new CollectionsSingletonSetSerializer() );
		kryo.register( Collections.singletonMap( "", "" ).getClass(), new CollectionsSingletonMapSerializer() );
		kryo.register( GregorianCalendar.class, new GregorianCalendarSerializer() );
		kryo.register( InvocationHandler.class, new JdkProxySerializer() );
		kryo.register( Enum.class, new EnumSetSerializer() );
		UnmodifiableCollectionsSerializer.registerSerializers( kryo );
		SynchronizedCollectionsSerializer.registerSerializers( kryo );
		kryo.register( CGLibProxySerializer.CGLibProxyMarker.class, new CGLibProxySerializer( ) );
		// joda DateTime, LocalDate and LocalDateTime
		kryo.register( DateTime.class, new JodaDateTimeSerializer() );
		kryo.register( LocalDate.class, new JodaLocalDateSerializer() );
		kryo.register( LocalDateTime.class, new JodaLocalDateTimeSerializer() );
		// guava ImmutableList, ImmutableSet, ImmutableMap, ImmutableMultimap
		ImmutableListSerializer.registerSerializers( kryo );
		ImmutableSetSerializer.registerSerializers( kryo );
		ImmutableMapSerializer.registerSerializers( kryo );
		ImmutableMultimapSerializer.registerSerializers( kryo );

	}
	
}
