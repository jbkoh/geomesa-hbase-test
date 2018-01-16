package io.jbkoh.geomesatest;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.geomesa.index.conf.QueryHints;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

/**
 * Hello world!
 *
 */
public class App {
  static private String simpleFeatureTypeName = "TestFeature";
  static GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
  DataStore dataStore = null;

  static SimpleFeatureType createSimpleFeatureType() throws SchemaException {
    SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();
    b.setName(simpleFeatureTypeName);
    b.add("loc", Geometry.class, 4326);
    b.setDefaultGeometry("loc");
    b.add("uuid", String.class);
    b.add("date", Date.class);
    b.add("value", String.class); // TODO: Should this be String? Can't perform value-based query.
    SimpleFeatureType sft = b.buildFeatureType();
    sft.getUserData().put("geomesa.mixed.geometries", "true");
    // TODO: Below should work but does not.
    //       When featureWriter is generrated, "Could not read table name from metadata for index xz2:1"
    sft.getDescriptor("uuid").getUserData().put("index", "join");
    sft.getDescriptor("uuid").getUserData().put("cardinality", "high");
    //sft.getUserData().put("geomesa.xz.precision", 14); // Default is 12. Experimental.
    return sft;
  }
  
  public void geomesa_initialize() {
      if (dataStore == null) {
        Map<String, Serializable> parameters = new HashMap<>();
        parameters.put("bigtable.table.name", "TestTable");

        // DataStoreFinder is from Geotools, returns an indexed datastore if one is
        // available.
        try {
          dataStore = DataStoreFinder.getDataStore(parameters);
          SimpleFeatureType simpleFeatureType = createSimpleFeatureType();
          dataStore.createSchema(simpleFeatureType);
        } catch (Exception e) {
          e.printStackTrace();
          System.exit(1);
        }
        System.out.println("Geomesa initialized");
    } // end if
  }
  
  public void testInsertData() {
    try {
      FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriterAppend(simpleFeatureTypeName, Transaction.AUTO_COMMIT);
      SimpleFeature newFeature = writer.next();
      Point point = geometryFactory.createPoint(new Coordinate(-117.0, 33.0));
      newFeature.setAttribute("loc", point);
      newFeature.setAttribute("uuid", "123456789");
      newFeature.setAttribute("value", 77.0);
      newFeature.setAttribute("date", new Date(516143059000L));
      writer.write();
      writer.close();
      System.out.println("Write success");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public void testQueryData() {
    try {
      String cqlGeometry = "BBOX(" + "loc" + ", " + Float.toString(-120) 
                                           + ", " + Float.toString(30) 
                                           + ", " + Float.toString(-110) 
                                           + ", " + Float.toString(40) 
                                           + ")";
      String filter = cqlGeometry;
      Filter cqlFilter = CQL.toFilter(filter);
      Query query = new Query(simpleFeatureTypeName, cqlFilter);
      query.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.FALSE);
      FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = dataStore.getFeatureSource(simpleFeatureTypeName);
      FeatureIterator<SimpleFeature> featureItr = featureSource.getFeatures(query).features();
      while (featureItr.hasNext()) {
        Feature feature = null;
        feature = featureItr.next();
        System.out.println("uuid" + feature.getProperty("uuid").getValue());
        Date date = (Date) feature.getProperty("date").getValue();
        System.out.println("timestamp" + date.getTime());
        Geometry loc = (Geometry) feature.getProperty("loc").getValue();
        String geometryType = loc.getGeometryType();
        Coordinate[] cds = loc.getCoordinates();
        System.out.println(cds[0]);
        System.out.println("value" + feature.getProperty("value").getValue());
        System.out.println("Read Success");
      }
      featureItr.close();
      
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    System.out.println("Hello World!");
    App app = new App();
    app.geomesa_initialize();
    app.testInsertData();
    app.testQueryData();
  }
}
