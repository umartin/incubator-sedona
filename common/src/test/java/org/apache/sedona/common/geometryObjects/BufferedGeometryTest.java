package org.apache.sedona.common.geometryObjects;

import junit.framework.TestCase;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class BufferedGeometryTest extends TestCase {

    public void testCovers() {
        BufferedGeometry bufferedGeometry = new BufferedGeometry(fromWKT("LINESTRING (0 0, 1 1)"), 1.0);
        assertFalse(bufferedGeometry.covers(fromWKT("POINT (1 10)")));
        assertFalse("covers() shouldn't return true from geometries that are just touching", bufferedGeometry.covers(fromWKT("POINT (1 2)")));
        assertTrue(bufferedGeometry.covers(fromWKT("POINT (1 1.5)")));
    }

    public void testIntersects() {
        BufferedGeometry bufferedGeometry = new BufferedGeometry(fromWKT("LINESTRING (0 0, 1 1)"), 1.0);
        assertFalse(bufferedGeometry.intersects(fromWKT("POINT (1 10)")));
        assertTrue("intersects() should return true from geometries that are just touching", bufferedGeometry.intersects(fromWKT("POINT (1 2)")));
        assertTrue(bufferedGeometry.intersects(fromWKT("POINT (1 1.5)")));
    }

    private Geometry fromWKT(String wkt) {
        try {
            return new WKTReader().read(wkt);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}