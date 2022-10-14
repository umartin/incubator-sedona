package org.apache.sedona.common.geometryObjects;

import org.locationtech.jts.geom.*;

public class BufferedGeometry extends Geometry {

    private final Geometry wrappedGeometry;
    private final double bufferSize;

    public BufferedGeometry(Geometry wrappedGeometry, double bufferSize) {
        super(wrappedGeometry.getFactory());
        this.wrappedGeometry = wrappedGeometry;
        this.bufferSize = bufferSize;
        this.setUserData(wrappedGeometry.getUserData());
    }

    public Geometry getWrappedGeometry() {
        return wrappedGeometry;
    }

    public double getBufferSize() {
        return bufferSize;
    }

    @Override
    public boolean covers(Geometry g) {
        return getEnvelopeInternal().covers(g.getEnvelopeInternal()) && wrappedGeometry.distance(g) < bufferSize;
    }

    @Override
    public boolean intersects(Geometry g) {
        return getEnvelopeInternal().intersects(g.getEnvelopeInternal()) && wrappedGeometry.distance(g) <= bufferSize;
    }

    @Override
    public String getGeometryType() {
        return getClass().getSimpleName();
    }

    @Override
    public Coordinate getCoordinate() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Coordinate[] getCoordinates() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getNumPoints() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isEmpty() {
        return wrappedGeometry.isEmpty();
    }

    @Override
    public int getDimension() {
        return wrappedGeometry.getDimension();
    }

    @Override
    public Geometry getBoundary() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getBoundaryDimension() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected Geometry reverseInternal() {
        return new BufferedGeometry(wrappedGeometry.reverse(), bufferSize);
    }

    @Override
    public boolean equalsExact(Geometry other, double tolerance) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void apply(CoordinateFilter filter) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void apply(CoordinateSequenceFilter filter) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void apply(GeometryFilter filter) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void apply(GeometryComponentFilter filter) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected Geometry copyInternal() {
        return copy();
    }

    @Override
    public void normalize() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected Envelope computeEnvelopeInternal() {
        Envelope envelope = wrappedGeometry.getEnvelopeInternal().copy();
        envelope.expandBy(bufferSize);
        return envelope;
    }

    @Override
    protected int compareToSameClass(Object o) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected int compareToSameClass(Object o, CoordinateSequenceComparator comp) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected int getTypeCode() {
        return 0;
    }
}
