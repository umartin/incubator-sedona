package org.apache.sedona.core.spatialPartitioning;

import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.spark.broadcast.Broadcast;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SpatialPartitionerWrapper extends SpatialPartitioner {

    Broadcast<SpatialPartitioner> bc;

    SpatialPartitionerWrapper(Broadcast<SpatialPartitioner> bc) {
        super(GridType.KDBTREE, new ArrayList<Envelope>());
        this.bc = bc;
    }

    @Override
    public <T extends Geometry> Iterator<Tuple2<Integer, T>> placeObject(T spatialObject) throws Exception {
        return bc.value().placeObject(spatialObject);
    }

    @Nullable
    @Override
    public DedupParams getDedupParams() {
        return bc.value().getDedupParams();
    }

    @Override
    public int numPartitions() {
        return bc.value().numPartitions();
    }
    public GridType getGridType()
    {
        return bc.value().getGridType();
    }

    public List<Envelope> getGrids()
    {
        return bc.value().getGrids();
    }

}
