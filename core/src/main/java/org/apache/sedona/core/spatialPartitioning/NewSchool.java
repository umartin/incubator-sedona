package org.apache.sedona.core.spatialPartitioning;

import org.antlr.v4.runtime.tree.Tree;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.sedona.core.spatialPartitioning.quadtree.StandardQuadTree;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.core.utils.RDDSampleUtils;
import org.apache.spark.GetMapOutputMessage;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.random.SamplingUtils;
import org.jetbrains.annotations.Nullable;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Int;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
//import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Collectors;


public class NewSchool implements Serializable {

    private static class PartAcc extends AccumulatorV2<Tuple2<Integer, Set<Integer>>, Map<Integer, Set<Integer>>> {

        private Map<Integer, Set<Integer>> value;

        public PartAcc(Map<Integer, Set<Integer>> value) {
            this.value = value;
        }

        @Override
        public boolean isZero() {
            return value.isEmpty();
        }

        @Override
        public AccumulatorV2<Tuple2<Integer, Set<Integer>>, Map<Integer, Set<Integer>>> copy() {
            return new PartAcc(new HashMap<>(value));
        }

        @Override
        public void reset() {
            value.clear();
        }

        @Override
        public void add(Tuple2<Integer, Set<Integer>> v) {
            boolean empty = value.isEmpty();
            if (!value.containsKey(v._1())) {
                value.put(v._1(), new HashSet<>());
            }
            value.get(v._1()).addAll(v._2());
            value = new HashMap<>(value);
            if (empty) {
                System.out.println("Added: " + value);
            }
        }

        @Override
        public void merge(AccumulatorV2<Tuple2<Integer, Set<Integer>>, Map<Integer, Set<Integer>>> other) {
            System.out.println("Merging: " + value + " and " + other.value());
            for (Map.Entry<Integer, Set<Integer>> entry : other.value().entrySet()) {
                add(new Tuple2<>(entry.getKey(), entry.getValue()));
            }
            System.out.println("Post merge: " + value);
        }

        @Override
        public Map<Integer, Set<Integer>> value() {
            return value;
        }
    }

    private final class V1SpatialPartitioner extends SpatialPartitioner {

        private final PartitioningUtils pu;
        private final PartAcc acc;

        protected V1SpatialPartitioner(GridType gridType, PartitioningUtils pu, PartAcc acc) {
            super(gridType, pu.fetchLeafZones());
            this.pu = pu;
            this.acc = acc;
        }

        @Override
        public <T extends Geometry> Iterator<Tuple2<Integer, T>> placeObject(T spatialObject) throws Exception {
            Set<Integer> keys = pu.getKeys(spatialObject);
            if (keys.isEmpty()) {
                return new ArrayList<Tuple2<Integer, T>>().iterator();
            }
            Integer partition = keys.stream().findFirst().get();
            acc.add(new Tuple2<>(partition, keys));
            return Arrays.asList(new Tuple2<>(partition, spatialObject)).iterator();
        }

        @Nullable
        @Override
        public DedupParams getDedupParams() {
            return null;
        }

        @Override
        public boolean equals(Object obj) {
            return true;
        }
    }

    private static class FollowerPartitioner extends SpatialPartitioner {

        private final Map<Integer, Set<Integer>> gridToPartitions;
        private final PartitioningUtils pu;

        protected FollowerPartitioner(GridType gridType, PartitioningUtils pu, Map<Integer, Set<Integer>> gridToPartitions) {
            super(gridType, pu.fetchLeafZones());
            this.pu = pu;
            this.gridToPartitions = gridToPartitions;
        }

        @Override
        public <T extends Geometry> Iterator<Tuple2<Integer, T>> placeObject(T spatialObject) throws Exception {
            return pu.getKeys(spatialObject).stream()
                    .flatMap(i -> gridToPartitions.getOrDefault(i, new HashSet<>())
                            .stream()).collect(Collectors.toSet()).stream().map(i -> new Tuple2<>(i, spatialObject)).iterator();
        }

        @Nullable
        @Override
        public DedupParams getDedupParams() {
            return null;
        }

        @Override
        public boolean equals(Object obj) {
            return true;
        }
    }

    private final SpatialRDD<Geometry> dominant;
    private final SpatialRDD<Geometry> follower;
    private final int numPartitions;
    private final GridType gridType;

    public NewSchool(SpatialRDD<Geometry> dominant, SpatialRDD<Geometry> follower, int numberOfPartitions, GridType gridType) {
        this.dominant = dominant;
        this.follower = follower;
        this.numPartitions = numberOfPartitions;
        this.gridType = gridType;
    }

    public void oldschool() {
        SpatialPartitioner partitioner = partitioner();
        dominant.spatialPartitioning(partitioner);
        follower.spatialPartitioning(partitioner);
    }

    public void newschhol() {
        PartitioningUtils grid = createGrid();
        Map<Integer, Set<Integer>> karta = new HashMap<>();
        for (int i = 0; i < grid.fetchLeafZones().size(); i++) {
            karta.put(i, new HashSet<>());
        }
        PartAcc partAcc = new PartAcc(karta);
        int actualPartionNum = grid.fetchLeafZones().size();
        System.out.println("Size: " + grid.fetchLeafZones().size());
        SparkSession.active().sparkContext().register(partAcc);

        // lägg på set av gridid
        JavaPairRDD<TreeSet, Geometry> withGridIds = dominant.rawSpatialRDD.mapToPair(geom -> {
            return new Tuple2<>(new TreeSet(grid.getKeys(geom)), geom);
        });
        // faktisk partitionering
        dominant.spatialPartitionedRDD = withGridIds.partitionBy(new Partitioner() {
            @Override
            public int getPartition(Object key) {
                return ((TreeSet<Integer>)key).first();
            }

            @Override
            public int numPartitions() {
                return actualPartionNum;
            }
        }).map(t -> t._2());
        // räkna ut gridid -> set av partitioner.
        Map<Integer, Set<Integer>> karta2 = withGridIds.mapToPair(t -> new Tuple2<Integer, Set<Integer>>((Integer) t._1().first(), t._1())).foldByKey(new TreeSet<Integer>(), (m1, m2) -> {
            TreeSet<Integer> integers = new TreeSet<>();
            integers.addAll(m1);
            integers.addAll(m2);
            return integers;
        }).collect().stream().collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));


        //dominant.spatialPartitionedRDD.foreach(f -> {});
        System.out.println(karta2);
        System.out.println(flip(karta2));

        follower.spatialPartitioning(new FollowerPartitioner(gridType, grid, flip(karta2)));
        dominant.partitioner = follower.partitioner;
    }

    static Map<Integer, Set<Integer>> flip(Map<Integer, Set<Integer>> value) {
            HashMap<Integer, Set<Integer>> flipped = new HashMap<>();
            value.entrySet().stream().flatMap(entry -> entry.getValue().stream().map((val -> new Tuple2<>(val, entry.getKey())))).forEach(t -> {
                if (!flipped.containsKey(t._1())) {
                    flipped.put(t._1(), new HashSet<>());
                }
                flipped.get(t._1()).add(t._2());
            });
            return flipped;
    }

    SpatialPartitioner partitioner() {
        PartitioningUtils pu = createGrid();
        if (pu instanceof KDB) {
            return new KDBTreePartitioner((KDB) pu);
        }
        if (pu instanceof StandardQuadTree) {
            return new QuadTreePartitioner((StandardQuadTree<? extends Geometry>) pu);
        }
        throw new RuntimeException("lol");
    }

    PartitioningUtils createGrid() {
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Number of partitions must be >= 0");
        }

        if (dominant.boundaryEnvelope == null) {
            throw new RuntimeException("[AbstractSpatialRDD][spatialPartitioning] SpatialRDD boundary is null. Please call analyze() first.");
        }
        if (dominant.approximateTotalCount == -1) {
            throw new RuntimeException("[AbstractSpatialRDD][spatialPartitioning] SpatialRDD total count is unkown. Please call analyze() first.");
        }

        //Calculate the number of samples we need to take.
        int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, dominant.approximateTotalCount, dominant.getSampleNumber());
        //Take Sample
        // RDD.takeSample implementation tends to scan the data multiple times to gather the exact
        // number of samples requested. Repeated scans increase the latency of the join. This increase
        // is significant for large datasets.
        // See https://github.com/apache/spark/blob/412b0e8969215411b97efd3d0984dc6cac5d31e0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L508
        // Here, we choose to get samples faster over getting exactly specified number of samples.
        final double fraction = SamplingUtils.computeFractionForSampleSize(sampleNumberOfRecords, dominant.approximateTotalCount, false);
        List<Envelope> samples = dominant.rawSpatialRDD.sample(false, fraction)
                .map(new Function<Geometry, Envelope>()
                {
                    @Override
                    public Envelope call(Geometry geometry)
                            throws Exception
                    {
                        return geometry.getEnvelopeInternal();
                    }
                })
                .collect();
        System.out.println("Sample size: " + samples.size());
        System.out.println("Numpartitions: " + numPartitions);

        //logger.info("Collected " + samples.size() + " samples");

        // Add some padding at the top and right of the boundaryEnvelope to make
        // sure all geometries lie within the half-open rectangle.
        final Envelope paddedBoundary = new Envelope(
                dominant.boundaryEnvelope.getMinX(), dominant.boundaryEnvelope.getMaxX() + 0.01,
                dominant.boundaryEnvelope.getMinY(), dominant.boundaryEnvelope.getMaxY() + 0.01);

        switch (gridType) {
            case EQUALGRID: {
                // Force the quad-tree to grow up to a certain level
                // So the actual num of partitions might be slightly different
                int minLevel = (int) Math.max(Math.log(numPartitions)/Math.log(4), 0);
                QuadtreePartitioning quadtreePartitioning = new QuadtreePartitioning(new ArrayList<Envelope>(), paddedBoundary,
                        numPartitions, minLevel);
                return quadtreePartitioning.getPartitionTree();
            }
            case QUADTREE: {
                QuadtreePartitioning quadtreePartitioning = new QuadtreePartitioning(samples, paddedBoundary, numPartitions);
                return quadtreePartitioning.getPartitionTree();
            }
            case KDBTREE: {
                final KDB tree = new KDB(samples.size() / numPartitions, numPartitions, paddedBoundary);
                for (final Envelope sample : samples) {
                    tree.insert(sample);
                }
                tree.assignLeafIds();
                return tree;
            }
            default:
                throw new RuntimeException("[AbstractSpatialRDD][spatialPartitioning] Unsupported spatial partitioning method. " +
                        "The following partitioning methods are not longer supported: R-Tree, Hilbert curve, Voronoi");
        }

    }
}
