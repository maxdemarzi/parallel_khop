package com.maxdemarzi;

import com.maxdemarzi.results.LongResult;
import org.neo4j.graphdb.*;
import org.neo4j.internal.kernel.api.*;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class Procedures {

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/neo4j.log`
    @Context
    public Log log;

    // How many CPUs do I have available?
    private static final int THREADS = Runtime.getRuntime().availableProcessors();


    @Procedure(name = "com.maxdemarzi.khops", mode = Mode.READ)
    @Description("com.maxdemarzi.khops(Node node, Long distance, List<String> relationshipTypes)")
    public Stream<LongResult> khops(@Name("startingNode") Node startingNode,
                                    @Name(value = "distance", defaultValue = "1") Long distance,
                                    @Name(value = "relationshipTypes", defaultValue = "[]") List<String> relationshipTypes) {
        if (distance < 1) return Stream.empty();

        if (startingNode == null) {
            return Stream.empty();
        } else {

            RelationshipType[] types = new RelationshipType[relationshipTypes.size()];
            for (int i = 0; i < types.length; i++) {
                types[i] = RelationshipType.withName(relationshipTypes.get(i));
            }

            Node node;
            // Initialize bitmaps for iteration
            Roaring64NavigableMap seen = new Roaring64NavigableMap();
            Roaring64NavigableMap nextA = new Roaring64NavigableMap();
            Roaring64NavigableMap nextB = new Roaring64NavigableMap();

            long nodeId = startingNode.getId();
            seen.add(nodeId);
            Iterator<Long> iterator;

            // First Hop
            if (types.length == 0) {
                for (Relationship r : startingNode.getRelationships()) {
                    nextB.add(r.getOtherNodeId(nodeId));
                }
            } else {
                for (Relationship r : startingNode.getRelationships(types)) {
                    nextB.add(r.getOtherNodeId(nodeId));
                }
            }

            for (int i = 1; i < distance; i++) {
                // next even Hop
                nextB.andNot(seen);
                seen.or(nextB);
                nextA.clear();
                iterator = nextB.iterator();
                while (iterator.hasNext()) {
                    nodeId = iterator.next();
                    node = db.getNodeById(nodeId);
                    if (types.length == 0) {
                        for (Relationship r : node.getRelationships()) {
                            nextA.add(r.getOtherNodeId(nodeId));
                        }
                    } else {
                        for (Relationship r : node.getRelationships(types)) {
                            nextA.add(r.getOtherNodeId(nodeId));
                        }
                    }
                }

                i++;
                if (i < distance) {
                    // next odd Hop
                    nextA.andNot(seen);
                    seen.or(nextA);
                    nextB.clear();
                    iterator = nextA.iterator();
                    while (iterator.hasNext()) {
                        nodeId = iterator.next();
                        node = db.getNodeById(nodeId);
                        if (types.length == 0) {
                            for (Relationship r : node.getRelationships()) {
                                nextB.add(r.getOtherNodeId(nodeId));
                            }
                        } else {
                            for (Relationship r : node.getRelationships(types)) {
                                nextB.add(r.getOtherNodeId(nodeId));
                            }

                        }
                    }
                }
            }

            if ((distance % 2) == 0) {
                seen.or(nextA);
            } else {
                seen.or(nextB);
            }
            // remove starting node
            seen.removeLong(startingNode.getId());

            return Stream.of(new LongResult(seen.getLongCardinality()));
        }
    }

    @Procedure(name = "com.maxdemarzi.khops2", mode = Mode.READ)
    @Description("com.maxdemarzi.khops2(Node node, Long distance, List<String> relationshipTypes)")
    public Stream<LongResult> khops2(@Name("startingNode") Node startingNode, @Name(value = "distance", defaultValue = "1") Long distance,
                                   @Name(value = "relationshipTypes", defaultValue = "[]") List<String> relationshipTypes) {
        if (distance < 1) return Stream.empty();

        if (startingNode == null) {
            return Stream.empty();
        } else {
            DependencyResolver dependencyResolver = ((GraphDatabaseAPI)db).getDependencyResolver();
            final ThreadToStatementContextBridge ctx = dependencyResolver.resolveDependency(ThreadToStatementContextBridge.class, DependencyResolver.SelectionStrategy.FIRST);
            KernelTransaction ktx = ctx.getKernelTransactionBoundToThisThread(true);
            CursorFactory cursors = ktx.cursors();
            Read read = ktx.dataRead();
            TokenRead tokenRead = ktx.tokenRead();

            int[] types = new int[relationshipTypes.size()];
            for (int i = 0; i < types.length; i++) {
                types[i] = tokenRead.relationshipType(relationshipTypes.get(i));
            }

            Roaring64NavigableMap seen = new Roaring64NavigableMap();
            Roaring64NavigableMap nextOdd = new Roaring64NavigableMap();
            Roaring64NavigableMap nextEven = new Roaring64NavigableMap();

            seen.add(startingNode.getId());

            RelationshipTraversalCursor rels = cursors.allocateRelationshipTraversalCursor();
            NodeCursor nodeCursor = cursors.allocateNodeCursor();

            read.singleNode(startingNode.getId(), nodeCursor);

            nodeCursor.next();

            // First Hop
            if (types.length == 0) {
                nodeCursor.allRelationships(rels);
                while (rels.next()) {
                    nextEven.add(rels.neighbourNodeReference());
                }
            } else {
                RelationshipSelectionCursor typedRels = RelationshipSelections.allCursor(cursors, nodeCursor, types);
                while (typedRels.next()) {
                    nextEven.add(typedRels.otherNodeReference());
                }
            }

            for (int i = 1; i < distance; i++) {
                // Next even Hop
                nextHop(read, seen, nextOdd, nextEven, types, cursors);

                i++;
                if (i < distance) {
                    // Next odd Hop
                    nextHop(read, seen, nextEven, nextOdd, types, cursors);
                }
            }

            if ((distance % 2) == 0) {
                seen.or(nextOdd);
            } else {
                seen.or(nextEven);
            }

            // remove starting node
            seen.removeLong(startingNode.getId());

            return Stream.of(new LongResult(seen.getLongCardinality()));
        }
    }

    @Procedure(name = "com.maxdemarzi.parallel.khops2", mode = Mode.READ)
    @Description("com.maxdemarzi.parallel.khops2(Node node, Long distance, List<String> relationshipTypes)")
    public Stream<LongResult> parallelkhops2(@Name("startingNode") Node startingNode,
                                           @Name(value = "distance", defaultValue = "1") Long distance,
                                           @Name(value = "relationshipTypes", defaultValue = "[]") List<String> relationshipTypes) {
        if (distance < 1) return Stream.empty();

        if (startingNode == null) {
            return Stream.empty();
        } else {
            DependencyResolver dependencyResolver = ((GraphDatabaseAPI) db).getDependencyResolver();
            final ThreadToStatementContextBridge ctx = dependencyResolver.resolveDependency(ThreadToStatementContextBridge.class, DependencyResolver.SelectionStrategy.FIRST);
            KernelTransaction ktx = ctx.getKernelTransactionBoundToThisThread(true);
            CursorFactory cursors = ktx.cursors();
            Read read = ktx.dataRead();

            TokenRead tokenRead = ktx.tokenRead();

            int[] types = new int[relationshipTypes.size()];
            for (int i = 0; i < types.length; i++) {
                types[i] = tokenRead.relationshipType(relationshipTypes.get(i));
            }

            ExecutorService service = Executors.newFixedThreadPool(THREADS);
            Roaring64NavigableMap seen = new Roaring64NavigableMap();

            Phaser ph = new Phaser(1);

            Roaring64NavigableMap[] nextA = new Roaring64NavigableMap[THREADS + 1];
            Roaring64NavigableMap[] nextB = new Roaring64NavigableMap[THREADS + 1];

            for (int i = 0; i < (THREADS + 1); ++i) {
                nextA[i] = new Roaring64NavigableMap();
                nextB[i] = new Roaring64NavigableMap();
            }

            seen.add(startingNode.getId());

            RelationshipTraversalCursor rels = cursors.allocateRelationshipTraversalCursor();
            NodeCursor nodeCursor = cursors.allocateNodeCursor();

            read.singleNode(startingNode.getId(), nodeCursor);
            nodeCursor.next();

            // First Hop
            final AtomicLong index = new AtomicLong(0);
            if (types.length == 0) {
                nodeCursor.allRelationships(rels);
                while (rels.next()) {
                    nextB[(int)(index.getAndIncrement() % THREADS)].add(rels.neighbourNodeReference());
                }
            } else {
                RelationshipSelectionCursor typedRels = RelationshipSelections.allCursor(cursors, nodeCursor, types);
                while (typedRels.next()) {
                    nextB[(int)(index.getAndIncrement() % THREADS)].add(typedRels.otherNodeReference());
                }
            }

            // Next even Hop
            for (int i = 1; i < distance; i++) {

                // Combine next (after initial)
                if (i > 1) {
                    for (int j = 0; j < THREADS; j++) {
                        nextB[THREADS].or(nextB[j]);
                        nextB[j].clear();
                    }

                    // Redistribute next
                    index.set(0);
                    nextB[THREADS].andNot(seen);
                    seen.or(nextB[THREADS]);

                    nextB[THREADS].forEach(l -> nextB[(int)(index.getAndIncrement() % THREADS)].add(l));

                } else {
                    for (int j = 0; j < THREADS; j++) {
                        seen.or(nextB[j]);
                    }
                }

                // Next even Hop
                for (int j = 0; j < THREADS; j++) {
                    nextA[j].clear();
                    service.submit(new NextHop(db, log, nextA[j], nextB[j], types, ph));
                }

                // Wait until all have finished
                ph.arriveAndAwaitAdvance();

                i++;
                if (i < distance) {

                    // Combine next
                    for (int j = 0; j < THREADS; j++) {
                        nextA[THREADS].or(nextA[j]);
                        nextA[j].clear();
                    }

                    // Redistribute next
                    index.set(0);
                    nextA[THREADS].andNot(seen);
                    seen.or(nextA[THREADS]);
                    nextA[THREADS].forEach(l -> nextA[(int)(index.getAndIncrement() % THREADS)].add(l));

                    // Next odd Hop
                    for (int j = 0; j < THREADS; j++) {
                        nextB[j].clear();
                        service.submit(new NextHop(db, log, nextB[j], nextA[j], types, ph));
                    }

                    // Wait until all have finished
                    ph.arriveAndAwaitAdvance();

                }
            }

            if ((distance % 2) == 0) {
                for (int j = 0; j < THREADS; j++) {
                    seen.or(nextA[j]);
                }
            } else {
                for (int j = 0; j < THREADS; j++) {
                    seen.or(nextB[j]);
                }
            }

            ph.arriveAndDeregister();

            // remove starting node
            seen.removeLong(startingNode.getId());

            return Stream.of(new LongResult(seen.getLongCardinality()));

        }
    }

    private void nextHop(Read read, Roaring64NavigableMap seen, Roaring64NavigableMap next,
                         Roaring64NavigableMap current, int[] types, CursorFactory cursors) {
        current.andNot(seen);
        seen.or(current);
        next.clear();
        RelationshipTraversalCursor rels = cursors.allocateRelationshipTraversalCursor();
        NodeCursor nodeCursor = cursors.allocateNodeCursor();

        current.forEach(nodeId -> {
            read.singleNode(nodeId, nodeCursor);
            nodeCursor.next();

            if (types.length == 0) {
                nodeCursor.allRelationships(rels);
                while (rels.next()) {
                    next.add(rels.neighbourNodeReference());
                }
            } else {
                RelationshipSelectionCursor typedRels = RelationshipSelections.allCursor(cursors, nodeCursor, types);
                while (typedRels.next()) {
                    next.add(typedRels.otherNodeReference());
                }
            }
        });
    }

}

