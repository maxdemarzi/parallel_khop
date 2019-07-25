package com.maxdemarzi;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.*;
import org.neo4j.internal.kernel.api.*;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.concurrent.Phaser;

public class NextHop implements Runnable {
    private final GraphDatabaseService db;
    private final Log log;
    private final Roaring64NavigableMap next;
    private final Roaring64NavigableMap current;
    private final int[] types;
    private Phaser ph;

    public NextHop(GraphDatabaseService db, Log log, Roaring64NavigableMap next,
                   Roaring64NavigableMap current, int[] types, Phaser ph) {
        this.db = db;
        this.log = log;
        this.next = next;
        this.current = current;
        this.types = types;
        this.ph = ph;
        ph.register();
    }

    @Override
    public void run() {
        try(Transaction tx = db.beginTx()) {
            DependencyResolver dependencyResolver = ((GraphDatabaseAPI) db).getDependencyResolver();
            final ThreadToStatementContextBridge ctx = dependencyResolver.resolveDependency(ThreadToStatementContextBridge.class, DependencyResolver.SelectionStrategy.FIRST);
            KernelTransaction ktx = ctx.getKernelTransactionBoundToThisThread(true);
            CursorFactory cursors = ktx.cursors();
            Read read = ktx.dataRead();

            RelationshipTraversalCursor rels = cursors.allocateRelationshipTraversalCursor();
            NodeCursor nodeCursor = cursors.allocateNodeCursor();

            current.forEach(l -> {
                read.singleNode(l,nodeCursor);
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
        ph.arriveAndDeregister();
    }
}
