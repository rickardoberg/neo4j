/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.factory;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.function.Supplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipAutoIndexer;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.Clock;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.KernelEventHandlers;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.TransactionEventHandlers;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.NonTransactionalTokenNameLookup;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.cache.MonitorGc;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.StartupStatisticsProvider;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.coreapi.IndexManagerImpl;
import org.neo4j.kernel.impl.coreapi.IndexProvider;
import org.neo4j.kernel.impl.coreapi.IndexProviderImpl;
import org.neo4j.kernel.impl.coreapi.LegacyIndexProxy;
import org.neo4j.kernel.impl.coreapi.NodeAutoIndexerImpl;
import org.neo4j.kernel.impl.coreapi.RelationshipAutoIndexerImpl;
import org.neo4j.kernel.impl.coreapi.schema.SchemaImpl;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.Logging;

public class DataSourceModule
{

    public ThreadToStatementContextBridge threadToTransactionBridge;

    public NodeManager nodeManager;

    public NeoStoreDataSource neoStoreDataSource;

    public IndexManager indexManager;

    public Schema schema;

    public AvailabilityGuard availabilityGuard;

    public Supplier<KernelAPI> kernelAPI;

    public Supplier<QueryExecutionEngine> queryExecutor;

    public KernelEventHandlers kernelEventHandlers;

    public TransactionEventHandlers transactionEventHandlers;

    public Supplier<StoreId> storeId;

    public DataSourceModule(final GraphDatabaseFacadeFactory.Dependencies dependencies, final PlatformModule platformModule, EditionModule editionModule)
    {
        final org.neo4j.kernel.impl.util.Dependencies deps = platformModule.dependencies;
        Config config = platformModule.config;
        Logging logging = platformModule.logging;
        FileSystemAbstraction fileSystem = platformModule.fileSystem;
        DataSourceManager dataSourceManager = platformModule.dataSourceManager;
        LifeSupport life = platformModule.life;
        final GraphDatabaseFacade graphDatabaseFacade = platformModule.graphDatabaseFacade;
        RelationshipTypeTokenHolder relationshipTypeTokenHolder = editionModule.relationshipTypeTokenHolder;
        File storeDir = platformModule.storeDir;
        DiagnosticsManager diagnosticsManager = platformModule.diagnosticsManager;
        StringLogger msgLog = logging.getMessagesLog( getClass() );

        threadToTransactionBridge = deps.satisfyDependency( life.add( new ThreadToStatementContextBridge() ) );

        nodeManager = deps.satisfyDependency( new NodeManager(graphDatabaseFacade,
                        threadToTransactionBridge, relationshipTypeTokenHolder ));

        NodeProxy.NodeActions nodeActions = deps.satisfyDependency( createNodeActions( graphDatabaseFacade,
                threadToTransactionBridge, nodeManager ) );
        RelationshipProxy.RelationshipActions relationshipActions = deps.satisfyDependency( createRelationshipActions
                ( graphDatabaseFacade, threadToTransactionBridge, nodeManager, relationshipTypeTokenHolder ) );

        transactionEventHandlers = new TransactionEventHandlers( nodeActions, relationshipActions,
                threadToTransactionBridge );

        IndexConfigStore indexStore = life.add( deps.satisfyDependency(new IndexConfigStore( storeDir, fileSystem ) ));

        diagnosticsManager.prependProvider( config );

        life.add( platformModule.kernelExtensions );

        schema = new SchemaImpl( threadToTransactionBridge );

        final LegacyIndexProxy.Lookup indexLookup = new LegacyIndexProxy.Lookup()
                {
                    @Override
                    public GraphDatabaseService getGraphDatabaseService()
                    {
                        return graphDatabaseFacade;
                    }
                };

        final IndexProvider indexProvider = new IndexProviderImpl( indexLookup, threadToTransactionBridge );
        NodeAutoIndexerImpl nodeAutoIndexer = life.add( new NodeAutoIndexerImpl( config, indexProvider, nodeManager ) );
        RelationshipAutoIndexer relAutoIndexer = life.add( new RelationshipAutoIndexerImpl( config, indexProvider,
                nodeManager ) );
        indexManager = new IndexManagerImpl(
                threadToTransactionBridge, indexProvider, nodeAutoIndexer, relAutoIndexer );

        // Factories for things that needs to be created later
        StoreFactory storeFactory = new StoreFactory( config, editionModule.idGeneratorFactory, platformModule.pageCache, fileSystem,
                        logging.getMessagesLog( StoreFactory.class ), platformModule.monitors );

        StartupStatisticsProvider startupStatistics = deps.satisfyDependency(new StartupStatisticsProvider());

        SchemaWriteGuard schemaWriteGuard = deps.satisfyDependency( editionModule.schemaWriteGuard );

        StoreUpgrader storeMigrationProcess = new StoreUpgrader( editionModule.upgradeConfiguration, fileSystem,
                platformModule.monitors.newMonitor( StoreUpgrader.Monitor.class ), logging );

        StringLogger messagesLog = logging.getMessagesLog( StoreMigrator.class );
        VisibleMigrationProgressMonitor progressMonitor =
                new VisibleMigrationProgressMonitor( messagesLog, System.out );
        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( new StoreVersionCheck( fileSystem ) );
        storeMigrationProcess.addParticipant( new StoreMigrator(
                progressMonitor, fileSystem, upgradableDatabase, config, logging ) );

        TransactionCounters transactionMonitor = deps.satisfyDependency( createTransactionCounters() );

        Guard guard = config.get( GraphDatabaseFacadeFactory.Configuration.execution_guard_enabled ) ? deps.satisfyDependency( new Guard( msgLog
        ) ) : null;

        kernelEventHandlers = new KernelEventHandlers( logging.getMessagesLog(
                KernelEventHandlers.class ) );

        KernelPanicEventGenerator kernelPanicEventGenerator = new KernelPanicEventGenerator( kernelEventHandlers );

        KernelHealth kernelHealth = deps.satisfyDependency(new KernelHealth( kernelPanicEventGenerator, logging ));

        neoStoreDataSource = deps.satisfyDependency(new NeoStoreDataSource( config,
                storeFactory, logging.getMessagesLog( NeoStoreDataSource.class ), platformModule.jobScheduler, logging,
                new NonTransactionalTokenNameLookup( editionModule.labelTokenHolder, editionModule.propertyKeyTokenHolder ),
                deps, editionModule.propertyKeyTokenHolder, editionModule.labelTokenHolder, relationshipTypeTokenHolder,
                editionModule.lockManager, schemaWriteGuard, transactionEventHandlers,
                platformModule.monitors.newMonitor( IndexingService.Monitor.class ), fileSystem,
                storeMigrationProcess, transactionMonitor, kernelHealth,
                platformModule.monitors.newMonitor( PhysicalLogFile.Monitor.class ),
                editionModule.headerInformationFactory, startupStatistics, nodeManager, guard, indexStore,
                editionModule.commitProcessFactory, platformModule.pageCache, platformModule.monitors, platformModule.tracers ));
        dataSourceManager.register( neoStoreDataSource );

        life.add( new MonitorGc( config, msgLog ) );

        life.add( nodeManager );

        availabilityGuard = new AvailabilityGuard( Clock.SYSTEM_CLOCK );
        life.add( new DatabaseAvailability( availabilityGuard, transactionMonitor ) );

        // Kernel event handlers should be the very last, i.e. very first to receive shutdown events
        life.add( kernelEventHandlers );

        final AtomicReference<QueryExecutionEngine> queryExecutor = new AtomicReference<>( QueryEngineProvider
                .noEngine() );

        dataSourceManager.addListener( new DataSourceManager.Listener()
        {
            private QueryExecutionEngine engine;

            @Override
            public void registered( NeoStoreDataSource dataSource )
            {
                if (engine == null)
                {
                    engine = QueryEngineProvider.initialize( platformModule.graphDatabaseFacade,
                                            dependencies.executionEngines() );

                    deps.satisfyDependency( engine );
                }

                queryExecutor.set( engine );
            }

            @Override
            public void unregistered( NeoStoreDataSource dataSource )
            {
                queryExecutor.set( QueryEngineProvider.noEngine() );
            }
        } );

        storeId = new Supplier<StoreId>()
        {
            @Override
            public StoreId get()
            {
                return neoStoreDataSource.getStoreId();
            }
        };

        kernelAPI = new Supplier<KernelAPI>()
        {
            @Override
            public KernelAPI get()
            {
                return neoStoreDataSource.getKernel();
            }
        };

        this.queryExecutor = new Supplier<QueryExecutionEngine>()
        {
            @Override
            public QueryExecutionEngine get()
            {
                return queryExecutor.get();
            }
        };
    }

    protected TransactionCounters createTransactionCounters()
    {
        return new TransactionCounters();
    }

    protected RelationshipProxy.RelationshipActions createRelationshipActions(final GraphDatabaseService graphDatabaseService,
                                                                              final ThreadToStatementContextBridge threadToStatementContextBridge,
                                                                              final NodeManager nodeManager, final RelationshipTypeTokenHolder relationshipTypeTokenHolder)
    {
        return new RelationshipProxy.RelationshipActions()
        {
            @Override
            public GraphDatabaseService getGraphDatabaseService()
            {
                return graphDatabaseService;
            }

            @Override
            public void failTransaction()
            {
                threadToStatementContextBridge.getKernelTransactionBoundToThisThread( true ).failure();
            }

            @Override
            public void assertInUnterminatedTransaction()
            {
                threadToStatementContextBridge.assertInUnterminatedTransaction();
            }

            @Override
            public Statement statement()
            {
                return threadToStatementContextBridge.get();
            }

            @Override
            public Node newNodeProxy( long nodeId )
            {
                // only used by relationship already checked as valid in cache
                return nodeManager.newNodeProxyById( nodeId );
            }

            @Override
            public RelationshipType getRelationshipTypeById( int type )
            {
                try
                {
                    return relationshipTypeTokenHolder.getTokenById( type );
                }
                catch ( TokenNotFoundException e )
                {
                    throw new NotFoundException( e );
                }
            }
        };
    }

    protected NodeProxy.NodeActions createNodeActions(final GraphDatabaseService graphDatabaseService,
                                                                                  final ThreadToStatementContextBridge threadToStatementContextBridge,
                                                                                  final NodeManager nodeManager)
    {
        return new NodeProxy.NodeActions()
        {
            @Override
            public Statement statement()
            {
                return threadToStatementContextBridge.get();
            }

            @Override
            public GraphDatabaseService getGraphDatabase()
            {
                // TODO This should be wrapped as well
                return graphDatabaseService;
            }

            @Override
            public void assertInUnterminatedTransaction()
            {
                threadToStatementContextBridge.assertInUnterminatedTransaction();
            }

            @Override
            public void failTransaction()
            {
                threadToStatementContextBridge.getKernelTransactionBoundToThisThread( true ).failure();
            }

            @Override
            public Relationship lazyRelationshipProxy( long id )
            {
                return nodeManager.newRelationshipProxyById( id );
            }

            @Override
            public Relationship newRelationshipProxy( long id )
            {
                return nodeManager.newRelationshipProxy( id );
            }

            @Override
            public Relationship newRelationshipProxy( long id, long startNodeId, int typeId, long endNodeId )
            {
                return nodeManager.newRelationshipProxy( id, startNodeId, typeId, endNodeId );
            }
        };
    }
}
