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

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.Version;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.ReadOnlyTransactionCommitProcess;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.core.DefaultLabelIdCreator;
import org.neo4j.kernel.impl.core.DefaultPropertyTokenCreator;
import org.neo4j.kernel.impl.core.DefaultRelationshipTypeCreator;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.ReadOnlyTokenCreator;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.TokenCreator;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.locking.community.CommunityLockManger;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.storemigration.ConfigMapUpgradeConfiguration;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.transaction.state.NeoStoreInjectedTransactionValidator;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleListener;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.kernel.logging.DefaultLogging;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

public class CommunityEditionModule
    extends EditionModule
{
    public CommunityEditionModule( PlatformModule platformModule )
    {
        org.neo4j.kernel.impl.util.Dependencies deps = platformModule.dependencies;
        Config config = platformModule.config;
        Logging logging = platformModule.logging;
        FileSystemAbstraction fileSystem = platformModule.fileSystem;
        DataSourceManager dataSourceManager = platformModule.dataSourceManager;
        LifeSupport life = platformModule.life;
        GraphDatabaseFacade graphDatabaseFacade = platformModule.graphDatabaseFacade;

        lockManager = deps.satisfyDependency( createLockManager( config, logging ) );

        idGeneratorFactory = deps.satisfyDependency( createIdGeneratorFactory() );

        propertyKeyTokenHolder = life.add( deps.satisfyDependency( new PropertyKeyTokenHolder(
                createPropertyKeyCreator( config, dataSourceManager, idGeneratorFactory ) ) ));
        labelTokenHolder = life.add( deps.satisfyDependency(new LabelTokenHolder( createLabelIdCreator( config,
                dataSourceManager, idGeneratorFactory ) ) ));
        relationshipTypeTokenHolder = life.add( deps.satisfyDependency(new RelationshipTypeTokenHolder(
                createRelationshipTypeCreator( config, dataSourceManager, idGeneratorFactory ) ) ));

        life.add( deps.satisfyDependency(createKernelData( config, graphDatabaseFacade ) ));

        commitProcessFactory = new CommitProcessFactory()
            {
                @Override
                public TransactionCommitProcess create( LogicalTransactionStore logicalTransactionStore,
                                                        KernelHealth kernelHealth, NeoStore neoStore,
                                                        TransactionRepresentationStoreApplier storeApplier,
                                                        NeoStoreInjectedTransactionValidator txValidator,
                                                        IndexUpdatesValidator indexUpdatesValidator,
                                                        TransactionApplicationMode mode, Config config )
                {
                    if ( config.get( GraphDatabaseSettings.read_only ) )
                    {
                        return new ReadOnlyTransactionCommitProcess();
                    }
                    else
                    {
                        return new TransactionRepresentationCommitProcess( logicalTransactionStore, kernelHealth,
                                neoStore, storeApplier, indexUpdatesValidator, mode );
                    }
                }
            };

        headerInformationFactory = createHeaderInformationFactory();

        schemaWriteGuard = new SchemaWriteGuard()
        {
            @Override
            public void assertSchemaWritesAllowed() throws InvalidTransactionTypeKernelException
            {

            }
        };

        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout );

        upgradeConfiguration = new ConfigMapUpgradeConfiguration( config );

        registerRecovery( config.get( GraphDatabaseFacadeFactory.Configuration.editionName), life, deps );
    }


    protected TokenCreator createRelationshipTypeCreator( Config config, DataSourceManager dataSourceManager,
                                                          IdGeneratorFactory idGeneratorFactory )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            return new DefaultRelationshipTypeCreator( dataSourceManager, idGeneratorFactory );
        }
    }

    protected TokenCreator createPropertyKeyCreator( Config config, DataSourceManager dataSourceManager,
                                                     IdGeneratorFactory idGeneratorFactory )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            return new DefaultPropertyTokenCreator( dataSourceManager, idGeneratorFactory );
        }
    }

    protected TokenCreator createLabelIdCreator( Config config, DataSourceManager dataSourceManager,
                                                 IdGeneratorFactory idGeneratorFactory )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            return new DefaultLabelIdCreator( dataSourceManager, idGeneratorFactory );
        }
    }

    protected KernelData createKernelData( Config config, GraphDatabaseAPI graphAPI )
    {
        return new DefaultKernelData( config, graphAPI );
    }

    protected IdGeneratorFactory createIdGeneratorFactory()
    {
        return new DefaultIdGeneratorFactory();
    }

    private Locks createLockManager( Config config, Logging logging )
    {
        String key = config.get( GraphDatabaseFacadeFactory.Configuration.lock_manager );
        for ( Locks.Factory candidate : Service.load( Locks.Factory.class ) )
        {
            String candidateId = candidate.getKeys().iterator().next();
            if ( candidateId.equals( key ) )
            {
                return candidate.newInstance( ResourceTypes.values() );
            }
            else if ( key.equals( "" ) )
            {
                logging.getMessagesLog( CommunityFacadeFactory.class )
                        .info( "No locking implementation specified, defaulting to '" + candidateId + "'" );
                return candidate.newInstance( ResourceTypes.values() );
            }
        }

        if ( key.equals( "community" ) )
        {
            return new CommunityLockManger();
        }
        else if ( key.equals( "" ) )
        {
            logging.getMessagesLog( CommunityFacadeFactory.class )
                    .info( "No locking implementation specified, defaulting to 'community'" );
            return new CommunityLockManger();
        }

        throw new IllegalArgumentException( "No lock manager found with the name '" + key + "'." );
    }

    protected Logging createLogging( DependencyResolver resolver )
    {
        return DefaultLogging.createDefaultLogging( resolver.resolveDependency( Config.class ),
                resolver.resolveDependency( Monitors.class ) );
    }

    protected TransactionHeaderInformationFactory createHeaderInformationFactory()
    {
        return TransactionHeaderInformationFactory.DEFAULT;
    }

    protected void registerRecovery( final String editionName, LifeSupport life,
                                     final DependencyResolver dependencyResolver )
    {
        life.addLifecycleListener( new LifecycleListener()
        {
            @Override
            public void notifyStatusChanged( Object instance, LifecycleStatus from, LifecycleStatus to )
            {
                if ( instance instanceof DatabaseAvailability && to.equals( LifecycleStatus.STARTED ) )
                {
                    doAfterRecoveryAndStartup( editionName, dependencyResolver );
                }
            }
        } );
    }

    protected final class DefaultKernelData extends KernelData implements Lifecycle
    {
        private final GraphDatabaseAPI graphDb;

        public DefaultKernelData( Config config, GraphDatabaseAPI graphDb )
        {
            super( config );
            this.graphDb = graphDb;
        }

        @Override
        public Version version()
        {
            return Version.getKernel();
        }

        @Override
        public GraphDatabaseAPI graphDatabase()
        {
            return graphDb;
        }

        @Override
        public void init() throws Throwable
        {
        }

        @Override
        public void start() throws Throwable
        {
        }

        @Override
        public void stop() throws Throwable
        {
        }
    }
}
