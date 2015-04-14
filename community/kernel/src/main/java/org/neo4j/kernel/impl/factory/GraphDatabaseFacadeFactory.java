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
import java.util.Map;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsExtractor;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.helpers.Settings.setting;

public abstract class GraphDatabaseFacadeFactory
{
    public interface Dependencies
    {
        /**
         * Allowed to be null. Null means that no external {@link org.neo4j.kernel.monitoring.Monitors} was created,
         * let the
         * database create its own monitors instance.
         */
        Monitors monitors();

        /**
         * Allowed to be null. Null means that no external {@link org.neo4j.kernel.logging.Logging} was created, let the
         * database create its own logging.
         */
        Logging logging();

        Iterable<Class<?>> settingsClasses();

        Iterable<KernelExtensionFactory<?>> kernelExtensions();

        Iterable<QueryEngineProvider> executionEngines();
    }

    public static class Configuration
    {
        public static final Setting<Boolean> read_only = GraphDatabaseSettings.read_only;
        public static final Setting<Boolean> execution_guard_enabled = GraphDatabaseSettings.execution_guard_enabled;
        public static final Setting<Boolean> ephemeral = setting( "ephemeral", Settings.BOOLEAN, Settings.FALSE );
        public static final Setting<File> store_dir = GraphDatabaseSettings.store_dir;
        public static final Setting<File> neo_store = GraphDatabaseSettings.neo_store;

        // Kept here to have it not be publicly documented.
        public static final Setting<String> lock_manager = setting( "lock_manager", Settings.STRING, "" );
        public static final Setting<String> tracer =
                setting( "dbms.tracer", Settings.STRING, (String) null ); // 'null' default.

        public static final Setting<String> log_configuration_file = setting( "log.configuration", Settings.STRING,
                "neo4j-logback.xml" );
        public static final Setting<String> pagecache_monitor =
                setting( "pagecache_monitor", Settings.STRING, (String) null ); // 'null' default.

        public static final Setting<String> editionName = setting("edition", Settings.STRING, "Community");
    }

    protected enum Diagnostics implements DiagnosticsExtractor<NeoStore>
    {
        NEO_STORE_VERSIONS( "Store versions:" )
                {
                    @Override
                    void dump( NeoStore source, StringLogger.LineLogger log )
                    {
                        source.logVersions( log );
                    }
                },
        NEO_STORE_ID_USAGE( "Id usage:" )
                {
                    @Override
                    void dump( NeoStore source, StringLogger.LineLogger log )
                    {
                        source.logIdUsage( log );
                    }
                };

        private final String message;

        private Diagnostics( String message )
        {
            this.message = message;
        }

        @Override
        public void dumpDiagnostics( final NeoStore source, DiagnosticsPhase phase, StringLogger log )
        {
            if ( applicable( phase ) )
            {
                log.logLongMessage( message, new Visitor<StringLogger.LineLogger,RuntimeException>()
                {
                    @Override
                    public boolean visit( StringLogger.LineLogger logger )
                    {
                        dump( source, logger );
                        return false;
                    }
                }, true );
            }
        }

        boolean applicable( DiagnosticsPhase phase )
        {
            return phase.isInitialization() || phase.isExplicitlyRequested();
        }

        abstract void dump( NeoStore source, StringLogger.LineLogger log );
    }

    public GraphDatabaseFacade newFacade( Map<String, String> params, final Dependencies dependencies )
    {
        return newFacade( params, dependencies, new GraphDatabaseFacade() );
    }

    public GraphDatabaseFacade newFacade( Map<String, String> params, final Dependencies dependencies,
                                          final GraphDatabaseFacade graphDatabaseFacade )
    {
        PlatformModule platform = createPlatform( params, dependencies, graphDatabaseFacade );
        EditionModule edition = createEdition( platform );
        final DataSourceModule dataSource = createDataSource( dependencies, platform, edition );

        // Start it
        graphDatabaseFacade.init( dataSource.threadToTransactionBridge, dataSource.nodeManager, dataSource.indexManager, dataSource.schema, dataSource.availabilityGuard,
                platform.logging.getMessagesLog( getClass() ), platform.life, dataSource.kernelAPI, dataSource.queryExecutor, dataSource.kernelEventHandlers,
                dataSource.transactionEventHandlers, edition.transactionStartTimeout, platform.dependencies, dataSource.storeId, platform.storeDir.getAbsolutePath() );

        boolean failed = false;
        try
        {
            enableAvailabilityLogging( dataSource.availabilityGuard, platform.logging.getMessagesLog( getClass() ) ); // Done after create to avoid a redundant
            // "database is now unavailable"

            platform.life.start();
        }
        catch ( final Throwable throwable )
        {
            failed = true;
            throw new RuntimeException( "Error starting " + getClass().getName() + ", "  + platform.storeDir.getAbsolutePath(),
                    throwable );
        }
        finally
        {
            if ( failed )
            {
                try
                {
                    graphDatabaseFacade.shutdown();
                }
                catch (Throwable ex)
                {
                    // Ignore
                }
            }
        }

        return graphDatabaseFacade;
    }

    protected PlatformModule createPlatform(Map<String, String> params, final Dependencies dependencies,
                                                  final GraphDatabaseFacade graphDatabaseFacade)
    {
        return new PlatformModule( params, dependencies, graphDatabaseFacade );
    }

    protected abstract EditionModule createEdition(PlatformModule platformModule);

    protected DataSourceModule createDataSource( final Dependencies dependencies, final PlatformModule platformModule, EditionModule editionModule)
    {
        return new DataSourceModule( dependencies, platformModule, editionModule );
    }

    private void enableAvailabilityLogging( AvailabilityGuard availabilityGuard, final StringLogger msgLog )
    {
        availabilityGuard.addListener( new AvailabilityGuard.AvailabilityListener()
        {
            @Override
            public void available()
            {
                msgLog.info( "Database is now ready" );
            }

            @Override
            public void unavailable()
            {
                msgLog.info( "Database is now unavailable" );
            }
        } );
    }
}
