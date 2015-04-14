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
import java.util.List;
import java.util.Map;

import org.neo4j.function.Supplier;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.StoreLocker;
import org.neo4j.kernel.StoreLockerLifecycleAdapter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.extension.UnsatisfiedDependencyStrategies;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.info.JvmChecker;
import org.neo4j.kernel.info.JvmMetadataRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.DefaultLogging;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.RollingLogMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.Tracers;

public class PlatformModule
{
    public PageCache pageCache;

    public Monitors monitors;

    public GraphDatabaseFacade graphDatabaseFacade;

    public org.neo4j.kernel.impl.util.Dependencies dependencies;

    public Logging logging;

    public LifeSupport life;

    public File storeDir;

    public DiagnosticsManager diagnosticsManager;

    public Tracers tracers;

    public Config config;

    public FileSystemAbstraction fileSystem;

    public DataSourceManager dataSourceManager;

    public KernelExtensions kernelExtensions;

    public JobScheduler jobScheduler;

    public PlatformModule(Map<String, String> params, final GraphDatabaseFacadeFactory.Dependencies externalDependencies,
                                                  final GraphDatabaseFacade graphDatabaseFacade)
    {
        dependencies = new org.neo4j.kernel.impl.util.Dependencies( new Supplier<DependencyResolver>()
        {
            @Override
            public DependencyResolver get()
            {
                return dataSourceManager.getDataSource().getDependencyResolver();
            }
        } );
        life = new LifeSupport();
        this.graphDatabaseFacade = dependencies.satisfyDependency(graphDatabaseFacade);

        // SPI - provided services
        config = dependencies.satisfyDependency( new Config( params, getSettingsClasses(
                externalDependencies.settingsClasses(), externalDependencies.kernelExtensions() ) ) );

        storeDir = config.get( GraphDatabaseFacadeFactory.Configuration.store_dir );

        kernelExtensions = dependencies.satisfyDependency( new KernelExtensions(
                externalDependencies.kernelExtensions(),
                config,
                dependencies,
                UnsatisfiedDependencyStrategies.fail() ) );


        fileSystem = life.add( dependencies.satisfyDependency( createFileSystemAbstraction() ) );

        // Component monitoring
        monitors = externalDependencies.monitors() == null ? new Monitors() : externalDependencies.monitors();
        dependencies.satisfyDependency( monitors );

        // If no logging was passed in from the outside then create logging and register
        // with this life
        logging = externalDependencies.logging() == null ? life.add( createLogging(  ) ) : externalDependencies.logging();
        dependencies.satisfyDependency( logging );

        StringLogger msgLog = logging.getMessagesLog( getClass() );

        config.setLogger( msgLog );

        StoreLockerLifecycleAdapter storeLocker = life.add( new StoreLockerLifecycleAdapter(
                new StoreLocker( fileSystem ), storeDir ) );

        new JvmChecker( msgLog, new JvmMetadataRepository() ).checkJvmCompatibilityAndIssueWarning();

        String desiredImplementationName = config.get( GraphDatabaseFacadeFactory.Configuration.tracer );
        tracers = dependencies.satisfyDependency( new Tracers( desiredImplementationName, msgLog ) );

        pageCache = dependencies.satisfyDependency( createPageCache( fileSystem, config, logging, tracers ) );
        life.add( pageCache );

        diagnosticsManager = life.add( dependencies.satisfyDependency( new DiagnosticsManager( logging.getMessagesLog(
                DiagnosticsManager.class ) ) ) );
        monitors.addMonitorListener( new RollingLogMonitor()
        {
            @Override
            public void rolledOver()
            {
                // Add diagnostics at the top of every log file
                diagnosticsManager.dumpAll();
            }
        } );

        // TODO please fix the bad dependencies instead of doing this. Before the removal of JTA
        // this was the place of the XaDataSourceManager. NeoStoreXaDataSource is create further down than
        // (specifically) KernelExtensions, which creates an interesting out-of-order issue with #doAfterRecovery().
        // Anyways please fix this.
        dataSourceManager = life.add( dependencies.satisfyDependency(new DataSourceManager() ));

        jobScheduler = life.add( dependencies.satisfyDependency(createJobScheduler() ));
    }

    protected FileSystemAbstraction createFileSystemAbstraction()
    {
        return new DefaultFileSystemAbstraction();
    }

    protected Logging createLogging(  )
    {
        return DefaultLogging.createDefaultLogging( dependencies.resolveDependency( Config.class ),
                dependencies.resolveDependency( Monitors.class ) );
    }

    protected Neo4jJobScheduler createJobScheduler()
    {
        return new Neo4jJobScheduler( config.get( GraphDatabaseFacadeFactory.Configuration.editionName ));
    }

    protected PageCache createPageCache( FileSystemAbstraction fileSystem, Config config, Logging logging, Tracers tracers)
    {
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fileSystem, config, tracers.pageCacheTracer );
        PageCache pageCache = pageCacheFactory.getOrCreatePageCache();

        if ( config.get( GraphDatabaseSettings.dump_configuration ) )
        {
            pageCacheFactory.dumpConfiguration( logging.getMessagesLog( PageCache.class ) );
        }
        return pageCache;
    }

    private Iterable<Class<?>> getSettingsClasses( Iterable<Class<?>> settingsClasses,
                                                   Iterable<KernelExtensionFactory<?>> kernelExtensions)
    {
        List<Class<?>> totalSettingsClasses = Iterables.toList( settingsClasses );

        // Get the list of settings classes for extensions
        for ( KernelExtensionFactory<?> kernelExtension : kernelExtensions )
        {
            if ( kernelExtension.getSettingsClass() != null )
            {
                totalSettingsClasses.add( kernelExtension.getSettingsClass() );
            }
        }

        return totalSettingsClasses;
    }
}
