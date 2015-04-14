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
package org.neo4j.kernel;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.neo4j.helpers.Format;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.info.DiagnosticsProvider;

/**
 * @deprecated This will be moved to internal packages in the next major release.
 */
@Deprecated
public abstract class KernelDiagnostics implements DiagnosticsProvider
{
    public static class Versions extends KernelDiagnostics
    {
        private final String edition;
        private final StoreId storeId;

        public Versions( String edition, StoreId storeId )
        {
            this.edition = edition;
            this.storeId = storeId;
        }

        @Override
        void dump( StringLogger logger )
        {
            logger.logMessage( "Graph Database: " + edition + " " + storeId );
            logger.logMessage( "Kernel version: " + Version.getKernel() );
            logger.logMessage( "Neo4j component versions:" );
            for ( Version componentVersion : Service.load( Version.class ) )
            {
                logger.logMessage( "  " + componentVersion + "; revision: " + componentVersion.getRevision() );
            }
        }
    }

    public static class StoreFiles extends KernelDiagnostics implements Visitor<StringLogger.LineLogger,
            RuntimeException>
    {
        private final File storeDir;
        private static String FORMAT_DATE_ISO = "yyyy-MM-dd'T'HH:mm:ssZ";
        final private SimpleDateFormat dateFormat;

        public StoreFiles( String storeDir )
        {
            this.storeDir = new File( storeDir );
            TimeZone tz = TimeZone.getDefault();
            dateFormat = new SimpleDateFormat( FORMAT_DATE_ISO );
            dateFormat.setTimeZone( tz );
        }

        @Override
        void dump( StringLogger logger )
        {
            logger.logLongMessage( getDiskSpace( storeDir ) + "\nStorage files: (filename : modification date - size)", this, true );
        }

        @Override
        public boolean visit( StringLogger.LineLogger logger )
        {
            logStoreFiles( logger, "  ", storeDir );
            return false;
        }

        private long logStoreFiles( StringLogger.LineLogger logger, String prefix, File dir )
        {
            if ( !dir.isDirectory() )
            {
                return 0;
            }
            File[] files = dir.listFiles();
            if ( files == null )
            {
                logger.logLine( prefix + "<INACCESSIBLE>" );
                return 0;
            }
            long total = 0;

            // Sort by name
            List<File> fileList = Arrays.asList( files );
            Collections.sort( fileList, new Comparator<File>()
            {
                @Override
                public int compare( File o1, File o2 )
                {
                    return o1.getName().compareTo( o2.getName() );
                }
            } );

            for ( File file : fileList )
            {
                long size;
                String filename = file.getName();
                if ( file.isDirectory() )
                {
                    logger.logLine( prefix + filename + ":" );
                    size = logStoreFiles( logger, prefix + "  ", file );
                    filename = "- Total";
                } else
                {
                    size = file.length();
                }

                String fileModificationDate = getFileModificationDate( file );
                String bytes = Format.bytes( size );
                String fileInformation = String.format( "%s%s: %s - %s", prefix, filename, fileModificationDate, bytes );
                logger.logLine( fileInformation );

                total += size;
            }
            return total;
        }

        private String getFileModificationDate( File file )
        {
            Date modifiedDate = new Date( file.lastModified() );
            return dateFormat.format( modifiedDate );
        }

        private String getDiskSpace( File storeDir )
        {
            long free = storeDir.getFreeSpace();
            long total = storeDir.getTotalSpace();
            long percentage = total != 0 ? (free * 100 / total) : 0;
            return String.format( "Disk space on partition (Total / Free / Free %%): %s / %s / %s", total, free, percentage );
        }
    }


    @Override
    public String getDiagnosticsIdentifier()
    {
        return getClass().getDeclaringClass().getSimpleName() + ":" + getClass().getSimpleName();
    }

    @Override
    public void acceptDiagnosticsVisitor( Object visitor )
    {
        // nothing visits ConfigurationLogging
    }

    @Override
    public void dump( DiagnosticsPhase phase, StringLogger log )
    {
        if ( phase.isInitialization() || phase.isExplicitlyRequested() )
        {
            dump( log );
        }
    }

    abstract void dump( StringLogger logger );
}
