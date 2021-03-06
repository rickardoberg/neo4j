/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl.standard;

import java.io.File;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapper;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SingleFilePageSwapperTest
{
    @Test
    public void shouldNotGoToDiskIfReadingPageBeyondFileSize() throws Exception
    {
        // Given
        ByteBuffer buffer = ByteBuffer.allocate( 64 );
        StoreChannel channel = mock( StoreChannel.class );
        when( channel.size() ).thenReturn( 128l );

        SingleFilePageSwapper io = new SingleFilePageSwapper( new File("SomeFile"), channel, 64, null );

        // When
        ByteBufferPage page = new ByteBufferPage( buffer );
        io.read( 16, page );
        io.read( 3, page );

        // Then
        verify( channel, times( 2 ) ).size();
        verifyNoMoreInteractions( channel );
    }

}
