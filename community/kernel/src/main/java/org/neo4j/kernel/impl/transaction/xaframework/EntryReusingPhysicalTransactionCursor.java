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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryReader;

public class EntryReusingPhysicalTransactionCursor extends PhysicalTransactionCursor
{
    private final List<Command> entries = new ArrayList<>();

    public EntryReusingPhysicalTransactionCursor( ReadableLogChannel channel,
            LogEntryReader<ReadableLogChannel> entryReader)
    {
        super( channel, entryReader);
    }

    @Override
    protected List<Command> commandList()
    {
        entries.clear();
        return entries;
    }
}
