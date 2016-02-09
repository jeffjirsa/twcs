/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jeffjirsa.cassandra.db.compaction.writers;

import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * CompactionAwareWriter not only buckets by time, but also cleans up non-owned partitions
 *
 */
public class CleaningTimeWindowCompactionWriter extends CompactionAwareWriter
{
    private static final Logger logger = LoggerFactory.getLogger(CleaningTimeWindowCompactionWriter.class);

    private final Set<SSTableReader> allSSTables;
    private long currentBytesToWrite;
    private int currentRatioIndex = 0;
    protected final LifecycleTransaction txn;
    protected final SSTableRewriter sstableWriter;
    protected List<Range<Token>> sortedRanges;
    protected final int nowInSec;


    public CleaningTimeWindowCompactionWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, int nowInSec)
    {
        this(cfs, directories, txn, nonExpiredSSTables, nowInSec, false, false);
    }

    @SuppressWarnings("resource")
    public CleaningTimeWindowCompactionWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, int nowInSec, boolean offline, boolean keepOriginals)
    {

        super(cfs, directories, txn, nonExpiredSSTables, offline, keepOriginals);
        this.allSSTables = txn.originals();
        this.txn = txn;
        this.sstableWriter = SSTableRewriter.constructKeepingOriginals(txn, keepOriginals, maxAge, offline);
        this.sortedRanges = Range.normalize(StorageService.instance.getLocalRanges(cfs.keyspace.getName()));
        this.nowInSec = nowInSec;
    }

    @Override
    public boolean realAppend(UnfilteredRowIterator partition)
    {
        if(Range.isInRanges(partition.partitionKey().getToken(), sortedRanges))
        {
            RowIndexEntry rie = sstableWriter.append(partition);
            return rie != null;
        }
        else
        {
            cfs.invalidateCachedPartition(partition.partitionKey());
            cfs.indexManager.deletePartition(partition, nowInSec);
            return false;
        }
    }

    @Override
    protected void switchCompactionLocation(Directories.DataDirectory directory)
    {
        @SuppressWarnings("resource")
        SSTableWriter writer = SSTableWriter.create(Descriptor.fromFilename(cfs.getSSTablePath(getDirectories().getLocationForDisk(directory))),
                estimatedTotalKeys,
                minRepairedAt,
                cfs.metadata,
                new MetadataCollector(txn.originals(), cfs.metadata.comparator, 0),
                SerializationHeader.make(cfs.metadata, nonExpiredSSTables),
                cfs.indexManager.listIndexes(),
                txn);
        sstableWriter.switchWriter(writer);
    }

    @Override
    public long estimatedKeys()
    {
        return estimatedTotalKeys;
    }


    @Override
    public List<SSTableReader> finish(long repairedAt)
    {
        return sstableWriter.setRepairedAt(repairedAt).finish();
    }

}