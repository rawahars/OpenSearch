/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.translog;

import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.io.Channels;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * A checkpoint for OpenSearch operations
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class Checkpoint {

    final long offset;
    final int numOps;
    final long generation;
    final long minSeqNo;
    final long maxSeqNo;
    final long globalCheckpoint;
    final long minTranslogGeneration;
    final long trimmedAboveSeqNo;
    // translogChecksum is not declared as final since the checksum for translog file is not available readily.
    // It is available once the TranslogCheckedContainer is initialised for the same.
    // Therefore, we will be setting it using a setter method.
    long translogChecksum;

    private static final int VERSION_LUCENE_BIG_ENDIAN = 3; // big endian format (Lucene 9+ switches to little endian)
    private static final int CURRENT_VERSION = 5; // introduction of translog checksum
    private static final String CHECKPOINT_CODEC = "ckp";

    static final int V4_FILE_SIZE = CodecUtil.headerLength(CHECKPOINT_CODEC) + Integer.BYTES  // ops
        + Long.BYTES // offset
        + Long.BYTES // generation
        + Long.BYTES // minimum sequence number
        + Long.BYTES // maximum sequence number
        + Long.BYTES // global checkpoint
        + Long.BYTES // minimum translog generation in the translog
        + Long.BYTES // maximum reachable (trimmed) sequence number
        + CodecUtil.footerLength();

    static final int V5_FILE_SIZE = V4_FILE_SIZE // Existing V4 File size
        + Long.BYTES; // translog checksum

    // CHECKPOINT_VERSION_MAP is a map of checkpoint versions and the corresponding size of the file for that version.
    private static final Map<Integer, Integer> SUPPORTED_CHECKPOINT_VERSION_MAP = new HashMap<>() {
        {
            put(3, null); // big endian format (Lucene 9+ switches to little endian)
            put(4, V4_FILE_SIZE); // introduction of trimmed above seq#
            put(5, V5_FILE_SIZE); // introduction of translog checksum
        }
    };

    // EMPTY_TRANSLOG_CHECKSUM is the checksum associated with empty translog or older unsupported versions.
    private static final long EMPTY_TRANSLOG_CHECKSUM = -1;

    /**
     * Create a new translog checkpoint.
     *
     * @param offset                the current offset in the translog
     * @param numOps                the current number of operations in the translog
     * @param generation            the current translog generation
     * @param minSeqNo              the current minimum sequence number of all operations in the translog
     * @param maxSeqNo              the current maximum sequence number of all operations in the translog
     * @param globalCheckpoint      the last-known global checkpoint
     * @param minTranslogGeneration the minimum generation referenced by the translog at this moment.
     * @param trimmedAboveSeqNo     all operations with seq# above trimmedAboveSeqNo should be ignored and not read from the
     *                              corresponding translog file. {@link SequenceNumbers#UNASSIGNED_SEQ_NO} is used to disable trimming.
     */
    Checkpoint(
        long offset,
        int numOps,
        long generation,
        long minSeqNo,
        long maxSeqNo,
        long globalCheckpoint,
        long minTranslogGeneration,
        long trimmedAboveSeqNo
    ) {
        assert minSeqNo <= maxSeqNo : "minSeqNo [" + minSeqNo + "] is higher than maxSeqNo [" + maxSeqNo + "]";
        assert trimmedAboveSeqNo <= maxSeqNo : "trimmedAboveSeqNo [" + trimmedAboveSeqNo + "] is higher than maxSeqNo [" + maxSeqNo + "]";
        assert minTranslogGeneration <= generation : "minTranslogGen ["
            + minTranslogGeneration
            + "] is higher than generation ["
            + generation
            + "]";
        this.offset = offset;
        this.numOps = numOps;
        this.generation = generation;
        this.minSeqNo = minSeqNo;
        this.maxSeqNo = maxSeqNo;
        this.globalCheckpoint = globalCheckpoint;
        this.minTranslogGeneration = minTranslogGeneration;
        this.trimmedAboveSeqNo = trimmedAboveSeqNo;
        this.translogChecksum = EMPTY_TRANSLOG_CHECKSUM;
    }

    private void write(DataOutput out) throws IOException {
        out.writeLong(offset);
        out.writeInt(numOps);
        out.writeLong(generation);
        out.writeLong(minSeqNo);
        out.writeLong(maxSeqNo);
        out.writeLong(globalCheckpoint);
        out.writeLong(minTranslogGeneration);
        out.writeLong(trimmedAboveSeqNo);
        out.writeLong(translogChecksum);
    }

    /**
     * Returns the maximum sequence number of operations in this checkpoint after applying {@link #trimmedAboveSeqNo}.
     */
    long maxEffectiveSeqNo() {
        if (trimmedAboveSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            return maxSeqNo;
        } else {
            return Math.min(trimmedAboveSeqNo, maxSeqNo);
        }
    }

    static Checkpoint emptyTranslogCheckpoint(
        final long offset,
        final long generation,
        final long globalCheckpoint,
        long minTranslogGeneration
    ) {
        final long minSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        final long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        final long trimmedAboveSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
        return new Checkpoint(offset, 0, generation, minSeqNo, maxSeqNo, globalCheckpoint, minTranslogGeneration, trimmedAboveSeqNo);
    }

    static Checkpoint readCheckpointForVersion(final DataInput in, final int version) throws IOException {
        switch (version) {
            case 3:
                return Checkpoint.readCheckpoint(EndiannessReverserUtil.wrapDataInput(in), version);
            case 4:
            case 5:
            default:
                return Checkpoint.readCheckpoint(in, version);
        }
    }

    static Checkpoint readCheckpoint(final DataInput in, final int version) throws IOException {
        final long offset = in.readLong();
        final int numOps = in.readInt();
        final long generation = in.readLong();
        final long minSeqNo = in.readLong();
        final long maxSeqNo = in.readLong();
        final long globalCheckpoint = in.readLong();
        final long minTranslogGeneration = in.readLong();
        final long trimmedAboveSeqNo = in.readLong();
        Checkpoint checkpoint = new Checkpoint(
            offset,
            numOps,
            generation,
            minSeqNo,
            maxSeqNo,
            globalCheckpoint,
            minTranslogGeneration,
            trimmedAboveSeqNo
        );

        if (version >= 5) {
            checkpoint.translogChecksum = in.readLong();
        }

        return checkpoint;
    }

    @Override
    public String toString() {
        return "Checkpoint{"
            + "offset="
            + offset
            + ", numOps="
            + numOps
            + ", generation="
            + generation
            + ", minSeqNo="
            + minSeqNo
            + ", maxSeqNo="
            + maxSeqNo
            + ", globalCheckpoint="
            + globalCheckpoint
            + ", minTranslogGeneration="
            + minTranslogGeneration
            + ", trimmedAboveSeqNo="
            + trimmedAboveSeqNo
            + ", translogChecksum="
            + translogChecksum
            + '}';
    }

    public static Checkpoint read(Path path) throws IOException {
        try (Directory dir = new NIOFSDirectory(path.getParent())) {
            try (IndexInput indexInput = dir.openInput(path.getFileName().toString(), IOContext.DEFAULT)) {
                // We checksum the entire file before we even go and parse it. If it's corrupted we barf right here.
                CodecUtil.checksumEntireFile(indexInput);
                final int fileVersion = CodecUtil.checkHeader(indexInput, CHECKPOINT_CODEC, VERSION_LUCENE_BIG_ENDIAN, CURRENT_VERSION);
                // Check if the file version of checkpoint is in our supported list.
                assert SUPPORTED_CHECKPOINT_VERSION_MAP.containsKey(fileVersion) : fileVersion;
                // If we need to compare the size of the file for the given version, we will assert the same.
                assert SUPPORTED_CHECKPOINT_VERSION_MAP.get(fileVersion) == null
                    || indexInput.length() == SUPPORTED_CHECKPOINT_VERSION_MAP.get(fileVersion) : indexInput.length();
                return Checkpoint.readCheckpointForVersion(indexInput, fileVersion);
            } catch (CorruptIndexException | NoSuchFileException | IndexFormatTooOldException | IndexFormatTooNewException e) {
                throw new TranslogCorruptedException(path.toString(), e);
            }
        }
    }

    public static void write(ChannelFactory factory, Path checkpointFile, Checkpoint checkpoint, OpenOption... options) throws IOException {
        byte[] bytes = createCheckpointBytes(checkpointFile, checkpoint);

        // now go and write to the channel, in one go.
        try (FileChannel channel = factory.open(checkpointFile, options)) {
            Channels.writeToChannel(bytes, channel);
            // force fsync with metadata since this is used on file creation
            channel.force(true);
        }
    }

    public static void write(FileChannel fileChannel, Path checkpointFile, Checkpoint checkpoint, boolean fsync) throws IOException {
        byte[] bytes = createCheckpointBytes(checkpointFile, checkpoint);
        Channels.writeToChannel(bytes, fileChannel, 0);
        if (fsync == true) {
            // no need to force metadata, file size stays the same and we did the full fsync
            // when we first created the file, so the directory entry doesn't change as well
            fileChannel.force(false);
        }
    }

    public static byte[] createCheckpointBytes(Path checkpointFile, Checkpoint checkpoint) throws IOException {
        final Integer CURRENT_VERSION_FILE_SIZE = SUPPORTED_CHECKPOINT_VERSION_MAP.get(CURRENT_VERSION);
        assert CURRENT_VERSION_FILE_SIZE != null : CURRENT_VERSION + " has file file size set as null";

        final ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream(CURRENT_VERSION_FILE_SIZE) {
            @Override
            public synchronized byte[] toByteArray() {
                // don't clone
                return buf;
            }
        };
        final String resourceDesc = "checkpoint(path=\"" + checkpointFile + "\", gen=" + checkpoint + ")";
        try (
            OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput(
                resourceDesc,
                checkpointFile.toString(),
                byteOutputStream,
                CURRENT_VERSION_FILE_SIZE
            )
        ) {
            CodecUtil.writeHeader(indexOutput, CHECKPOINT_CODEC, CURRENT_VERSION);
            checkpoint.write(indexOutput);
            CodecUtil.writeFooter(indexOutput);

            assert indexOutput.getFilePointer() == CURRENT_VERSION_FILE_SIZE : "get you numbers straight; bytes written: "
                + indexOutput.getFilePointer()
                + ", buffer size: "
                + CURRENT_VERSION_FILE_SIZE;
            assert indexOutput.getFilePointer() < 512 : "checkpoint files have to be smaller than 512 bytes for atomic writes; size: "
                + indexOutput.getFilePointer();
        }
        return byteOutputStream.toByteArray();
    }

    public long getMinTranslogGeneration() {
        return minTranslogGeneration;
    }

    public long getGeneration() {
        return generation;
    }

    public void setTranslogChecksum(final long translogChecksum) {
        this.translogChecksum = translogChecksum;
    }

    public long getTranslogChecksum() {
        return translogChecksum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Checkpoint that = (Checkpoint) o;

        if (offset != that.offset) return false;
        if (numOps != that.numOps) return false;
        if (generation != that.generation) return false;
        if (minSeqNo != that.minSeqNo) return false;
        if (maxSeqNo != that.maxSeqNo) return false;
        if (globalCheckpoint != that.globalCheckpoint) return false;
        if (trimmedAboveSeqNo != that.trimmedAboveSeqNo) return false;
        return translogChecksum == that.translogChecksum;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(offset);
        result = 31 * result + numOps;
        result = 31 * result + Long.hashCode(generation);
        result = 31 * result + Long.hashCode(minSeqNo);
        result = 31 * result + Long.hashCode(maxSeqNo);
        result = 31 * result + Long.hashCode(globalCheckpoint);
        result = 31 * result + Long.hashCode(trimmedAboveSeqNo);
        result = 31 * result + Long.hashCode(translogChecksum);
        return result;
    }

}
