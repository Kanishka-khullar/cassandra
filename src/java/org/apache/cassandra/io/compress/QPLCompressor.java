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

package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.intel.qpl.QPLException;
import com.intel.qpl.QPLJob;
import com.intel.qpl.QPLUtils;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.exceptions.ConfigurationException;

public class QPLCompressor implements ICompressor {
    private static final Logger logger = LoggerFactory.getLogger(QPLCompressor.class);
    private static final Set<String> VALID_EXECUTION_PATH = new HashSet<>(Arrays.asList("auto", "hardware", "software"));
    private static final Map<String, QPLUtils.ExecutionPaths> executionPathMap = createMap();

    public static final String DEFAULT_EXECUTION_PATH = "hardware";
    public static final int DEFAULT_COMPRESSION_LEVEL = 1;
    public static final int DEFAULT_RETRY_COUNT = 0;

    //If hardware path is not available then redirect it to software path
    public static final String REDIRECT_EXECUTION_PATH = "software";

    //Configuarable parameters name
    public static final String QPL_EXECUTION_PATH = "execution_path";
    public static final String QPL_COMPRESSOR_LEVEL = "compressor_level";
    public static final String QPL_RETRY_COUNT = "retry_count";
    private static final Cleaner qplJobCleaner = Cleaner.create();

    private final Set<Uses> recommendedUses;

    public int initialCompressedBufferLength(int chunkLength) {
        return chunkLength + (chunkLength >> 12) + (chunkLength >> 14) + (chunkLength >> 25) + 13;
    }

    public static QPLCompressor create(Map<String, String> options) {
        return new QPLCompressor(options);
    }

    @VisibleForTesting
    String executionPath;
    @VisibleForTesting
    Integer compressionLevel;
    @VisibleForTesting
    Integer retryCount;
    private final FastThreadLocal<QPLJob> reusableJob;

    private QPLCompressor(Map<String, String> options) {
        this.executionPath = validateExecutionPath(options);
        this.compressionLevel = validateCompressionLevel(options, this.executionPath);
        this.retryCount = validateRetryCount(options);
        reusableJob = new FastThreadLocal<QPLJob>() {
            @Override
            protected QPLJob initialValue() {
                QPLJob job = new QPLJob(executionPathMap.get(executionPath));
                job.setCompressionLevel(compressionLevel);
                job.setRetryCount(retryCount);
                Runnable runnable = job.cleaningAction();
                qplJobCleaner.register(job, runnable);
                return job;
            }
        };
        recommendedUses = ImmutableSet.of(Uses.GENERAL);
        logger.trace("Creating QPLCompressor with execution path {}  and compression level {} add retry_count {}.", this.executionPath, this.compressionLevel, this.retryCount);
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException {
        try {
            if (inputLength > 0) {
                QPLJob qplJob = reusableJob.get();
                qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
                qplJob.setFlags(QPLUtils.Flags.QPL_FLAG_FIRST.getId() | QPLUtils.Flags.QPL_FLAG_LAST.getId());
                return qplJob.execute(input, inputOffset, inputLength, output, outputOffset, output.length - outputOffset);
            }
        } catch (QPLException e) {
            throw new IOException(e);
        }
        return 0;
    }

    public void compress(ByteBuffer input, ByteBuffer output) throws IOException {
        try {
            if (input.hasRemaining()) {
                QPLJob qplJob = reusableJob.get();
                qplJob.setOperationType(QPLUtils.Operations.QPL_OP_COMPRESS);
                qplJob.setFlags(QPLUtils.Flags.QPL_FLAG_FIRST.getId() | QPLUtils.Flags.QPL_FLAG_LAST.getId() | QPLUtils.Flags.QPL_FLAG_DYNAMIC_HUFFMAN.getId()| QPLUtils.Flags.QPL_FLAG_OMIT_VERIFY.getId());
                qplJob.execute(input, output);
            }
        } catch (QPLException e) {
            throw new IOException(e);
        }
    }

    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException {
        try {
            if (input.hasRemaining()) {
                QPLJob qplJob = reusableJob.get();
                qplJob.setOperationType(QPLUtils.Operations.QPL_OP_DECOMPRESS);
                qplJob.setFlags(QPLUtils.Flags.QPL_FLAG_FIRST.getId() | QPLUtils.Flags.QPL_FLAG_LAST.getId());
                qplJob.execute(input, output);
            }
        } catch (QPLException e) {
            throw new IOException(e);
        }
    }

    public BufferType preferredBufferType() {
        return BufferType.OFF_HEAP;
    }

    public boolean supports(BufferType bufferType) {
        return bufferType == BufferType.OFF_HEAP;
    }

    public Set<String> supportedOptions() {
        return new HashSet<>(Arrays.asList(QPL_EXECUTION_PATH, QPL_COMPRESSOR_LEVEL, QPL_RETRY_COUNT));
    }

    @Override
    public Set<Uses> recommendedUses() {
        return recommendedUses;
    }

    public static String validateExecutionPath(Map<String, String> options) throws ConfigurationException {
        String execPath;
        if (options.get(QPL_EXECUTION_PATH) == null) {
            execPath = DEFAULT_EXECUTION_PATH;
            String validExecPath = QPLJob.getValidExecutionPath(executionPathMap.get(execPath)).name();
            if (!QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE.name().equalsIgnoreCase(validExecPath)) {
                logger.warn("The execution path 'hardware' is not available hence default it to software.");
                execPath = REDIRECT_EXECUTION_PATH;
            }
            return execPath;
        }

        execPath = options.get(QPL_EXECUTION_PATH);

        if (!VALID_EXECUTION_PATH.contains(execPath)) {
            throw new ConfigurationException(String.format("Invalid execution path '%s' specified for QPLCompressor parameter '%s'. "
                                                           + "Valid options are %s.", execPath, QPL_EXECUTION_PATH,
                                                           VALID_EXECUTION_PATH));
        } else {
            String validPath = QPLJob.getValidExecutionPath(executionPathMap.get(execPath)).name();
            if (!validPath.equalsIgnoreCase(executionPathMap.get(execPath).name())) {
                logger.warn("The execution path '{}' is not available hence default it to software.", execPath);
                execPath = REDIRECT_EXECUTION_PATH;
            }
            return execPath;
        }
    }

    public static int validateCompressionLevel(Map<String, String> options, String execPath) throws ConfigurationException {
        if (options.get(QPL_COMPRESSOR_LEVEL) == null)
            return DEFAULT_COMPRESSION_LEVEL;

        String compressionLevel = options.get(QPL_COMPRESSOR_LEVEL);

        ConfigurationException ex = new ConfigurationException("Invalid value [" + compressionLevel + "] for parameter '"
                                                               + QPL_COMPRESSOR_LEVEL + "'.");

        int level;
        try {
            level = Integer.parseInt(compressionLevel);
            if (level < 0)
                throw ex;
        } catch (NumberFormatException e) {
            throw ex;
        }

        int validLevel = QPLJob.getValidCompressionLevel(executionPathMap.get(execPath), level);
        if (level != validLevel) {
            logger.warn("The compression level '{}' is not supported for {} execution path hence its default to {}.", level, execPath, validLevel);
        }
        return validLevel;
    }

    public static int validateRetryCount(Map<String, String> options) throws ConfigurationException {
        if (options.get(QPL_RETRY_COUNT) == null)
            return DEFAULT_RETRY_COUNT;

        String retryCount = options.get(QPL_RETRY_COUNT);

        ConfigurationException ex = new ConfigurationException("Invalid value [" + retryCount + "] for parameter '"
                                                               + QPL_RETRY_COUNT + "'. Value must be >= 0.");

        int count;
        try {
            count = Integer.parseInt(retryCount);
            if (count < 0) {
                throw ex;
            }
        } catch (NumberFormatException e) {
            throw ex;
        }

        return count;
    }

    private static Map<String, QPLUtils.ExecutionPaths> createMap() {
        return ImmutableMap.of("auto", QPLUtils.ExecutionPaths.QPL_PATH_AUTO, "hardware", QPLUtils.ExecutionPaths.QPL_PATH_HARDWARE, "software", QPLUtils.ExecutionPaths.QPL_PATH_SOFTWARE);
    }
}