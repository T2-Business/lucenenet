using Lucene.Net.Diagnostics;
using Lucene.Net.Support;
using Lucene.Net.Support.Threading;
using Lucene.Net.Util;
using System;
using System.IO;
using System.Runtime.CompilerServices;

namespace Lucene.Net.Store
{
    /*
     * Licensed to the Apache Software Foundation (ASF) under one or more
     * contributor license agreements.  See the NOTICE file distributed with
     * this work for additional information regarding copyright ownership.
     * The ASF licenses this file to You under the Apache License, Version 2.0
     * (the "License"); you may not use this file except in compliance with
     * the License.  You may obtain a copy of the License at
     *
     *     http://www.apache.org/licenses/LICENSE-2.0
     *
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */

    /// <summary>
    /// A straightforward implementation of <see cref="FSDirectory"/>
    /// using <see cref="FileStream"/>.
    /// <para/>
    /// <see cref="FSDirectory"/> is ideal for use cases where efficient
    /// writing is required without utilizing too much RAM. However, reading
    /// is less efficient than when using <see cref="MMapDirectory"/>.
    /// This class has poor concurrent read performance (multiple threads will
    /// bottleneck) as it synchronizes when multiple threads
    /// read from the same file. It's usually better to use
    /// <see cref="MMapDirectory"/> for reading.
    /// <para/>
    /// <font color="red"><b>NOTE:</b> Unlike in Java, it is not recommended to use
    /// <see cref="System.Threading.Thread.Interrupt()"/> in .NET
    /// in conjunction with an open <see cref="FSDirectory"/> because it is not guaranteed to exit atomically.
    /// Any <c>lock</c> statement or <see cref="System.Threading.Monitor.Enter(object)"/> call can throw a
    /// <see cref="System.Threading.ThreadInterruptedException"/>, which makes shutting down unpredictable.
    /// To exit parallel tasks safely, we recommend using <see cref="System.Threading.Tasks.Task"/>s
    /// and "interrupt" them with <see cref="System.Threading.CancellationToken"/>s.</font>
    /// </summary>
    public class EncryptedSimpleFSDirectory : FSDirectory
    {
        protected readonly byte[] m_key;
        /// <summary>
        /// Create a new <see cref="SimpleFSDirectory"/> for the named location.
        /// </summary>
        /// <param name="path"> the path of the directory </param>
        /// <param name="lockFactory"> the lock factory to use, or null for the default
        /// (<see cref="NativeFSLockFactory"/>); </param>
        /// <exception cref="IOException"> if there is a low-level I/O error </exception>
        public EncryptedSimpleFSDirectory(DirectoryInfo path, LockFactory lockFactory, byte[] key)
            : base(path, lockFactory)
        {
            m_key = key;
        }

        /// <summary>
        /// Create a new <see cref="SimpleFSDirectory"/> for the named location and <see cref="NativeFSLockFactory"/>.
        /// </summary>
        /// <param name="path"> the path of the directory </param>
        /// <exception cref="IOException"> if there is a low-level I/O error </exception>
        public EncryptedSimpleFSDirectory(DirectoryInfo path, byte[] key)
            : base(path, null)
        {
            m_key = key;
        }

        /// <summary>
        /// Create a new <see cref="SimpleFSDirectory"/> for the named location.
        /// <para/>
        /// LUCENENET specific overload for convenience using string instead of <see cref="DirectoryInfo"/>.
        /// </summary>
        /// <param name="path"> the path of the directory </param>
        /// <param name="lockFactory"> the lock factory to use, or null for the default
        /// (<see cref="NativeFSLockFactory"/>); </param>
        /// <exception cref="IOException"> if there is a low-level I/O error </exception>
        public EncryptedSimpleFSDirectory(string path, LockFactory lockFactory, byte[] key)
            : this(new DirectoryInfo(path), lockFactory, key)
        {
        }

        /// <summary>
        /// Create a new <see cref="SimpleFSDirectory"/> for the named location and <see cref="NativeFSLockFactory"/>.
        /// <para/>
        /// LUCENENET specific overload for convenience using string instead of <see cref="DirectoryInfo"/>.
        /// </summary>
        /// <param name="path"> the path of the directory </param>
        /// <exception cref="IOException"> if there is a low-level I/O error </exception>
        public EncryptedSimpleFSDirectory(string path, byte[] key)
            : this(path, null, key)
        {
        }


        /// <summary>
        /// Creates an <see cref="IndexOutput"/> for the file with the given name. </summary>
        public override IndexOutput CreateOutput(string name, IOContext context)
        {
            EnsureOpen();

            EnsureCanWrite(name);
            return new EncryptedFSIndexOutput(this, name, m_key);
        }
        /// <summary>
        /// Creates an <see cref="IndexInput"/> for the file with the given name. </summary>
        public override IndexInput OpenInput(string name, IOContext context)
        {
            EnsureOpen();
            var path = new FileInfo(Path.Combine(Directory.FullName, name));
            var raf = new FileStream(path.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            return new EncryptedIndexInput("EncryptedIndexInput(path=\"" + path.FullName + "\")", raf, context, m_key);
        }

        public override IndexInputSlicer CreateSlicer(string name, IOContext context)
        {
            EnsureOpen();
            var file = new FileInfo(Path.Combine(Directory.FullName, name));
            var descriptor = new FileStream(file.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            return new IndexInputSlicerAnonymousClass(context, file, descriptor, m_key);
        }

        private class IndexInputSlicerAnonymousClass : IndexInputSlicer
        {
            private readonly IOContext context;
            private readonly FileInfo file;
            private readonly FileStream descriptor;
            private readonly byte[] m_key;

            public IndexInputSlicerAnonymousClass(IOContext context, FileInfo file, FileStream descriptor, byte[] key)
            {
                this.m_key = key;
                this.context = context;
                this.file = file;
                this.descriptor = descriptor;
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    descriptor.Dispose();
                }
            }

            public override IndexInput OpenSlice(string sliceDescription, long offset, long length)
            {
                return new EncryptedIndexInput("EncryptedIndexInput(" + sliceDescription + " in path=\"" + file.FullName + "\" slice=" + offset + ":" + (offset + length) + ")", descriptor, offset, length, BufferedIndexInput.GetBufferSize(context), m_key);
            }

            [Obsolete("Only for reading CFS files from 3.x indexes.")]
            public override IndexInput OpenFullSlice()
            {
                try
                {
                    return OpenSlice("full-slice", 0, descriptor.Length);
                }
                catch (Exception ex) when (ex.IsIOException())
                {
                    throw RuntimeException.Create(ex);
                }
            }
        }

        /// <summary>
        /// Reads bytes with <see cref="FileStream.Seek(long, SeekOrigin)"/> followed by
        /// <see cref="FileStream.Read(byte[], int, int)"/>.
        /// </summary>
        protected internal class EncryptedIndexInput : BufferedIndexInput
        {
            // LUCENENET specific: chunk size not needed
            ///// <summary>
            ///// The maximum chunk size is 8192 bytes, because <seealso cref="RandomAccessFile"/> mallocs
            ///// a native buffer outside of stack if the read buffer size is larger.
            ///// </summary>
            //private const int CHUNK_SIZE = 8192;

            /// <summary>
            /// the file channel we will read from </summary>
            protected internal readonly FileStream m_file;

            /// <summary>
            /// is this instance a clone and hence does not own the file to close it </summary>
            public bool IsClone { get; set; }

            /// <summary>
            /// start offset: non-zero in the slice case </summary>
            protected internal readonly long m_off;

            /// <summary>
            /// end offset (start+length) </summary>
            protected internal readonly long m_end;

            protected readonly byte[] m_key;

            public EncryptedIndexInput(string resourceDesc, FileStream file, IOContext context, byte[] key)
                : base(resourceDesc, context)
            {
                this.m_key = key;
                this.m_file = file;
                this.m_off = 0L;
                this.m_end = file.Length;
                this.IsClone = false;
            }

            public EncryptedIndexInput(string resourceDesc, FileStream file, long off, long length, int bufferSize, byte[] key)
                : base(resourceDesc, bufferSize)
            {
                this.m_key = key;
                this.m_file = file;
                this.m_off = off;
                this.m_end = off + length;
                this.IsClone = true;
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing && !IsClone)
                {
                    m_file.Dispose();
                }
            }

            public override object Clone()
            {

                EncryptedIndexInput clone = (EncryptedIndexInput)base.Clone();
                clone.IsClone = true;
                return clone;
            }

            public override sealed long Length => m_end - m_off;

            /// <summary>
            /// <see cref="IndexInput"/> methods </summary>
            protected override void ReadInternal(byte[] b, int offset, int len)
            {
                UninterruptableMonitor.Enter(m_file);
                try
                {
                    long position = m_off + Position; // LUCENENET specific: Renamed from getFilePointer() to match FileStream
                    m_file.Seek(position, SeekOrigin.Begin);
                    int total = 0;

                    if (position + len > m_end)
                    {
                        throw EOFException.Create("read past EOF: " + this);
                    }

                    try
                    {
                        //while (total < len)
                        //{
                        //    int toRead = Math.Min(CHUNK_SIZE, len - total);
                        //    int i = m_file.Read(b, offset + total, toRead);
                        //    if (i < 0) // be defensive here, even though we checked before hand, something could have changed
                        //    {
                        //        throw EOFException.Create("read past EOF: " + this + " off: " + offset + " len: " + len + " total: " + total + " chunkLen: " + toRead + " end: " + m_end);
                        //    }
                        //    if (Debugging.AssertsEnabled) Debugging.Assert(i > 0, "RandomAccessFile.read with non zero-length toRead must always read at least one byte");
                        //    total += i;
                        //}

                        // LUCENENET specific: FileStream is already optimized to read natively
                        // using the buffer size that is passed through its constructor. So,
                        // all we need to do is Read().
                        total = m_file.Read(b, offset, len);
                        for (long i = 0; i < total; ++i)
                        {
                            var positionor = i + position;
                            var key_indexor = positionor % m_key.Length;
                            b[i + offset] ^= m_key[(int)key_indexor];
                            //b[i + offset] ^= m_key[0];
                        }

                        if (Debugging.AssertsEnabled) Debugging.Assert(total == len);
                    }
                    catch (Exception ioe) when (ioe.IsIOException())
                    {
                        throw new IOException(ioe.Message + ": " + this, ioe);
                    }
                }
                finally
                {
                    UninterruptableMonitor.Exit(m_file);
                }
            }

            protected override void SeekInternal(long position)
            {
            }

            public virtual bool IsFDValid => m_file != null;
        }

        protected class EncryptedFSIndexOutput : BufferedIndexOutput
        {
            private const int CHUNK_SIZE = DEFAULT_BUFFER_SIZE;

            private readonly EncryptedSimpleFSDirectory parent;
            internal readonly string name;
            internal readonly byte[] m_key;
#pragma warning disable CA2213 // Disposable fields should be disposed
            private readonly FileStream file;
#pragma warning restore CA2213 // Disposable fields should be disposed
            private volatile bool isOpen; // remember if the file is open, so that we don't try to close it more than once
            private readonly CRC32 crc = new CRC32();

            public EncryptedFSIndexOutput(EncryptedSimpleFSDirectory parent, string name, byte[] key)
                : base(CHUNK_SIZE, null)
            {
                m_key = key;
                this.parent = parent;
                this.name = name;
                file = new FileStream(
                    path: Path.Combine(parent.m_directory.FullName, name),
                    mode: FileMode.OpenOrCreate,
                    access: FileAccess.Write,
                    share: FileShare.ReadWrite,
                    bufferSize: CHUNK_SIZE);
                isOpen = true;
            }


            protected byte[] EncryptBuffer(byte[] b, int offset, int length)
            {
                
                var encryptedBuffer = new byte[length];
                for (int i = 0; i < length; ++i)
                {
                    var positionor = file.Position + i;
                    var key_indexor = positionor % m_key.Length;
                    encryptedBuffer[i] = (byte)(b[i + offset] ^ m_key[(int)key_indexor]);
                    //encryptedBuffer[i] = (byte)(b[i+offset] ^ m_key[0]);
                }
                return encryptedBuffer;
                
            }
            void WriteEncryptedBuffer(byte[] b, int offset, int length)
            {
                var encryptedBuffer = EncryptBuffer(b, offset, length);
                file.Write(encryptedBuffer, 0, length);
            }

            /// <inheritdoc/>
            public override void WriteByte(byte b)
            {
                // LUCENENET specific: Guard to ensure we aren't disposed.
                if (!isOpen)
                    throw AlreadyClosedException.Create(this.GetType().FullName, "This FSIndexOutput is disposed.");

                crc.Update(b);
                b ^= m_key[(int)file.Position % m_key.Length];
                //b ^= m_key[0];
                file.WriteByte(b);
            }

            /// <inheritdoc/>
            public override void WriteBytes(byte[] b, int offset, int length)
            {
                // LUCENENET specific: Guard to ensure we aren't disposed.
                if (!isOpen)
                    throw AlreadyClosedException.Create(this.GetType().FullName, "This FSIndexOutput is disposed.");

                crc.Update(b, offset, length);
                WriteEncryptedBuffer(b, offset, length);
            }

            /// <inheritdoc/>
            protected internal override void FlushBuffer(byte[] b, int offset, int size)
            {
                // LUCENENET specific: Guard to ensure we aren't disposed.
                if (!isOpen)
                    throw AlreadyClosedException.Create(this.GetType().FullName, "This FSIndexOutput is disposed.");

                crc.Update(b, offset, size);
                WriteEncryptedBuffer(b, offset, size);
            }

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.NoInlining)]
            public override void Flush()
            {
                // LUCENENET specific: Guard to ensure we aren't disposed.
                if (!isOpen)
                    throw AlreadyClosedException.Create(this.GetType().FullName, "This FSIndexOutput is disposed.");

                file.Flush();
            }

            /// <inheritdoc/>
            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    //parent.OnIndexOutputClosed(this);
                    // only close the file if it has not been closed yet
                    if (isOpen)
                    {
                        Exception priorE = null; // LUCENENET: No need to cast to IOExcpetion
                        try
                        {
                            file.Flush(flushToDisk: true);
                        }
                        catch (Exception ioe) when (ioe.IsIOException())
                        {
                            priorE = ioe;
                        }
                        finally
                        {
                            isOpen = false;
                            IOUtils.DisposeWhileHandlingException(priorE, file);
                        }
                    }
                }
            }

            /// <summary>
            /// Random-access methods </summary>
            [Obsolete("(4.1) this method will be removed in Lucene 5.0")]
            public override void Seek(long pos)
            {
                // LUCENENET specific: Guard to ensure we aren't disposed.
                if (!isOpen)
                    throw AlreadyClosedException.Create(this.GetType().FullName, "This FSIndexOutput is disposed.");

                file.Seek(pos, SeekOrigin.Begin);
            }

            /// <inheritdoc/>
            public override long Length => file.Length;

            // LUCENENET NOTE: FileStream doesn't have a way to set length

            /// <inheritdoc/>
            public override long Checksum => crc.Value; // LUCENENET specific - need to override, since we are buffering locally

            /// <inheritdoc/>
            public override long Position => file.Position; // LUCENENET specific - need to override, since we are buffering locally, renamed from getFilePointer() to match FileStream
        }

    }
}