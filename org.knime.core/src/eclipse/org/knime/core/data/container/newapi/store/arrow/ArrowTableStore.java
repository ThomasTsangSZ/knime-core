/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Mar 26, 2020 (dietzc): created
 */
package org.knime.core.data.container.newapi.store.arrow;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.container.newapi.store.Store;
import org.knime.core.data.container.newapi.store.StoreReadAccess;
import org.knime.core.data.container.newapi.store.StoreReadAccessConfig;
import org.knime.core.data.container.newapi.store.StoreWriteAccess;
import org.knime.core.data.container.newapi.store.arrow.ArrowBooleanReaderFactory.ArrowBooleanReader;
import org.knime.core.data.container.newapi.store.arrow.ArrowBooleanWriterFactory.ArrowBooleanWriter;
import org.knime.core.data.container.newapi.store.arrow.ArrowDoubleWriterFactory.ArrowDoubleWriter;
import org.knime.core.data.container.newapi.store.arrow.ArrowIntReaderFactory.ArrowIntReader;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.util.FileUtil;

/**
 *
 * @author dietzc
 */
public class ArrowTableStore implements Store {

    // TODO configurable?
    // TODO memory dependent?
    // TODO ?
    private static final int BATCH_SIZE = 64;

    // TODO Logging
    private static final Map<DataType, ArrowWriterFactory<?, ?>> WRITER_FACTORIES = new HashMap<>();

    private static final Map<DataType, ArrowReaderFactory<?, ?>> READER_FACTORIES = new HashMap<>();
    {
        // TODO let's make this extensible later on..
        // TODO missing collections
        WRITER_FACTORIES.put(BooleanCell.TYPE, new ArrowBooleanWriterFactory());
        WRITER_FACTORIES.put(DoubleCell.TYPE, new ArrowDoubleWriterFactory());
        WRITER_FACTORIES.put(StringCell.TYPE, new ArrowStringWriterFactory());

        READER_FACTORIES.put(BooleanCell.TYPE, new ArrowBooleanReaderFactory());
        READER_FACTORIES.put(BooleanCell.TYPE, new ArrowIntReaderFactory());
        READER_FACTORIES.put(BooleanCell.TYPE, new ArrowStringReaderFactory());
    }

    private DataTableSpec m_spec;

    private RootAllocator m_rootAllocator;

    private File m_destFile;

    private boolean m_isWriting = false;;

    // TODO maybe later we want to get rid of File for something more generic, e.g. URI or "Connection" or ...
    // TODO we likely don't need the entire spec here...; actually don't need KNIME-specific spec at all since we're only dealing with standard Java types
    // TODO we want to make sure to "chunk data" later. Dest File could be a directory...
    // TODO later we can add serializers of all sorts to the table..
    // TODO we also want to be able to identify consecutive primitive types of same type in the table which are then stored as an array in arrow, rather than individual columns (later).
    public ArrowTableStore(final DataTableSpec spec) {
        m_spec = spec;
        m_rootAllocator = new RootAllocator(Long.MAX_VALUE);
        try {
            // TODO later... directory :-)
            m_destFile = FileUtil.createTempFile(UUID.randomUUID().toString(), ".arrow");
        } catch (IOException ex) {
            // TODO handle later!!!
            // TODO @SimonSchmid :-))
        }
    }

    @Override
    public StoreWriteAccess createWriteAccess() {
        // yey, not synchronized. only tmp anyways.
        if (m_isWriting) {
            throw new IllegalStateException("only single writer supported");
        }
        m_isWriting = true;
        try {
            return new ArrowTableStoreWriteAccess();
        } catch (final FileNotFoundException ex) {
            // TODO: What to do?
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public StoreReadAccess createReadAccess(final StoreReadAccessConfig config) {
        if (m_isWriting) {
            // TODO implement synchrouniosuosuosu read/write
            throw new IllegalStateException("Not allowed atm");
        }
        try {
            return new ArrowTableStoreReadAccess();
        } catch (final IOException ex) {
            // TODO: What to do?
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public void destroy() {
        m_rootAllocator.close();
        m_destFile.delete();
    }

    private final class ArrowTableStoreWriteAccess implements StoreWriteAccess {

        private final ArrowWriter<?>[] m_writers;

        private VectorSchemaRoot m_schemaRoot;

        private final ArrowStreamWriter m_streamWriter;

        // TODO: Only supports single file (integer row count) for now
        private int m_rowIndex = -1;

        public ArrowTableStoreWriteAccess() throws FileNotFoundException {
            m_writers = new ArrowWriter[m_spec.getNumColumns()];
            final List<FieldVector> vectors = new ArrayList<>();
            for (int i = 0; i < m_writers.length; i++) {
                // TODO set batch size to 1024. No idea how to set optimal number here...
                // for performance reasons we might add API for the developer (or the framework) to allocate size.
                m_writers[i] = WRITER_FACTORIES.get(m_spec.getColumnSpec(i).getType()).create(m_spec.getName(),
                    m_rootAllocator, /* TODO*/ BATCH_SIZE);
                @SuppressWarnings("resource") // Closed via writer. (TODO that feels very wrong...)
                final FieldVector vector = m_writers[i].getVector();
                vectors.add(vector);
            }
            @SuppressWarnings("resource") // Closed via writer and channel.
            final RandomAccessFile raFile = new RandomAccessFile(m_destFile, "rw");
            @SuppressWarnings("resource") // Closed via writer.
            final FileChannel channel = raFile.getChannel();
            m_schemaRoot = new VectorSchemaRoot(vectors);
            m_streamWriter = new ArrowStreamWriter(m_schemaRoot, null, channel);
        }

        @Override
        public long getCapacity() {
            return BATCH_SIZE;
        }

        @Override
        public void forward() {
            flushIfRequired();
            m_rowIndex++;
        }

        private void flushIfRequired() {
            if (m_rowIndex == BATCH_SIZE - 1) {
                flush();
            }
        }

        private void flush() {
            try {
                m_streamWriter.writeBatch();
            } catch (Exception e) {
                // TODO later
                throw new RuntimeException(e);
            }
            m_rowIndex = -1;
        }

        @Override
        public long getNumColumns() {
            return m_writers.length;
        }

        // set-Methods:
        // TODO: Get rid of element-wise (potentially unsafe) casts?
        // TODO: Do we really need a long column index? m_readers array only supports integer index.

        @Override
        public void setBoolean(final long index, final boolean value) {
            ((ArrowBooleanWriter)m_writers[(int)index]).writeBoolean(m_rowIndex, value);
        }

        @Override
        public void setDouble(final long index, final double value) {
            ((ArrowDoubleWriter)m_writers[(int)index]).writeDouble(m_rowIndex, value);
        }

        @Override
        public void setString(final long index, final String value) {
            ((ArrowWriter<String>)m_writers[(int)index]).write(m_rowIndex, value);
        }

        @Override
        public void close() throws Exception {
            // TODO: Carefully handle exceptions (later).
            flush();
            m_streamWriter.close();
            m_schemaRoot.close();
            for (int i = 0; i < m_writers.length; i++) {
                m_writers[i].close();
            }
            m_isWriting = false;
        }
    }

    private final class ArrowTableStoreReadAccess implements StoreReadAccess {

        private final ArrowStreamReader m_streamReader;

        private final VectorSchemaRoot m_schemaRoot;

        private final ArrowReader<?>[] m_readers;

        // TODO: Only supports single file (integer row count) for now
        private int m_rowIndex = -1;

        public ArrowTableStoreReadAccess() throws IOException {
            @SuppressWarnings("resource") // Closed via reader and channel.
            final RandomAccessFile raFile = new RandomAccessFile(m_destFile, "r");
            m_streamReader = new ArrowStreamReader(raFile.getChannel(), m_rootAllocator);
            m_schemaRoot = m_streamReader.getVectorSchemaRoot();
            m_readers = new ArrowReader[m_spec.getNumColumns()];
            for (int i = 0; i < m_readers.length; i++) {
                final DataType columnType = m_spec.getColumnSpec(i).getType();
                final ArrowReaderFactory<?, ?> readerFactory = READER_FACTORIES.get(columnType);
                @SuppressWarnings("resource") // Handled by vector schema root.
                final FieldVector vector = m_schemaRoot.getVector(i);
                m_readers[i] = createReader(readerFactory, vector);
            }
        }

        private <I extends ValueVector> ArrowReader<?> createReader(final ArrowReaderFactory<I, ?> readerFactory,
            final FieldVector vector) {
            final Class<?> readerSourceType = readerFactory.getSourceType();
            if (readerSourceType.isInstance(vector)) {
                @SuppressWarnings("unchecked") // Type was checked dynamically.
                final I castedVector = (I)vector;
                return readerFactory.create(castedVector);
            } else {
                throw new IllegalStateException("Type mismatch. Reader expects source of type: "
                    + readerSourceType.getTypeName() + ", but vector is of type: " + vector.getClass().getTypeName());
            }
        }

        @Override
        public long getNumRows() {
            return m_schemaRoot.getRowCount();
        }

        @Override
        public boolean canForward() {
            return m_rowIndex < getNumRows() - 1;
        }

        @Override
        public void forward() {
            m_rowIndex++;
        }

        @Override
        public long getNumColumns() {
            return m_readers.length;
        }

        // get-Methods:
        // TODO: Get rid of element-wise (potentially unsafe) casts?
        // TODO: Do we really need a long column index? m_readers array only supports integer index.

        @Override
        public boolean getBoolean(final long index) {
            return ((ArrowBooleanReader)m_readers[(int)index]).readBoolean(m_rowIndex);
        }

        @Override
        public int getInt(final long index) {
            return ((ArrowIntReader)m_readers[(int)index]).readInt(m_rowIndex);
        }

        @Override
        public String getString(final long index) {
            return ((ArrowReader<String>)m_readers[(int)index]).read(m_rowIndex);
        }

        @Override
        public void close() throws Exception {
            // TODO: Carefully handle exceptions (later).
            for (int i = 0; i < m_readers.length; i++) {
                m_readers[i].close();
            }
            m_streamReader.close();
        }
    }
}
