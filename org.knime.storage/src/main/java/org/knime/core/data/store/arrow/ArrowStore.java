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
package org.knime.core.data.store.arrow;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.knime.core.data.store.PrimitiveRow;
import org.knime.core.data.store.PrimitiveSpec;
import org.knime.core.data.store.PrimitiveType;
import org.knime.core.data.store.Store;
import org.knime.core.data.store.StoreReadAccess;
import org.knime.core.data.store.StoreReadAccessConfig;
import org.knime.core.data.store.StoreWriteAccess;
import org.knime.core.data.store.arrow.ArrowBooleanWriterFactory.ArrowBooleanWriter;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 *
 * @author dietzc
 * @param <K>
 */

// TODO: physical vs. logical batch-size seems to be important for making use of CPU cache etc.
// TODO: when to flush all (workflow save with producing node in green state)?
// TODO: when to invalidate all? on-reset etc.
public class ArrowStore<K> implements Store {

	// TODO configurable?
	// TODO memory dependent?
	// TODO ?
	private static final int BATCH_SIZE = 64; // REST IS WORK

	// TODO Logging
	private static final Map<PrimitiveType, ArrowWriterFactory<?>> WRITER_FACTORIES = new HashMap<>();

	private static final Map<PrimitiveType, ArrowReaderFactory<?, ?>> READER_FACTORIES = new HashMap<>();
	{
		// TODO let's make this extensible later on..
		// TODO missing collections
		WRITER_FACTORIES.put(PrimitiveType.BOOLEAN, new ArrowBooleanWriterFactory());
		WRITER_FACTORIES.put(PrimitiveType.DOUBLE, new ArrowDoubleWriterFactory());
		WRITER_FACTORIES.put(PrimitiveType.STRING, new ArrowStringWriterFactory());

		READER_FACTORIES.put(PrimitiveType.BOOLEAN, new ArrowBooleanReaderFactory());
		READER_FACTORIES.put(PrimitiveType.DOUBLE, new ArrowIntReaderFactory());
		READER_FACTORIES.put(PrimitiveType.STRING, new ArrowStringReaderFactory());
	}

	private PrimitiveSpec m_spec;

	private RootAllocator m_rootAllocator;

	private File m_destFile;

	private boolean m_isWriting = false;

	// TODO use something smarter here? Guava?
	// TODO this cache is currently a "per table cache". No centralize effort to
	// coordinate caches of multiple tables, yet. Integrate cache life-cycle
	// management by Marc B. (Buffer.LIFECYCLE)
	// TODO connect this to memory alert system and ALSO track off-heap cache usage.
	// TODO need to close batch "onMemoryRelease"
	private final Cache<Integer, ArrowRecordBatch> CACHE;

	private BitSet m_written = new BitSet();

	private AtomicBoolean m_destroyed;

	// TODO primtive spec. Chunk first class citizen (e.g. actually API later to
	// generalize buffering?)
	// TODO maybe later we want to get rid of File for something more generic, e.g.
	// URI or "Connection" or ...
	// TODO we likely don't need the entire spec here...; actually don't need
	// KNIME-specific spec at all since we're only dealing with standard Java types
	// TODO later we can add serializers of all sorts to the table..
	// TODO we also want to be able to identify consecutive primitive types of same
	// type in the table which are then stored as an array in arrow, rather than
	// individual columns (later).
	public ArrowStore(final PrimitiveSpec spec) {
		m_spec = spec;
		m_destroyed = new AtomicBoolean();
		m_rootAllocator = new RootAllocator(Long.MAX_VALUE);

		// TODO handle case where we want to write asynchrounously but keep cache entry
		// e.g. in weak ref cache or wherever (idea: write always to be safe, but keep
		// in cache)

		CACHE = CacheBuilder.newBuilder().removalListener(new RemovalListener<Integer, ArrowRecordBatch>() {

			// TODO DONT USE onRemoval directly. Rather do a sequential writing of batches
			// on memory alerts.
			// TODO if batch is about to be written which is NOT next batch which hasn't
			// been written, then first write all other batches

			// TODO potential alternative: multiple files... (?) drawback: copy of workflow
			// is much worse than today (which is already bad because of directory
			// structure).
			@Override
			public void onRemoval(final RemovalNotification<Integer, ArrowRecordBatch> notification) {

				// TODO write async? or at least configurable
				switch (notification.getCause()) {
				case SIZE:
				case EXPLICIT:
					// two cases:
					// 1) destroying the entire thing: Clean-up cache!
					// 2) entry has been removed and not yet written to disc
					if (!m_destroyed.get() && !m_written.get(notification.getKey())) {
						ArrowRecordBatch batch = notification.getValue();
						VectorSchemaRoot root = new VectorSchemaRoot(fieldVectorFromSpec(spec));
						VectorLoader loader = new VectorLoader(root);
						try (RandomAccessFile ra = new RandomAccessFile(m_destFile, "w");
								FileChannel channel = ra.getChannel();
								ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
							// load into schema
							loader.load(batch);

							// write into file
							writer.writeBatch();

						} catch (FileNotFoundException ex) {
							throw new RuntimeException(ex);
						} catch (IOException ex) {
							throw new RuntimeException(ex);
						}

						// GO WRITE
					}
					break;
				case COLLECTED:
					// not of interest at the moment. as we don't have a weak-reference cache yet.
					// Also if we would use weak-reference cache, we would already have written the
					// file.
				case REPLACED:
					// never happens so far
				case EXPIRED:
					// we could set an expiration time which lets an entry expire after last access
					break;
				}
				// case 1: has already been written (read)
				// case 2: has not been written but should be written (happens during write and
				// during read)

				// obvious potential problems
				// 1) Out of order write into file (?)
				// 2) where do we store info what has been written and what hasn't?
				// 3)

			}
		}).build();
		// TODO create temp file
//			m_destFile = Files.create.createTempFile(UUID.randomUUID().toString(), ".arrow");
	}

	private Iterable<FieldVector> fieldVectorFromSpec(final PrimitiveSpec spec) {
		// TODO Auto-generated method stub
		return null;
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
		if (!m_destroyed.getAndSet(true)) {
			CACHE.invalidateAll();
			CACHE.cleanUp();
			m_rootAllocator.close();
			m_destFile.delete();
		}
	}

	/*
	 *
	 * ACCESSES
	 *
	 * TODO for write access it is safe to assume that only a single writer writes
	 * to an individual store.
	 *
	 */
	private final class ArrowTableStoreWriteAccess implements StoreWriteAccess {

		private final ArrowWriter[] m_writers;

		private VectorSchemaRoot m_schemaRoot;

		// TODO: Only supports single file (integer row count) for now
		private int m_rowIndex = 0;

		private VectorUnloader m_unloader;

		public ArrowTableStoreWriteAccess() throws FileNotFoundException {
			m_writers = new ArrowWriter[m_spec.getNumColumns()];
			final List<FieldVector> vectors = new ArrayList<>();
			for (int colIdx = 0; colIdx < m_writers.length; colIdx++) {
				// TODO set batch size to 1024. No idea how to set optimal number here...
				// for performance reasons we might add API for the developer (or the framework)
				// to allocate size.
				m_writers[colIdx] = WRITER_FACTORIES.get(m_spec.getTypeAt(colIdx)).create("column_" + colIdx,
						m_rootAllocator, /* TODO */ BATCH_SIZE, colIdx);
				@SuppressWarnings("resource") // Closed via writer. (TODO that feels very wrong...)
				final FieldVector vector = m_writers[colIdx].getVector();
				vectors.add(vector);
			}
			m_schemaRoot = new VectorSchemaRoot(vectors);
			m_unloader = new VectorUnloader(m_schemaRoot);
		}

		// This means: close batch on arrow side.
		// cache batch.
		// stuff is written to disc somewhere else (controlled by cache) ->
		// asynchronously
		private void flushIfRequired() {
			if (m_rowIndex + 1 % BATCH_SIZE == 0) {
				flush();
			}
		}

		// finish batch and cache
		private void flush() {
			try {
				// PROBLEM: This guy still has pointers to same vectors as schema root.
				// Subsequent writes to schema root will overwrite this record.
				CACHE.put(m_rowIndex / BATCH_SIZE, m_unloader.getRecordBatch());
				m_schemaRoot.allocateNew();
			} catch (Exception e) {
				// TODO later
				throw new RuntimeException(e);
			}
		}

		@Override
		public void close() throws Exception {
			// TODO: Carefully handle exceptions (later).
			flush();
			m_schemaRoot.close();
			for (int i = 0; i < m_writers.length; i++) {
				m_writers[i].close();
			}
			m_isWriting = false;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void accept(final PrimitiveRow t) {
			flushIfRequired();
			// TODO do we want getNumColumns on row level? This should rather be part of a
			// "PrimitiveSpec"
			long numColumns = t.getNumColumns();
			// TODO implement for all types. Likely we pass proxy down to write and write
			// can do whatever writer wants here...
			for (int i = 0; i < numColumns; i++) {
				((ArrowBooleanWriter) m_writers[i]).writeBoolean(m_rowIndex, t.getBoolean(i));
			}
			m_rowIndex++;
		}
	}

	private final class ArrowTableStoreReadAccess implements StoreReadAccess {

		private final ArrowFileReader m_fileReader;

		private final VectorSchemaRoot m_schemaRoot;

		private final ArrowReader<?>[] m_readers;

		private ArrowRecordBatch m_currentBatch = null;

		// TODO: Only supports single file (integer row count) for now
		private int m_rowIndex = -1;

		private VectorLoader m_loader;

		public ArrowTableStoreReadAccess() throws IOException {
			@SuppressWarnings("resource") // Closed via reader and channel.
			final RandomAccessFile raFile = new RandomAccessFile(m_destFile, "r");
			m_fileReader = new ArrowFileReader(raFile.getChannel(), m_rootAllocator) {
				@Override
				protected void loadRecordBatch(final ArrowRecordBatch batch) {
					m_currentBatch = batch;
				}
			};
			m_schemaRoot = m_fileReader.getVectorSchemaRoot();
			m_readers = new ArrowReader[m_spec.getNumColumns()];
			for (int i = 0; i < m_readers.length; i++) {
				final PrimitiveType columnType = m_spec.getTypeAt(i);
				final ArrowReaderFactory<?, ?> readerFactory = READER_FACTORIES.get(columnType);
				@SuppressWarnings("resource") // Handled by vector schema root.
				final FieldVector vector = m_schemaRoot.getVector(i);
				m_readers[i] = createReader(readerFactory, vector);
			}
			m_loader = new VectorLoader(m_schemaRoot);
		}

		private <I extends ValueVector> ArrowReader<?> createReader(final ArrowReaderFactory<I, ?> readerFactory,
				final FieldVector vector) {
			final Class<?> readerSourceType = readerFactory.getSourceType();
			if (readerSourceType.isInstance(vector)) {
				@SuppressWarnings("unchecked") // Type was checked dynamically.
				final I castedVector = (I) vector;
				return readerFactory.create(castedVector);
			} else {
				throw new IllegalStateException(
						"Type mismatch. Reader expects source of type: " + readerSourceType.getTypeName()
								+ ", but vector is of type: " + vector.getClass().getTypeName());
			}
		}

		@Override
		public boolean hasNext() {
			return m_rowIndex < m_schemaRoot.getRowCount() - 1;
		}

		@SuppressWarnings("resource")
		@Override
		public PrimitiveRow next() {
			try {
				final int batchIdx = m_rowIndex / BATCH_SIZE;
				final ArrowRecordBatch batch = CACHE.getIfPresent(batchIdx);

				// TODO implement that by extending arrowreader etc... own implementations help
				// TODO ALL CACHE ACCESS HAS TO BE THREAD-SAFE (don't put twice etc -> handled
				// by guava?)
				if (batch == null) {
					// I can directly load into schema. because it was requested.
					// but then I need to make sure to get ArrowRecordBatch out of schema to cache.
					m_fileReader.loadRecordBatch(m_fileReader.getRecordBlocks().get(batchIdx));
					CACHE.put(batchIdx, m_currentBatch);
					m_currentBatch = null;
				} else {
					m_loader.load(batch);
				}
				m_rowIndex++;

				return new PrimitiveRow() {

					@Override
					public boolean isMissing(final int colIndex) {
						// TODO Auto-generated method stub
						return false;
					}

					@Override
					public String getString(final long colIndex) {
						// TODO Auto-generated method stub
						return null;
					}

					@Override
					public long getNumColumns() {
						// TODO Auto-generated method stub
						return 0;
					}

					@Override
					public int getInt(final long colIndex) {
						// TODO Auto-generated method stub
						return 0;
					}

					@Override
					public boolean getBoolean(final long colIndex) {
						// TODO Auto-generated method stub
						return false;
					}
				};
			} catch (IOException ex) {
				// TODO move to next()
				throw new RuntimeException(ex);
			}
		}

		// get-Methods:
		// TODO: Get rid of element-wise (potentially unsafe) casts?
		// TODO: Do we really need a long column index? m_readers array only supports
		// integer index.

		@Override
		public void close() throws Exception {
			// TODO: Carefully handle exceptions (later).
			for (int i = 0; i < m_readers.length; i++) {
				m_readers[i].close();
			}
			m_fileReader.close();
		}
	}
}
