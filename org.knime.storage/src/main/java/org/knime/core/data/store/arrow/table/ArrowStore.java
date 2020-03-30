
package org.knime.core.data.store.arrow.table;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.knime.core.data.store.arrow.table.column.DefaultArrowReadableColumn;
import org.knime.core.data.store.arrow.table.column.DefaultArrowWritableColumn;
import org.knime.core.data.store.arrow.table.value.ArrowReadableBooleanValueAccess;
import org.knime.core.data.store.arrow.table.value.ArrowReadableDoubleValueAccess;
import org.knime.core.data.store.arrow.table.value.ArrowReadableStringValueAccess;
import org.knime.core.data.store.arrow.table.value.ArrowWritableBooleanValueAccess;
import org.knime.core.data.store.arrow.table.value.ArrowWritableDoubleValueAccess;
import org.knime.core.data.store.arrow.table.value.ArrowWritableStringValueAccess;
import org.knime.core.data.store.table.Store;
import org.knime.core.data.store.table.column.ColumnSchema;
import org.knime.core.data.store.table.column.ColumnType;
import org.knime.core.data.store.table.column.ReadableColumn;
import org.knime.core.data.store.table.column.WritableColumn;

public class ArrowStore implements Store {

	// Batch size in number of values, not in bytes!
	// TODO: Maybe adaptive later? Is the current value a reasonable default?
	private static final int BATCH_SIZE = 1024;

	private final BufferAllocator m_allocator;

	private final VectorStore<?>[] m_vectorStores;

	private final WritableColumn[] m_writableLogicalColumns;

	private final ReadableColumn[] m_readableLogicalColumns;

	// TODO: Handle wide tables more efficiently.
	// - Instantiate vector stores and columns on demand
	// - Access column schema schema on demand
	public ArrowStore(final ColumnSchema[] schemas) {
		// TODO: Likely we want to have a child allocator, passed from some central
		// memory management.
		m_allocator = new RootAllocator();

		m_vectorStores = new VectorStore<?>[schemas.length];
		m_writableLogicalColumns = new WritableColumn[schemas.length];
		m_readableLogicalColumns = new ReadableColumn[schemas.length];

		// TODO: Make creating store & columns more concise. Factory?
		for (int i = 0; i < schemas.length; i++) {
			final ColumnType type = schemas[i].getType();
			switch (type) {
				case BOOLEAN: {
					final VectorStore<BitVector> vectorStore = new CachedVectorStore<BitVector>() {

						@Override
						protected BitVector createNewChunk() {
							final BitVector vector = new BitVector((String) null, m_allocator);
							vector.allocateNew(BATCH_SIZE);
							return vector;
						}
					};
					m_vectorStores[i] = vectorStore;
					m_writableLogicalColumns[i] = //
						new DefaultArrowWritableColumn<>(new ArrowWritableBooleanValueAccess(), vectorStore);
					m_readableLogicalColumns[i] = //
						new DefaultArrowReadableColumn<>(ArrowReadableBooleanValueAccess::new, vectorStore);
					break;
				}
				case DOUBLE: {
					final VectorStore<Float8Vector> vectorStore = new CachedVectorStore<Float8Vector>() {

						@Override
						protected Float8Vector createNewChunk() {
							final Float8Vector vector = new Float8Vector((String) null, m_allocator);
							vector.allocateNew(BATCH_SIZE);
							return vector;
						}
					};
					m_vectorStores[i] = vectorStore;
					m_writableLogicalColumns[i] = //
						new DefaultArrowWritableColumn<>(new ArrowWritableDoubleValueAccess(), vectorStore);
					m_readableLogicalColumns[i] = //
						new DefaultArrowReadableColumn<>(ArrowReadableDoubleValueAccess::new, vectorStore);
					break;
				}
				case STRING: {
					final VectorStore<VarCharVector> vectorStore = new CachedVectorStore<VarCharVector>() {

						@Override
						protected VarCharVector createNewChunk() {
							final VarCharVector vector = new VarCharVector((String) null, m_allocator);
							// TODO: This assumes that string values are of size 64 bytes on
							// average. This might need tweaking or should at least be
							// configurable somewhere.
							vector.allocateNew(64l * BATCH_SIZE, BATCH_SIZE);
							return vector;
						}
					};
					m_vectorStores[i] = vectorStore;
					m_writableLogicalColumns[i] = //
						new DefaultArrowWritableColumn<>(new ArrowWritableStringValueAccess(), vectorStore);
					m_readableLogicalColumns[i] = //
						new DefaultArrowReadableColumn<>(ArrowReadableStringValueAccess::new, vectorStore);
					break;
				}
				default:
					// TODO: Support all possible types.
					throw new IllegalStateException("Type: " + type + " is not supported.");
			}
		}
	}

	@Override
	public long getNumLogicalColumns() {
		return m_writableLogicalColumns.length;
	}

	@Override
	public WritableColumn getWritableLogicalColumnAt(final long index) {
		return m_writableLogicalColumns[(int) index];
	}

	@Override
	public ReadableColumn getReadableLogicalColumnAt(final long index) {
		return m_readableLogicalColumns[(int) index];
	}
}
