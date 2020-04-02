package org.knime.core.data.vector;

import org.apache.commons.lang3.tuple.Pair;
import org.knime.core.data.table.value.ReadableStructValue;
import org.knime.core.data.table.value.ReadableValue;
import org.knime.core.data.table.value.WritableStructValue;
import org.knime.core.data.table.value.WritableValue;
import org.knime.core.data.vector.table.VectorValue;

// TODO common abstract class with ReadableVectorStoreGroup
public class WritableVectorStoreGroup<K, D> implements WritableVectorStore<K, D> {

	private ReadableVectorStore<K, ?>[] m_children;
	private VectorValue<?>[] m_values;

	public WritableVectorStoreGroup(ReadableVectorStore<K, ?>... children) {
		m_children = children;
		m_values = new VectorValue[children.length];
		for (int i = 0; i < children.length; i++) {
			m_values[i] = children[i].createLinkedValue();
		}
	}

	@Override
	public void release() {

	}

	@Override
	public Pair<K, Vector<D>> add() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void retain() {
		// TODO Auto-generated method stub

	}

	@Override
	public long numVectors() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public VectorValue<D> createLinkedValue() {
		return new VectorStoreGroupValue();
	}

	class VectorStoreGroupValue implements ReadableStructValue, WritableStructValue, VectorValue<D> {

		private Vector<D> m_currentPartition;

		@Override
		public boolean isMissing() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void setMissing() {
			// TODO Auto-generated method stub

		}

		@Override
		public void incIndex() {
			// TODO Auto-generated method stub

		}

		@Override
		public void updatePartition(Vector<D> partition) {
			m_currentPartition = partition;
			m_currentPartition.release();
		}

		@Override
		public WritableValue writableValueAt(long i) {
			return m_values[(int) i];
		}

		@Override
		public ReadableValue readableValueAt(long i) {
			return m_values[(int) i];
		}

	}

}
