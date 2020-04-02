package org.knime.core.data.vector.group;

import org.apache.commons.lang3.tuple.Pair;
import org.knime.core.data.table.value.WritableStructValue;
import org.knime.core.data.table.value.WritableValue;
import org.knime.core.data.vector.Vector;
import org.knime.core.data.vector.WritableVectorStore;
import org.knime.core.data.vector.table.VectorValue;

// TODO common abstract class with ReadableVectorStoreGroup
public class WritableVectorStoreGroup<D> implements WritableVectorStore<D> {

	private WritableVectorStore<?>[] m_children;
	private VectorValue<?>[] m_values;

	public WritableVectorStoreGroup(WritableVectorStore<?>... children) {
		m_children = children;
		m_values = new VectorValue[children.length];
		for (int i = 0; i < children.length; i++) {
			m_values[i] = children[i].createLinkedValue();
		}
	}

	@Override
	public Pair<Long, Vector<D>> add() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VectorValue<D> createLinkedValue() {
		return new WritableVectorStoreGroupValue();
	}

	class WritableVectorStoreGroupValue implements WritableStructValue, VectorValue<D> {

		private Vector<D> m_currentPartition;

		@Override
		public boolean isMissing() {
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
		}

		@Override
		public WritableValue writableValueAt(long i) {
			return m_values[(int) i];
		}

	}

}
