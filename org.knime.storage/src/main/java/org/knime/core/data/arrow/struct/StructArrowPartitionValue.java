package org.knime.core.data.arrow.struct;

import org.knime.core.data.arrow.ArrowPartitionStore;
import org.knime.core.data.partition.Partition;
import org.knime.core.data.partition.PartitionValue;
import org.knime.core.data.table.value.ReadableStructValue;
import org.knime.core.data.table.value.ReadableValue;
import org.knime.core.data.table.value.WritableStructValue;
import org.knime.core.data.table.value.WritableValue;

class StructArrowPartitionValue implements PartitionValue<Partition<?>[]>, ReadableStructValue, WritableStructValue {

	private PartitionValue<?>[] m_values;
	private ArrowPartitionStore<?>[] m_stores;

	StructArrowPartitionValue(PartitionValue<?>[] values, final ArrowPartitionStore<?>[] stores) {
		m_values = values;
		m_stores = stores;
	}

	@Override
	public boolean isMissing() {
		boolean isMissing = true;

		// if all is missing.
		for (final PartitionValue<?> value : m_values) {
			isMissing = value.isMissing();
			if (!isMissing) {
				return false;
			}
		}
		return isMissing;
	}

	@Override
	public void setMissing() {
		for (PartitionValue<?> value : m_values) {
			value.setMissing();
		}
	}

	@Override
	public WritableValue writableValueAt(long i) {
		return m_values[(int) i];
	}

	@Override
	public ReadableValue readableValueAt(long i) {
		return m_values[(int) i];
	}

	@Override
	public void incIndex() {
		for (PartitionValue<?> value : m_values) {
			value.incIndex();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void updatePartition(Partition<Partition<?>[]> partition) {
		for (int i = 0; i < m_stores.length; i++) {
			// TODO is there a better way?
			m_values[i].updatePartition((Partition) partition.get()[i]);
		}
	}

}