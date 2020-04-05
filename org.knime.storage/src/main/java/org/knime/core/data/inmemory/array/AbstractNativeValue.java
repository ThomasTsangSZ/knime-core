
package org.knime.core.data.inmemory.array;

import org.knime.core.data.partition.Partition;
import org.knime.core.data.partition.PartitionValue;

// TODO composition over inheritance? :-(
abstract class AbstractNativeValue<A, N extends NativeArray<A>> //
		implements PartitionValue<N> {

	protected int m_index = -1;

	// TODO potential memory-leak. Never closed. always keeps reference on array.
	// however, maybe itself is garbage collected?
	protected A m_array;

	private NativeArray<A> m_proxy;

	@Override
	public void incIndex() {
		m_index++;
	}

	@Override
	public void updatePartition(final Partition<N> partition) {
		m_index = -1;
		m_proxy = partition.get();
		m_array = m_proxy.get();
	}

	@Override
	public boolean isMissing() {
		return m_proxy.isMissing(m_index);
	}

	@Override
	public void setMissing() {
		m_proxy.setMissing(m_index);
	}
}
