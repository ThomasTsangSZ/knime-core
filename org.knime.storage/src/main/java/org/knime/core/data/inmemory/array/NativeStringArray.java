package org.knime.core.data.inmemory.array;

import org.knime.core.data.table.value.ReadableStringValue;
import org.knime.core.data.table.value.WritableStringValue;

public class NativeStringArray implements NativeArray<String[]> {

	private String[] m_array;

	NativeStringArray(int capacity) {
		m_array = new String[capacity];
	}

	@Override
	public boolean isMissing(int index) {
		return m_array[index] == null;
	}

	@Override
	public void setMissing(int index) {
		m_array[index] = null;
	}

	@Override
	public String[] get() {
		return m_array;
	}

	public static final class NativeStringValue //
			extends AbstractNativeValue<String[], NativeStringArray> //
			implements ReadableStringValue, WritableStringValue {

		@Override
		public String getStringValue() {
			return m_array[m_index].toString();
		}

		@Override
		public void setStringValue(final String value) {
			m_array[m_index] = value;
		}
	}

	@Override
	public void close() throws Exception {
		m_array = null;
	}

	@Override
	public long size() {
		return m_array.length;
	}
}
