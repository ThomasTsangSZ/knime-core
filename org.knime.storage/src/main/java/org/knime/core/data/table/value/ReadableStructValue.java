package org.knime.core.data.table.value;

public interface ReadableStructValue extends ReadableValue {
	ReadableValue valueAt(long i);
}
