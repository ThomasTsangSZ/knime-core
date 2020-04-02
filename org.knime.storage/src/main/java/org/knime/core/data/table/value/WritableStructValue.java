package org.knime.core.data.table.value;

public interface WritableStructValue extends WritableValue {
	WritableValue writableValueAt(long i);
}
