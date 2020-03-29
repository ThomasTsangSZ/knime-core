
package org.knime.core.data.store.table.row;

public interface Row<V> {

	long getNumValues();

	V getValueAt(int idx);
}
