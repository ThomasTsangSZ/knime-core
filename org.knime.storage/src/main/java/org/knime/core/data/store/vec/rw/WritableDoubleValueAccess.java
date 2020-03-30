
package org.knime.core.data.store.vec.rw;

import org.knime.core.data.store.table.row.WritableValueAccess;

public interface WritableDoubleValueAccess extends WritableValueAccess {

	void setDoubleValue(double value);
}
