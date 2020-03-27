package org.knime.core.data.container.newapi.store;
public interface PrimitiveRow {
    long getNumColumns();

    boolean getBoolean(long index);

    int getInt(long index);

    String getString(long index);
}