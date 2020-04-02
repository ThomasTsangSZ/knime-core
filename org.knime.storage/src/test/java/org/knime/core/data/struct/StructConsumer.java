package org.knime.core.data.struct;

import java.util.function.Consumer;

import org.knime.core.data.table.WritableTable;

class StructConsumer<S> implements Consumer<S> {

	private WritableTable m_table;

	public StructConsumer(Class<S> person) {
		// TODO create table based on S (e.g. as a struct or row-based or ...)#
		// TODO dynamic struct values? Olalala
		// TODO etc
		m_table = null;

	}

	@Override
	public void accept(S t) {
		// TODO parse data from S
		// TODO
	}

}
