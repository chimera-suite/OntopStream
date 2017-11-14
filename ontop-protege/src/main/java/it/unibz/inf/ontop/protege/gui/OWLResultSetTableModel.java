package it.unibz.inf.ontop.protege.gui;

/*
 * #%L
 * ontop-protege4
 * %%
 * Copyright (C) 2009 - 2013 KRDB Research Centre. Free University of Bozen Bolzano.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import it.unibz.inf.ontop.io.PrefixManager;
import it.unibz.inf.ontop.owlrefplatform.owlapi.QuestOWLResultSet;
import org.semanticweb.owlapi.io.OWLObjectRenderer;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.util.SimpleRenderer;

import javax.swing.*;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.TableModel;
import java.util.ArrayList;
import java.util.List;

public class OWLResultSetTableModel implements TableModel {

	private final QuestOWLResultSet results;
	private final int numcols;
	private int numrows;

	// True while table is fetching results
	private boolean isFetching;
	// Set to true to signal the fetching thread to stop
	private boolean stopFetching;

	// The thread where the rows are fetched
	private final Thread rowFetcherThread;

	// Tabular data for exporting result
	private List<String[]> tabularData;
	// List data for presenting result to table GUI
	private final List<String[]> resultsTable = new ArrayList<>();

	private final List<TableModelListener> listeners = new ArrayList<>();

	private final boolean hideUris;
	private final PrefixManager prefixman;

	private final OWLObjectRenderer renderer;

	/**
	 * This constructor creates a TableModel from a ResultSet. It is package
	 * private because it is only intended to be used by
	 * ResultSetTableModelFactory, which is what you should use to obtain a
	 * ResultSetTableModel
	 */
	public OWLResultSetTableModel(QuestOWLResultSet results, PrefixManager prefixman,
								  boolean hideUri, boolean fetchAll, int fetchSizeLimit) throws OWLException {
		this.results = results;

		this.hideUris = hideUri;
		this.prefixman = prefixman;
		this.renderer = new SimpleRenderer(); // NOT ManchesterOWLSyntaxOWLObjectRendererImpl()!


		isFetching = true;
		stopFetching = false;

		numcols = results.getColumnCount();
		numrows = 0;

		// fetch rows asynchronously
		rowFetcherThread = new Thread(() -> {
			try {
				if (results != null)
					fetchRows(fetchAll, fetchSizeLimit);
			}
			catch (final Exception e){
				// Used to show error message during message fetching, but only if
				// another message has not already been shown.
				SwingUtilities.invokeLater(() -> {
					if (!stopFetching) {
						JOptionPane.showMessageDialog(
								null,
								"Error when fetching results. Aborting. " + e.toString());
					}
				});
				e.printStackTrace();
			}
			finally {
				isFetching = false;
			}
		});
		rowFetcherThread.start();
	}

	/**
	 * Returns whether the table is still being populated by SQL fetched from the result object.
	 * Called from the QueryInterfacePanel to decide whether to continue updating the result count
	 */
	public boolean isFetching(){
		return isFetching;
	}



	// Note that ResultSet row and column numbers start at 1, but TableModel column
	// numbers start at 0.

	private String[] getCurrentRow() throws OWLException {
		String[] crow = new String[numcols];
		for (int j = 0; j < numcols; j++) {
			if (stopFetching)
				break;

			OWLPropertyAssertionObject constant = results.getOWLPropertyAssertionObject(j + 1);
			String rendering = (constant == null) ? "" : renderer.render(constant);
			crow[j] = hideUris ? prefixman.getShortForm(rendering) : rendering;
		}
		return crow;
	}

	private void fetchRows(final boolean fetchAll, final int size) throws OWLException, InterruptedException {

		for (int rows_fetched = 0; results.nextRow() && (fetchAll || rows_fetched < size); rows_fetched++) {
			final String[] crow = getCurrentRow();
			if (stopFetching)
				break;

			// Adds a row to the result table.
			// Could interfere with AWT/swing calls, so encapsulated for passing to "invokeLater"
			SwingUtilities.invokeLater(() -> {
				resultsTable.add(crow);
				numrows++;

				// fireModelChangedEvent
				for(TableModelListener tl : listeners) {
					if (stopFetching)
						break;
					synchronized (tl) {
						tl.tableChanged(new TableModelEvent(OWLResultSetTableModel.this));
					}
				}
			});
		}
	}


	/**
	 * Fetch all the tuples returned by the result set.
	 */
	public List<String[]> getTabularData() throws OWLException, InterruptedException {
		if (tabularData == null) {
			tabularData = new ArrayList<>();
			String[] columnNames = results.getSignature().toArray(new String[numcols]);
			// Append the column names
			tabularData.add(columnNames);
			while (isFetching) {
				Thread.sleep(10);
			}
			if (stopFetching)
				return null;
			// Append first the already fetched tuples
			tabularData.addAll(resultsTable);
			// Append the rest
			while (results.nextRow()) {
				String[] crow = getCurrentRow();
				if (stopFetching)
					break; // ROMAN: or return null?

				tabularData.add(crow);
			}
		}
		return tabularData;
	}

	/**
	 * Call this when done with the table model. It closes the ResultSet and the
	 * Statement object used to create it.
	 */
	public void close() {
		stopFetching = true;
		if (rowFetcherThread != null)
			rowFetcherThread.interrupt();
		try {
			results.close();
		}
		catch (OWLException e) {
			e.printStackTrace();
		}
	}



	/** Automatically close when garbage collected */
	@Override
	protected void finalize() {
		try {
			super.finalize();
		}
		catch (Throwable throwable) {
			throwable.printStackTrace();
		}
		close();
	}

	// These two TableModel methods return the size of the table
	@Override
	public int getColumnCount() {
		return numcols;
	}

	@Override
	public int getRowCount() {
		return numrows;
	}

	// This TableModel method returns columns names from the ResultSetMetaData
	@Override
	public String getColumnName(int column) {
		try {
			List<String> signature = results.getSignature();
			if (signature != null && column < signature.size()) {
				return results.getSignature().get(column);
			} else {
				return "";
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			return "ERROR";
		}
	}

	// This TableModel method specifies the data type for each column.
	// We could map SQL types to Java types, but for this example, we'll just
	// convert all the returned data to strings.
	@Override
	public Class getColumnClass(int column) {
		return String.class;
	}

	/**
	 * This is the key method of TableModel: it returns the value at each cell
	 * of the table. We use strings in this case. If anything goes wrong, we
	 * return the exception as a string, so it will be displayed in the table.
	 */
	@Override
	public Object getValueAt(int row, int column) {
		String value = resultsTable.get(row)[column];
		return (value == null) ? "" : value;
	}

	// table is not editable
	@Override
	public boolean isCellEditable(int row, int column) {
		return false;
	}

	// table is not editable
	@Override
	public void setValueAt(Object value, int row, int column) { /* NO-OP */ }

	@Override
	public void addTableModelListener(TableModelListener l) {
		listeners.add(l);
	}

	@Override
	public void removeTableModelListener(TableModelListener l) {
		listeners.remove(l);
	}

}
