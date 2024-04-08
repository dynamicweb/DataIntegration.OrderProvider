using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.ProviderHelpers;
using Dynamicweb.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.OrderProvider;

internal class OrderDestinationWriter : BaseSqlWriter
{
    private new Mapping Mapping { get; }
    private ILogger Logger { get; }
    private SqlBulkCopy SqlBulkCopier { get; }
    private int SkippedFailedRowsCount { get; set; }
    private DataTable TableToWrite { get; set; }
    private DataSet DataToWrite { get; } = new DataSet();
    private string TempTablePrefix { get; }
    private bool SkipFailingRows { get; }
    private bool DiscardDuplicates { get; }
    internal SqlCommand SqlCommand { get; }
    internal int RowsToWriteCount { get; set; }
    private int LastLogRowsCount { get; set; }
    protected DuplicateRowsHandler duplicateRowsHandler;
    private readonly ColumnMappingCollection _columnMappings;
    private readonly IEnumerable<ColumnMapping> _activeColumnMappings;

    public OrderDestinationWriter(Mapping mapping, SqlConnection connection, ILogger logger, bool skipFailingRows, bool discardDuplicates)
    {
        Mapping = mapping;
        _columnMappings = Mapping.GetColumnMappings();
        _activeColumnMappings = _columnMappings?.Where(cm => cm.Active) ?? Enumerable.Empty<ColumnMapping>();
        SqlCommand = connection.CreateCommand();
        SqlCommand.CommandTimeout = 1200;
        Logger = logger;
        SkipFailingRows = skipFailingRows;
        DiscardDuplicates = discardDuplicates;
        TempTablePrefix = $"TempTableForBulkImport{mapping.GetId()}";
        SqlBulkCopier = new SqlBulkCopy(connection);
        SqlBulkCopier.DestinationTableName = mapping.DestinationTable.Name + TempTablePrefix;
        SqlBulkCopier.BulkCopyTimeout = 0;
        Initialize();
        if (connection.State != ConnectionState.Open)
            connection.Open();
    }

    protected new virtual void Initialize()
    {
        List<SqlColumn> destColumns = new();
        var columnMappings = Mapping.GetColumnMappings();
        foreach (ColumnMapping columnMapping in columnMappings.DistinctBy(obj => obj.DestinationColumn.Name))
        {
            destColumns.Add((SqlColumn)columnMapping.DestinationColumn);
        }
        SQLTable.CreateTempTable(SqlCommand, Mapping.DestinationTable.SqlSchema, Mapping.DestinationTable.Name, TempTablePrefix, destColumns, Logger);

        TableToWrite = DataToWrite.Tables.Add(Mapping.DestinationTable.Name + TempTablePrefix);
        foreach (SqlColumn column in destColumns)
        {
            TableToWrite.Columns.Add(column.Name, column.Type);
        }
        if (DiscardDuplicates)
        {
            duplicateRowsHandler = new DuplicateRowsHandler(Logger, Mapping);
        }
    }

    internal void FinishWriting()
    {
        SkippedFailedRowsCount = SqlBulkCopierWriteToServer(SqlBulkCopier, TableToWrite, SkipFailingRows, Mapping, Logger);
        if (TableToWrite.Rows.Count != 0)
        {
            RowsToWriteCount = RowsToWriteCount + TableToWrite.Rows.Count - SkippedFailedRowsCount;
            Logger.Log("Added " + RowsToWriteCount + " rows to temporary table for " + Mapping.DestinationTable.Name + ".");
        }
    }

    internal void MoveDataToMainTable(SqlTransaction sqlTransaction, bool updateOnlyExistingRecords, bool insertOnlyNewRecords) =>
        MoveDataToMainTable(Mapping, SqlCommand, sqlTransaction, TempTablePrefix, updateOnlyExistingRecords, insertOnlyNewRecords);

    public new void Write(Dictionary<string, object> row)
    {
        DataRow dataRow = TableToWrite.NewRow();

        foreach (ColumnMapping columnMapping in _activeColumnMappings)
        {
            object rowValue = null;
            if (columnMapping.HasScriptWithValue || row.TryGetValue(columnMapping.SourceColumn?.Name, out rowValue))
            {
                object dataToRow = columnMapping.ConvertInputValueToOutputValue(rowValue);

                if (_columnMappings.Any(obj => obj.DestinationColumn.Name == columnMapping.DestinationColumn.Name && obj.GetId() != columnMapping.GetId()))
                {
                    dataRow[columnMapping.DestinationColumn.Name] += dataToRow.ToString();
                }
                else
                {
                    dataRow[columnMapping.DestinationColumn.Name] = dataToRow;
                }
            }
            else
            {
                Logger.Info(BaseDestinationWriter.GetRowValueNotFoundMessage(row, columnMapping.SourceColumn.Table.Name, columnMapping.SourceColumn.Name));
            }
        }

        if (!DiscardDuplicates || !duplicateRowsHandler.IsRowDuplicate(_columnMappings, Mapping, dataRow, row))
        {
            TableToWrite.Rows.Add(dataRow);
            // if 10k write table to db, empty table
            if (TableToWrite.Rows.Count >= 1000)
            {
                RowsToWriteCount = RowsToWriteCount + TableToWrite.Rows.Count;
                SkippedFailedRowsCount = SqlBulkCopierWriteToServer(SqlBulkCopier, TableToWrite, SkipFailingRows, Mapping, Logger);
                RowsToWriteCount = RowsToWriteCount - SkippedFailedRowsCount;
                TableToWrite.Clear();
                if (RowsToWriteCount >= LastLogRowsCount + 10000)
                {
                    LastLogRowsCount = RowsToWriteCount;
                    Logger.Log("Added " + RowsToWriteCount + " rows to temporary table for " + Mapping.DestinationTable.Name + ".");
                }
            }
        }
    }

    public new void Close()
    {
        string text = Mapping.DestinationTable.Name + TempTablePrefix;
        SqlCommand.CommandText = "if exists (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'" + text + "') AND type in (N'U')) drop table " + text;
        SqlCommand.ExecuteNonQuery();
        ((IDisposable)SqlBulkCopier).Dispose();
        if (duplicateRowsHandler != null)
        {
            duplicateRowsHandler.Dispose();
        }
    }
}
