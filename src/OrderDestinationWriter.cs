using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.ProviderHelpers;
using Dynamicweb.Logging;
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
    internal SqlCommand SqlCommand { get; }
    internal int RowsToWriteCount { get; set; }
    private int LastLogRowsCount { get; set; }

    public OrderDestinationWriter(Mapping mapping, SqlConnection connection, ILogger logger, bool skipFailingRows)
    {
        Mapping = mapping;
        SqlCommand = connection.CreateCommand();
        SqlCommand.CommandTimeout = 1200;
        Logger = logger;
        SkipFailingRows = skipFailingRows;
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
        var columnMappings = Mapping.GetColumnMappings();

        var activeColumnMappings = columnMappings.Where(cm => cm.Active);
        foreach (ColumnMapping columnMapping in activeColumnMappings)
        {
            object rowValue = null;
            if (columnMapping.HasScriptWithValue || row.TryGetValue(columnMapping.SourceColumn?.Name, out rowValue))
            {
                object dataToRow = columnMapping.ConvertInputValueToOutputValue(rowValue);

                if (columnMappings.Any(obj => obj.DestinationColumn.Name == columnMapping.DestinationColumn.Name && obj.GetId() != columnMapping.GetId()))
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
