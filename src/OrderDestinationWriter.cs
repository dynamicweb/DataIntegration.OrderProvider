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
        if (Mapping.DestinationTable != null && Mapping.DestinationTable.Name == "EcomAssortmentPermissions")
        {
            if (columnMappings.Find(m => string.Compare(m.DestinationColumn.Name, "AssortmentPermissionAccessUserID", true) == 0) == null)
                destColumns.Add(new SqlColumn("AssortmentPermissionAccessUserID", typeof(string), SqlDbType.Int, null, -1, false, true, false));
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
}
