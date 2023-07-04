using Dynamicweb.Data;
using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.DataIntegration.Integration.Interfaces;
using Dynamicweb.DataIntegration.ProviderHelpers;
using Dynamicweb.DataIntegration.Providers.DynamicwebProvider;
using Dynamicweb.Ecommerce.Orders;
using Dynamicweb.Extensibility.AddIns;
using Dynamicweb.Extensibility.Editors;
using Dynamicweb.Logging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Xml;
using System.Xml.Linq;

namespace Dynamicweb.DataIntegration.Providers.OrderProvider
{
    [AddInName("Dynamicweb.DataIntegration.Providers.Provider"), AddInLabel("Order Provider"), AddInDescription("Order provider"), AddInIgnore(false)]
    public class OrderProvider : SqlProvider.SqlProvider, ISource, IDestination, IParameterOptions
    {
        private Job job = null;

        [AddInParameter("Export not yet exported Orders"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Source")]
        public virtual bool ExportNotExportedOrders { get; set; }

        [AddInParameter("Only export orders without externalID"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Source")]
        public virtual bool ExportOnlyOrdersWithoutExtID { get; set; }

        [AddInParameter("Export completed orders only"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Source")]
        public virtual bool DoNotExportCarts { get; set; }

        [AddInParameter("Order state after export"), AddInParameterEditor(typeof(GroupedDropDownParameterEditor), "none=true;noneText=Leave unchanged;"), AddInParameterGroup("Source")]
        public virtual string OrderStateAfterExport { get; set; }

        [AddInParameter("Remove missing order lines"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("Destination")]
        public bool RemoveMissingOrderLines { get; set; }

        #region Hide Properties in the UI section

        [AddInParameter("Source server"), AddInParameterEditor(typeof(TextParameterEditor), "htmlClass=connectionStringInput;"), AddInParameterGroup("hidden")]
        public override string SourceServer
        {
            get { return Server; }
            set { Server = value; }
        }
        [AddInParameter("Destination server"), AddInParameterEditor(typeof(TextParameterEditor), "htmlClass=connectionStringInput;"), AddInParameterGroup("hidden")]
        public override string DestinationServer
        {
            get { return Server; }
            set { Server = value; }
        }
        [AddInParameter("Use integrated security to connect to source server"), AddInParameterEditor(typeof(YesNoParameterEditor), "htmlClass=connectionStringInput;"), AddInParameterGroup("hidden")]
        public override bool SourceServerSSPI
        {
            get;
            set;
        }
        [AddInParameter("Use integrated security to connect to destination server"), AddInParameterEditor(typeof(YesNoParameterEditor), "htmlClass=connectionStringInput;"), AddInParameterGroup("hidden")]
        public override bool DestinationServerSSPI
        {
            get;
            set;
        }
        [AddInParameter("Sql source server username"), AddInParameterEditor(typeof(TextParameterEditor), "htmlClass=connectionStringInput;"), AddInParameterGroup("hidden")]
        public override string SourceUsername
        {
            get { return Username; }
            set { Username = value; }
        }
        [AddInParameter("Sql destination server username"), AddInParameterEditor(typeof(TextParameterEditor), "htmlClass=connectionStringInput;"), AddInParameterGroup("hidden")]
        public override string DestinationUsername
        {
            get { return Username; }
            set { Username = value; }
        }
        [AddInParameter("Sql source server password"), AddInParameterEditor(typeof(TextParameterEditor), "htmlClass=connectionStringInput;"), AddInParameterGroup("hidden")]
        public override string SourcePassword
        {
            get { return Password; }
            set { Password = value; }
        }
        [AddInParameter("Sql destination server password"), AddInParameterEditor(typeof(TextParameterEditor), "htmlClass=connectionStringInput;"), AddInParameterGroup("hidden")]
        public override string DestinationPassword
        {
            get { return Password; }
            set { Password = value; }
        }
        [AddInParameter("Sql source database"), AddInParameterEditor(typeof(TextParameterEditor), "htmlClass=connectionStringInput;"), AddInParameterGroup("hidden")]
        public override string SourceDatabase
        {
            get { return Catalog; }
            set { Catalog = value; }
        }
        [AddInParameter("Sql source connection string"), AddInParameterEditor(typeof(TextParameterEditor), "htmlClass=connectionStringInput;"), AddInParameterGroup("hidden")]
        public override string SourceConnectionString
        {
            get { return ManualConnectionString; }
            set { ManualConnectionString = value; }
        }
        [AddInParameter("Sql destination connection string"), AddInParameterEditor(typeof(TextParameterEditor), "htmlClass=connectionStringInput;"), AddInParameterGroup("hidden")]
        public override string DestinationConnectionString
        {
            get { return ManualConnectionString; }
            set { ManualConnectionString = value; }
        }
        [AddInParameter("Sql destination server password"), AddInParameterEditor(typeof(TextParameterEditor), "htmlClass=connectionStringInput;"), AddInParameterGroup("hidden")]
        public override string DestinationDatabase
        {
            get { return Catalog; }
            set { Catalog = value; }
        }
        [AddInParameter("Remove missing rows after import"), AddInParameterEditor(typeof(YesNoParameterEditor), ""), AddInParameterGroup("hidden")]
        public override bool RemoveMissingAfterImport { get; set; }
        #endregion

        protected override SqlConnection Connection
        {
            get { return connection ?? (connection = (SqlConnection)Database.CreateConnection()); }
            set { connection = value; }
        }

        public OrderProvider(string connectionString)
        {
            SqlConnectionString = connectionString;
            Connection = new SqlConnection(connectionString);
            DiscardDuplicates = false;
        }

        public override void LoadSettings(Job job)
        {
            this.job = job;
            OrderTablesInJob(job);
        }

        public new void Close()
        {
            if (job != null && job.Result == JobResult.Completed)
                OrderSourceReader.UpdateExportedOrdersInDb(OrderStateAfterExport, connection);

            base.Close();
        }

        public override Schema GetOriginalSourceSchema()
        {
            Schema result = base.GetOriginalSourceSchema();
            List<string> tablestToKeep = new List<string> { "EcomOrders", "EcomOrderLines", "EcomOrderLineFields", "EcomOrderLineFieldGroupRelation" };
            List<Table> tablesToRemove = new List<Table>();
            foreach (Table table in result.GetTables())
            {
                if (!tablestToKeep.Contains(table.Name))
                    tablesToRemove.Add(table);
                else if (table.Name == "EcomOrders")
                {
                    table.AddColumn(new SqlColumn(("OrderCustomerAccessUserExternalId"), typeof(string), SqlDbType.NVarChar, table, -1,
                                                  false, false, true));
                }
            }
            foreach (Table table in tablesToRemove)
            {
                result.RemoveTable(table);
            }

            return result;
        }

        public override Schema GetOriginalDestinationSchema()
        {
            return GetOriginalSourceSchema();
        }

        public override void OverwriteSourceSchemaToOriginal()
        {
            Schema = GetOriginalSourceSchema();
        }

        public override void OverwriteDestinationSchemaToOriginal()
        {
            Schema = GetOriginalSourceSchema();
        }

        public override Schema GetSchema()
        {
            if (Schema == null)
            {
                Schema = GetOriginalSourceSchema();
            }
            return Schema;
        }
        public OrderProvider(XmlNode xmlNode)
        {
            foreach (XmlNode node in xmlNode.ChildNodes)
            {
                switch (node.Name)
                {
                    case "SqlConnectionString":
                        if (node.HasChildNodes)
                        {
                            SqlConnectionString = node.FirstChild.Value;
                            Connection = new SqlConnection(SqlConnectionString);
                        }
                        break;
                    case "Schema":
                        Schema = new Schema(node);
                        break;
                    case "ExportNotYetExportedOrders":
                        if (node.HasChildNodes)
                        {
                            ExportNotExportedOrders = node.FirstChild.Value == "True";
                        }
                        break;
                    case "ExportOnlyOrdersWithoutExtID":
                        if (node.HasChildNodes)
                        {
                            ExportOnlyOrdersWithoutExtID = node.FirstChild.Value == "True";
                        }
                        break;
                    case "DoNotExportCarts":
                        if (node.HasChildNodes)
                        {
                            DoNotExportCarts = node.FirstChild.Value == "True";
                        }
                        break;
                    case "OrderStateAfterExport":
                        if (node.HasChildNodes)
                        {
                            OrderStateAfterExport = node.FirstChild.Value;
                        }
                        break;
                    case "DiscardDuplicates":
                        if (node.HasChildNodes)
                        {
                            DiscardDuplicates = node.FirstChild.Value == "True";
                        }
                        break;
                    case "RemoveMissingOrderLines":
                        if (node.HasChildNodes)
                        {
                            RemoveMissingOrderLines = node.FirstChild.Value == "True";
                        }
                        break;
                    case "SkipFailingRows":
                        if (node.HasChildNodes)
                        {
                            SkipFailingRows = node.FirstChild.Value == "True";
                        }
                        break;
                }
            }
        }

        public override string ValidateDestinationSettings()
        {
            return string.Empty;
        }
        public override string ValidateSourceSettings()
        {
            return null;
        }
        public new virtual void SaveAsXml(XmlTextWriter xmlTextWriter)
        {
            xmlTextWriter.WriteElementString("SqlConnectionString", SqlConnectionString);
            xmlTextWriter.WriteElementString("ExportNotYetExportedOrders", ExportNotExportedOrders.ToString(CultureInfo.CurrentCulture));
            xmlTextWriter.WriteElementString("ExportOnlyOrdersWithoutExtID", ExportOnlyOrdersWithoutExtID.ToString(CultureInfo.CurrentCulture));
            xmlTextWriter.WriteElementString("DoNotExportCarts ", DoNotExportCarts.ToString(CultureInfo.CurrentCulture));
            xmlTextWriter.WriteElementString("OrderStateAfterExport ", OrderStateAfterExport);
            xmlTextWriter.WriteElementString("DiscardDuplicates", DiscardDuplicates.ToString(CultureInfo.CurrentCulture));
            xmlTextWriter.WriteElementString("RemoveMissingOrderLines", RemoveMissingOrderLines.ToString(CultureInfo.CurrentCulture));
            xmlTextWriter.WriteElementString("SkipFailingRows", SkipFailingRows.ToString());
            GetSchema().SaveAsXml(xmlTextWriter);
        }
        public override void UpdateSourceSettings(ISource source)
        {
            OrderProvider newProvider = (OrderProvider)source;
            ExportNotExportedOrders = newProvider.ExportNotExportedOrders;
            ExportOnlyOrdersWithoutExtID = newProvider.ExportOnlyOrdersWithoutExtID;
            DoNotExportCarts = newProvider.DoNotExportCarts;
            OrderStateAfterExport = newProvider.OrderStateAfterExport;
            DiscardDuplicates = newProvider.DiscardDuplicates;
            RemoveMissingOrderLines = newProvider.RemoveMissingOrderLines;
            SkipFailingRows = newProvider.SkipFailingRows;
            base.UpdateSourceSettings(source);
        }
        public override void UpdateDestinationSettings(IDestination destination)
        {
            ISource newProvider = (ISource)destination;
            UpdateSourceSettings(newProvider);
        }
        public override string Serialize()
        {
            XDocument document = new XDocument(new XDeclaration("1.0", "utf-8", string.Empty));
            XElement root = new XElement("Parameters");
            root.Add(CreateParameterNode(GetType(), "Export not yet exported Orders", ExportNotExportedOrders.ToString(CultureInfo.CurrentCulture)));
            root.Add(CreateParameterNode(GetType(), "Only export orders without externalID", ExportOnlyOrdersWithoutExtID.ToString(CultureInfo.CurrentCulture)));
            root.Add(CreateParameterNode(GetType(), "Export completed orders only", DoNotExportCarts.ToString(CultureInfo.CurrentCulture)));
            root.Add(CreateParameterNode(GetType(), "Order state after export", OrderStateAfterExport));
            root.Add(CreateParameterNode(GetType(), "Discard duplicates", DiscardDuplicates.ToString()));
            root.Add(CreateParameterNode(GetType(), "Remove missing order lines", RemoveMissingOrderLines.ToString()));
            root.Add(CreateParameterNode(GetType(), "Persist successful rows and skip failing rows", SkipFailingRows.ToString()));
            document.Add(root);

            return document.ToString();
        }

        public OrderProvider()
        {
            DiscardDuplicates = false;
        }
        public new ISourceReader GetReader(Mapping mapping)
        {
            return new OrderSourceReader(mapping, Connection, ExportNotExportedOrders, ExportOnlyOrdersWithoutExtID, DoNotExportCarts);
        }

        protected internal static void OrderTablesInJob(Job job)
        {
            MappingCollection tables = new MappingCollection();
            if (GetMappingByName(job.Mappings, "EcomOrders") != null)
                tables.Add(GetMappingByName(job.Mappings, "EcomOrders"));
            if (GetMappingByName(job.Mappings, "EcomOrderLines") != null)
                tables.Add(GetMappingByName(job.Mappings, "EcomOrderLines"));
            if (GetMappingByName(job.Mappings, "EcomOrderLineFields") != null)
                tables.Add(GetMappingByName(job.Mappings, "EcomOrderLineFields"));
            if (GetMappingByName(job.Mappings, "EcomOrderLineFieldGroupRelation") != null)
                tables.Add(GetMappingByName(job.Mappings, "EcomOrderLineFieldGroupRelation"));
            job.Mappings = tables;
        }

        internal static Mapping GetMappingByName(MappingCollection collection, string name)
        {
            return collection.Find(map => map.DestinationTable.Name == name);
        }

        public override bool RunJob(Job job)
        {
            OrderTablesInJob(job);

            var writers = new List<DynamicwebBulkInsertDestinationWriter>();
            SqlTransaction sqlTransaction = null;
            Dictionary<string, object> sourceRow = null;
            try
            {
                ReplaceMappingConditionalsWithValuesFromRequest(job);
                if (Connection.State != ConnectionState.Open)
                    Connection.Open();

                AddMappingsToJobThatNeedsToBeThereForMoveToMainTables(job);
                foreach (Mapping mapping in job.Mappings)
                {
                    if (mapping.Active)
                    {
                        Logger.Log("Starting import to temporary table for " + mapping.DestinationTable.Name + ".");
                        using (var reader = job.Source.GetReader(mapping))
                        {
                            var writer = new DynamicwebBulkInsertDestinationWriter(mapping, Connection, false, false, Logger, null, DiscardDuplicates, false, SkipFailingRows);
                            var columnMappings = mapping.GetColumnMappings();
                            while (!reader.IsDone())
                            {
                                sourceRow = reader.GetNext();
                                ProcessInputRow(mapping, sourceRow);
                                ProcessRow(mapping, columnMappings, sourceRow);
                                writer.Write(sourceRow);
                            }
                            writer.FinishWriting();
                            writers.Add(writer);
                        }
                        Logger.Log("Finished import to temporary table for " + mapping.DestinationTable.Name + ".");
                    }
                }

                sourceRow = null;
                RemoveColumnMappingsFromJobThatShouldBeSkippedInMoveToMainTables(job);
                sqlTransaction = Connection.BeginTransaction();
                foreach (DynamicwebBulkInsertDestinationWriter writer in writers)
                {
                    writer.MoveDataToMainTable(sqlTransaction);
                }

                RemoveMissingRows(writers, sqlTransaction);

                sqlTransaction.Commit();
                Ecommerce.Services.Orders.ClearCache();
            }
            catch (Exception ex)
            {
                string msg = ex.Message;
                LogManager.System.GetLogger(LogCategory.Application, "Dataintegration").Error($"{GetType().Name} error: {ex.Message} Stack: {ex.StackTrace}", ex);

                if (sourceRow != null)
                    msg += GetFailedSourceRowMessage(sourceRow);

                Logger.Log("Import job failed: " + msg);
                if (sqlTransaction != null)
                    sqlTransaction.Rollback();
                return false;
            }
            finally
            {
                foreach (var writer in writers)
                {
                    writer.Close();
                }
                job.Source.Close();
                Connection.Dispose();
                sourceRow = null;
            }
            return true;
        }

        IEnumerable<ParameterOption> IParameterOptions.GetParameterOptions(string parameterName)
        {
            var result = new List<ParameterOption>();

            foreach (OrderState state in Ecommerce.Services.OrderStates.GetStatesByOrderType(OrderType.Order))
            {
                if (state.IsDeleted)
                    continue;
                string name = state.GetName(Ecommerce.Services.Languages.GetDefaultLanguageId());
                string group = Ecommerce.Services.OrderFlows.GetFlowById(state.OrderFlowId)?.Name;
                var value = new GroupedDropDownParameterEditor.DropDownItem(name, group, state.Id);
                result.Add(new(name, value) { Group = group });
            }
            return result;
        }

        private string SourceColumnNameForDestinationOrderCustomerAccessUserId = string.Empty;

        private void AddMappingsToJobThatNeedsToBeThereForMoveToMainTables(Job job)
        {
            Mapping mapping = job.Mappings.Find(m => m.DestinationTable.Name == "EcomOrders");
            if (mapping != null)
            {
                var columnMappings = mapping.GetColumnMappings();
                if (columnMappings.Find(cm => string.Compare(cm.DestinationColumn.Name, "OrderCustomerAccessUserExternalId", true) == 0) != null)
                {
                    var OrderCustomerAccessUserIdMapping = columnMappings.Find(cm => string.Compare(cm.DestinationColumn.Name, "OrderCustomerAccessUserId", true) == 0);
                    if (OrderCustomerAccessUserIdMapping == null)
                    {
                        Column randomColumn = new Column(Guid.NewGuid().ToString(), typeof(string), mapping.SourceTable, false, true);
                        SourceColumnNameForDestinationOrderCustomerAccessUserId = randomColumn.Name;
                        mapping.AddMapping(randomColumn, job.Destination.GetSchema().GetTables().Find(t => t.Name == "EcomOrders").Columns.Find(c => string.Compare(c.Name, "OrderCustomerAccessUserId", true) == 0), true);
                    }
                    else
                    {
                        if (OrderCustomerAccessUserIdMapping.SourceColumn != null)
                        {
                            SourceColumnNameForDestinationOrderCustomerAccessUserId = OrderCustomerAccessUserIdMapping.SourceColumn.Name;
                        }
                    }
                }
            }
        }

        private void RemoveColumnMappingsFromJobThatShouldBeSkippedInMoveToMainTables(Job job)
        {
            Mapping cleanMapping = job.Mappings.Find(m => m.DestinationTable.Name == "EcomOrders");
            if (cleanMapping != null)
            {
                ColumnMappingCollection columnMapping = cleanMapping.GetColumnMappings(true);
                columnMapping.RemoveAll(cm => cm.DestinationColumn != null && string.Compare(cm.DestinationColumn.Name, "OrderCustomerAccessUserExternalId", true) == 0);
            }
        }

        private Hashtable _existingUsers = null;
        /// <summary>
        /// Collection of <AccessUserExternalID>, <AccessUserID> key value pairs
        /// </summary>
        private Hashtable ExistingUsers
        {
            get
            {
                if (_existingUsers == null)
                {
                    _existingUsers = new Hashtable();
                    SqlDataAdapter usersDataAdapter = new SqlDataAdapter("select AccessUserExternalID, AccessUserID from AccessUser where AccessUserExternalID is not null and AccessUserExternalID <> ''", Connection);
                    new SqlCommandBuilder(usersDataAdapter);
                    DataSet dataSet = new DataSet();
                    usersDataAdapter.Fill(dataSet);
                    DataTable dataTable = dataSet.Tables[0];
                    if (dataTable != null)
                    {
                        string key;
                        foreach (DataRow row in dataTable.Rows)
                        {
                            key = row["AccessUserExternalID"].ToString();
                            if (!_existingUsers.ContainsKey(key))
                            {
                                _existingUsers.Add(key, row["AccessUserID"].ToString());
                            }
                        }
                    }
                }
                return _existingUsers;
            }
        }

        private void ProcessRow(Mapping mapping, ColumnMappingCollection columnMappings, Dictionary<string, object> row)
        {
            if (mapping != null && mapping.DestinationTable != null && mapping.DestinationTable.Name == "EcomOrders" && !string.IsNullOrEmpty(SourceColumnNameForDestinationOrderCustomerAccessUserId))
            {
                object accessUserId = DBNull.Value;
                var OrderCustomerAccessUserExternalIdMapping = columnMappings.Find(cm => string.Compare(cm.DestinationColumn.Name, "OrderCustomerAccessUserExternalId", true) == 0);
                if (OrderCustomerAccessUserExternalIdMapping != null && OrderCustomerAccessUserExternalIdMapping.SourceColumn != null)
                {
                    if (row.ContainsKey(OrderCustomerAccessUserExternalIdMapping.SourceColumn.Name))
                    {
                        string externalID = Convert.ToString(row[OrderCustomerAccessUserExternalIdMapping.SourceColumn.Name]);
                        if (!string.IsNullOrEmpty(externalID) && ExistingUsers.ContainsKey(externalID))
                        {
                            accessUserId = ExistingUsers[externalID];
                        }
                    }
                }
                if (!row.ContainsKey(SourceColumnNameForDestinationOrderCustomerAccessUserId))
                {
                    row.Add(SourceColumnNameForDestinationOrderCustomerAccessUserId, accessUserId);
                }
                else
                {
                    row[SourceColumnNameForDestinationOrderCustomerAccessUserId] = accessUserId;
                }
            }
        }

        private void RemoveMissingRows(IEnumerable<DynamicwebBulkInsertDestinationWriter> writers, SqlTransaction sqlTransaction)
        {
            if (RemoveMissingOrderLines)
            {
                DynamicwebBulkInsertDestinationWriter writer = writers.FirstOrDefault(w => string.Compare(w.Mapping?.DestinationTable?.Name, "EcomOrderLines", true) == 0);
                if (writer != null && writer.RowsToWriteCount > 0 && writer.SqlCommand != null)
                {
                    string tempTableName = $"EcomOrderLinesTempTableForBulkImport{writer.Mapping.GetId()}";
                    writer.SqlCommand.Transaction = sqlTransaction;
                    if (writer.Mapping.GetColumnMappings().Any(m => m.Active && m.DestinationColumn != null && string.Compare(m.DestinationColumn.Name, "OrderLineOrderID", true) == 0))
                    {
                        writer.SqlCommand.CommandText = $"DELETE t1 FROM EcomOrderLines AS t1 LEFT JOIN {tempTableName} t2 " +
                            "ON t1.OrderLineID = t2.OrderLineID AND t1.OrderLineOrderID = t2.OrderLineOrderID " +
                            $"WHERE t2.OrderLineID IS NULL AND t1.OrderLineOrderID IN (SELECT DISTINCT(OrderLineOrderID) from {tempTableName})";
                    }
                    else
                    {
                        writer.SqlCommand.CommandText = $"DELETE FROM EcomOrderLines WHERE OrderLineID NOT IN (SELECT OrderLineID FROM {tempTableName}) " +
                            $"AND OrderLineOrderID IN(SELECT DISTINCT(OrderLineOrderID) FROM EcomOrderLines WHERE OrderLineID IN(SELECT OrderLineID FROM {tempTableName}))";
                    }
                    writer.SqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}

