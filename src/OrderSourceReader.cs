﻿using Dynamicweb.DataIntegration.Integration;
using Dynamicweb.Ecommerce.Orders;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace Dynamicweb.DataIntegration.Providers.OrderProvider
{
    internal class OrderSourceReader : BaseSqlReader
    {
        private static MappingConditionalCollection _ordersConditions = null;
        private static List<string> _ordersToExport = null;
        private ColumnMappingCollection _columnMappings = null;

        public OrderSourceReader(Mapping mapping, SqlConnection connection, bool exportNotExportedOrders, bool exportOnlyOrdersWithoutExtID, bool doNotExportCarts) : base(mapping, connection)
        {
            _columnMappings = mapping.GetColumnMappings();
            _command = new SqlCommand { Connection = connection };
            if (connection.State.ToString() != "Open")
                connection.Open();
            //save order conditions for OrderLines filtering
            if (mapping.SourceTable.Name == "EcomOrders")
            {
                _ordersToExport = new List<string>();
                _ordersConditions = mapping.Conditionals;
            }
            //add order conditions for correct OrderLines filtering
            if (mapping.SourceTable.Name == "EcomOrderLines" && _ordersConditions != null && _ordersConditions.Count > 0)
                mapping.Conditionals.AddRange(_ordersConditions);

            string whereSql = string.Empty;

            //make where statement only for EcomOrders or EcomOrderLines tables
            if (mapping.SourceTable.Name == "EcomOrders" || mapping.SourceTable.Name == "EcomOrderLines")
            {
                whereSql = GetWhereSql(exportNotExportedOrders, exportOnlyOrdersWithoutExtID, doNotExportCarts);
            }

            LoadReader(whereSql);
        }

        private void LoadReader(string whereSql)
        {
            try
            {
                if (_columnMappings.Count == 0)
                    return;

                string sql = "select " + GetColumns() + " from  " + GetFromTables();

                if (!string.IsNullOrEmpty(whereSql))
                    sql = sql + " where " + whereSql;

                _command.CommandText = sql;
                _reader?.Close();
                _reader = _command.ExecuteReader();
            }
            catch (SqlException)
            {

                throw;
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to open sqlSourceReader. Reason: " + ex.Message, ex);
            }
        }
        protected override string GetColumns()
        {
            string columns = GetDistinctColumnsFromMapping(new string[] { "OrderCustomerAccessUserExternalId" });
            columns = columns.Substring(0, columns.Length - 2);
            switch (mapping.SourceTable.Name)
            {
                case "EcomOrders":
                    columns = columns + ", [AccessUserExternalId] as [OrderCustomerAccessUserExternalId]";
                    if (!columns.Split(',').Any(c => c.Trim(new char[] { ' ', '[', ']' }).Equals("OrderId", StringComparison.OrdinalIgnoreCase)))
                    {
                        columns += ", [OrderId]";
                    }
                    break;

            }
            return columns;
        }

        private string GetWhereSql(bool exportNotExportedOrders, bool exportOnlyOrdersWithoutExtID, bool doNotExportCarts)
        {
            List<SqlParameter> parameters = new List<SqlParameter>();
            string conditionalsSql = MappingExtensions.GetConditionalsSql(out parameters, mapping.Conditionals, false, false);
            if (conditionalsSql != "")
            {
                conditionalsSql = conditionalsSql.Substring(0, conditionalsSql.Length - 4);
                foreach (SqlParameter p in parameters)
                    _command.Parameters.Add(p);
            }

            if (exportNotExportedOrders)
            {
                conditionalsSql = (string.IsNullOrEmpty(conditionalsSql) ? "([OrderIsExported] = 0 OR [OrderIsExported] IS NULL)" :
                                                    conditionalsSql + " AND ([OrderIsExported] = 0 OR [OrderIsExported] IS NULL)");
            }

            if (exportOnlyOrdersWithoutExtID)
            {
                conditionalsSql = (string.IsNullOrEmpty(conditionalsSql) ? "([OrderIntegrationOrderID] = '' OR [OrderIntegrationOrderID] IS NULL)" :
                                                    conditionalsSql + " AND ([OrderIntegrationOrderID] = '' OR [OrderIntegrationOrderID] IS NULL)");
            }
            if (doNotExportCarts)
            {
                conditionalsSql = (string.IsNullOrEmpty(conditionalsSql) ? "[OrderCart] != 1 AND [OrderComplete] = 1" :
                                                    conditionalsSql + " AND [OrderCart] != 1  AND [OrderComplete] = 1");
            }

            return conditionalsSql;
        }

        protected override string GetFromTables()
        {
            string result = "[" + mapping.SourceTable.SqlSchema + "].[" + mapping.SourceTable.Name + "] ";

            switch (mapping.SourceTable.Name)
            {
                case "EcomOrderLines":
                    result = result + " inner join EcomOrders on EcomOrderLines.OrderLineOrderID = EcomOrders.OrderID";
                    break;
                case "EcomOrders":
                    result = "[dbo].[EcomOrders] left join dbo.AccessUser on OrderCustomerAccessUserID = AccessUserID";
                    break;
                default:
                    break;
            }
            return result;
        }

        public override Dictionary<string, object> GetNext()
        {
            if (mapping.SourceTable.Name == "EcomOrders")
            {
                string orderId = Core.Converter.ToString(_reader["OrderId"]);
                if (!string.IsNullOrEmpty(orderId) && !_ordersToExport.Contains(orderId))
                {
                    _ordersToExport.Add(orderId);
                }
            }
            return _columnMappings.Where(columnMapping => columnMapping.SourceColumn != null).GroupBy(cm => cm.SourceColumn.Name, (key, group) => group.First()).ToDictionary(columnMapping => columnMapping.SourceColumn.Name, columnMapping => _reader[columnMapping.SourceColumn.Name]);
        }

        public static void UpdateExportedOrdersInDb(string orderStateIDAfterExport, SqlConnection connection)
        {
            if (_ordersToExport != null && _ordersToExport.Count > 0)
            {
                //Execute script to update IsExported and OrderStateID columns in Orders Table
                SqlCommand command = new SqlCommand { Connection = connection };
                try
                {
                    if (connection.State.ToString() != "Open")
                        connection.Open();

                    string sql = "UPDATE EcomOrders SET OrderIsExported = 1";

                    if (!string.IsNullOrEmpty(orderStateIDAfterExport))
                    {
                        sql = sql + string.Format(", OrderStateID = '{0}'", orderStateIDAfterExport);
                    }
                    if (_ordersToExport.Count > 100)
                    {
                        var taken = 0;
                        int step = 100;
                        while (taken < _ordersToExport.Count)
                        {
                            var idsCollection = _ordersToExport.Skip(taken).Take(step);
                            string ids = string.Join("','", idsCollection);
                            if (!string.IsNullOrEmpty(ids))
                            {
                                command.CommandText = sql + string.Format(" WHERE [OrderID] IN ('{0}')", ids);
                                command.ExecuteNonQuery();
                                ClearOrderCache(idsCollection);
                            }
                            taken = taken + step;
                        }
                    }
                    else
                    {
                        command.CommandText = sql + string.Format(" WHERE [OrderID] IN ('{0}')", string.Join("','", _ordersToExport));
                        command.ExecuteNonQuery();
                        ClearOrderCache(_ordersToExport);
                    }
                }
                catch (Exception ex)
                {
                    throw new Exception(string.Format("Exception message: {0} Sql query: {1}", ex.Message, command.CommandText), ex);
                }
                finally
                {
                    _ordersConditions = null;
                    _ordersToExport = null;
                }
            }
        }

        private static void ClearOrderCache(IEnumerable<string> orderIds)
        {
            OrderService os = new OrderService();
            foreach (string id in orderIds)
            {
                os.RemoveOrderCache(id);
            }
        }
    }
}

