using System;
using Direct.Shared;
using Direct.Shared.Library;
using System.Data;
using System.Data.SqlClient;
using log4net;

namespace Direct.DBOps.Library
{
    [DirectSealed]
    [DirectDom("DBOps Library")]
    [ParameterType(false)]
    public class DBOps
    {

        private static readonly ILog logArchitect = LogManager.GetLogger(Loggers.LibraryObjects);


        #region DBOPS
        [DirectDom("Set DB Connection connection string")]
        [DirectDomMethod("Update {DB Connection} with {Connection String} ")]
        [MethodDescriptionAttribute("Populates a business entity according to values in an XML format.")]
        public static void UpdateDBConnection(Direct.Shared.DbConnectionComponentBase DBConnection, string connectionString)
        {
            if (logArchitect.IsDebugEnabled)
            {
                logArchitect.DebugFormat("DBOps.UpdateDBConnection - Changing connection string from {0}", DBConnection.ConnectionString);
            }
            DBConnection.ConnectionString = connectionString;
        }


        private static int _bufferSize = 1024;
        static readonly ILog LogDeveloper = LogManager.GetLogger(Loggers.LibraryObjects);

        [DirectDom]
        public delegate void SPEventHander(IDirectComponentBase sender, SPEeventArgs spe);

        [DirectDom("Executed Asynchronous")]
        public static event SPEventHander Executed;

        internal delegate DirectCollection<SPParam> ExecuteQuery(DbConnectionComponent connection, string storedProcedure, DirectCollection<SPParam> oParams);

        [DirectDom("Execute Stored Procedure Asynchronous")]
        [DirectDomMethod("Execute Stored Procedure Asynchronous using {Connection} with the following {Name} and {parameters}")]
        public static void ExcecuteSPAsync(DbConnectionComponent connection, string storedProcedure, DirectCollection<SPParam> spParams)
        {
            ExecuteQuery dlgt = new ExecuteQuery(ExecuteStoredProcedure);
            dlgt.BeginInvoke(connection, storedProcedure, spParams, new AsyncCallback(ExcecuteSPCallback), null);
        }

        static void ExcecuteSPCallback(IAsyncResult asyncResult)
        {
            ExecuteQuery dlgt = (ExecuteQuery)((System.Runtime.Remoting.Messaging.AsyncResult)asyncResult).AsyncDelegate;
            DirectCollection<SPParam> oParams = dlgt.EndInvoke(asyncResult);
            if (Executed != null)
                Executed(null, new SPEeventArgs(true, oParams));
        }

        [DirectDom("Set Buffer Size")]
        [DirectDomMethod("Set buffer to {size}")]
        public static void SetBufferSize(int size)
        {
            if (LogDeveloper.IsDebugEnabled)
            {
                LogDeveloper.DebugFormat("DBOps.SetBufferSize - About to set buffer size");
            }
            _bufferSize = size;
        }


        [DirectDom("Execute Stored Procedure")]
        [DirectDomMethod("Execute Stored Procedure using {Connection} with the following {Name} and {parameters}")]
        public static DirectCollection<SPParam> ExecuteStoredProcedure(DbConnectionComponent connection, string storedProcedure, DirectCollection<SPParam> spParams)
        {
            //connection.ConnectionString
            if (LogDeveloper.IsDebugEnabled)
                LogDeveloper.DebugFormat("DBOps.ExecuteStoredProcedure - Connection Details provider {0}, connection string {1}", connection.Provider, connection.ConnectionString);

            if (connection.Provider != DbConnectionComponent.Providers.Sql)
            {
                //We only support Oracle Connection.
                LogDeveloper.ErrorFormat("DBOps.ExecuteStoredProcedure - The given DB connection was not SQL. Provider is {0}", connection.Provider);
                return new DirectCollection<SPParam>();
            }

            if (string.IsNullOrEmpty(storedProcedure))
            {
                LogDeveloper.Error("DBOps.ExecuteStoredProcedure - The given Stored Procedure was empty");
                return new DirectCollection<SPParam>();
            }

            return ExecuteSP(connection, storedProcedure, spParams); 
        }

        private static DirectCollection<SPParam> ExecuteSP(DbConnectionComponent connection, string storedProcedure, DirectCollection<SPParam> oParams)
        {
            SqlConnection cn = null;
            if (string.IsNullOrEmpty(storedProcedure))
            {
                LogDeveloper.Error("DBOps.ExecuteStoredProcedure - Procedure name can't be empty.");
                return new DirectCollection<SPParam>();
            }
            try
            {
                cn = new SqlConnection(connection.ConnectionString);
                SqlCommand sqlCommand = new SqlCommand(storedProcedure, cn);
                sqlCommand.CommandType = CommandType.StoredProcedure;
                if (LogDeveloper.IsInfoEnabled)
                    LogDeveloper.Info("DBOps.ExecuteStoredProcedure - about to parse and add parameters");

                bool fail;
                foreach (SPParam par in oParams)
                {
                    SqlParameter param;
                    SqlDbType sqlDBTyp = Trans(par.Type, out fail);
                    if (fail)
                    {
                        LogDeveloper.ErrorFormat("DBOps.ExecuteStoredProcedure - DB Type Conversion Failed.");

                        return new DirectCollection<SPParam>();
                    }

                    if (LogDeveloper.IsDebugEnabled)
                        LogDeveloper.DebugFormat("DBOps.ExecuteStoredProcedure - Adding param name {0} of type {1} and value {2}", par.Name, par.Type, par.Value);

                    param = new SqlParameter(par.Name, sqlDBTyp, _bufferSize);

                    param.Value = par.Value;

                    if (par.OutputParameter)
                    {

                        param.Direction = ParameterDirection.InputOutput;

                        if (LogDeveloper.IsDebugEnabled)
                            LogDeveloper.DebugFormat("DBOps.ExecuteStoredProcedure - Parameter is out parameter.");

                        // op.Size = _bufferSize;

                    }
                    else
                    {
                        param.Direction = ParameterDirection.Input;
                    }

                    sqlCommand.Parameters.Add(param);
                }

                if (LogDeveloper.IsDebugEnabled)
                    LogDeveloper.DebugFormat("DBOps.ExecuteStoredProcedure - About to execute command.");

                cn.Open();
                sqlCommand.ExecuteNonQuery();
                cn.Close();
                DirectCollection<SPParam> ret = new DirectCollection<SPParam>();

                foreach (SqlParameter par in sqlCommand.Parameters)
                {
                    if ((par.Direction == ParameterDirection.Output) || (par.Direction == ParameterDirection.InputOutput))
                        ret.Add(new SPParam(par.ParameterName, par.SqlDbType.ToString(), par.Value.ToString()));
                }

                return ret;
            }
            catch (Exception e)
            {
                LogDeveloper.Error("DBOps.ExecuteStoredProcedure - Error. ", e);
                if (cn != null && cn.State == ConnectionState.Open)
                    cn.Close();
                return new DirectCollection<SPParam>();
            }
            finally
            {
                if (cn != null && cn.State == ConnectionState.Open)
                    cn.Close();
            }
        }

        private static SqlDbType Trans(string paramType, out bool fail)
        {
            fail = false;

            if (string.IsNullOrEmpty(paramType))
            {
                LogDeveloper.Error("DBOps.trans - The given parameter type was empty, it must be specified.");
                fail = true;
                return SqlDbType.BigInt;
            }

            switch (paramType.ToUpper())
            {
                case "BIGINT":
                    return SqlDbType.BigInt;
                case "BINARY":
                    return SqlDbType.Binary;
                case "BIT":
                    return SqlDbType.Bit;
                case "CHAR":
                    return SqlDbType.Char;
                case "DATETIME":
                    return SqlDbType.DateTime;
                case "DECIMAL":
                    return SqlDbType.Decimal;
                case "FLOAT":
                    return SqlDbType.Float;
                case "IMAGE":
                    return SqlDbType.Image;
                case "INT":
                    return SqlDbType.Int;
                case "MONEY":
                    return SqlDbType.Money;
                case "NCHAR":
                    return SqlDbType.NChar;
                case "NTEXT":
                    return SqlDbType.NText;
                case "NVARCHAR":
                    return SqlDbType.NVarChar;
                case "REAL":
                    return SqlDbType.Real;
                case "UNIQUEIDENTIFIER":
                    return SqlDbType.UniqueIdentifier;
                case "SMALLDATETIME":
                    return SqlDbType.SmallDateTime;
                case "SMALLINT":
                    return SqlDbType.SmallInt;
                case "SMALLMONEY":
                    return SqlDbType.SmallMoney;
                case "TEXT":
                    return SqlDbType.Text;
                case "TIMESTAMP":
                    return SqlDbType.Timestamp;
                case "TINYINT":
                    return SqlDbType.TinyInt;
                case "VARBINARY":
                    return SqlDbType.VarBinary;
                case "VARCHAR":
                    return SqlDbType.VarChar;
                case "VARIANT":
                    return SqlDbType.Variant;
                case "XML":
                    return SqlDbType.Xml;
                case "UDT":
                    return SqlDbType.Udt;
                case "STRUCTURED":
                    return SqlDbType.Structured;
                case "DATE":
                    return SqlDbType.Date;
                case "TIME":
                    return SqlDbType.Time;
                case "DATETIME2":
                    return SqlDbType.DateTime2;
                case "DATETIMEOFFSET":
                    return SqlDbType.DateTimeOffset;
                default:
                    break;
            }

            //If we got here it means that the user Did not use the correct data types.
            LogDeveloper.ErrorFormat("DBOps.trans - The given parameter type was recognized. Parameter was {0}", paramType);
            fail = true;
            return SqlDbType.BigInt;
        }
        #endregion DBOPS

        [DirectDom("SP Parameter", "Custom Utility Library", false)]
        public class SPParam : DirectComponentBase
        {
            protected PropertyHolder<string> _Name = new PropertyHolder<string>("Name");
            protected PropertyHolder<string> _Type = new PropertyHolder<string>("Type");
            protected PropertyHolder<string> _Value = new PropertyHolder<string>("Value");
            protected PropertyHolder<bool> _OutputParameter = new PropertyHolder<bool>("OutputParameter");

            [DirectDom("Name")]
            public string Name
            {
                get { return _Name.TypedValue; }
                set { this._Name.TypedValue = value; }
            }

            [DirectDom("Type")]
            public string Type
            {
                get { return _Type.TypedValue; }
                set { this._Type.TypedValue = value; }
            }

            [DirectDom("Value")]
            public string Value
            {
                get { return _Value.TypedValue; }
                set { this._Value.TypedValue = value; }
            }

            [DirectDom("Output Parameter")]
            [DesignTimeInfo("Output Parameter")]
            public bool OutputParameter
            {
                get { return _OutputParameter.TypedValue; }
                set { this._OutputParameter.TypedValue = value; }
            }

            public SPParam()
            {
            }

            public SPParam(string name, string type, string value)
            {
                //This Constructor is only used for return type so output is always true.
                Name = name;
                Type = type;
                Value = value;
                OutputParameter = true;
            }

            public SPParam(Direct.Shared.IProject project)
                : base(project)
            {
            }
        }

        [DirectSealed]
        [DirectDom]
        public class SPEeventArgs : DirectEventArgs
        {
            bool succeeded;
            DirectCollection<SPParam> oParams;

            public SPEeventArgs(bool succeeded, DirectCollection<SPParam> oParams)
            {
                this.succeeded = succeeded;
                this.oParams = oParams;
            }


            [DirectDom("Succeeded")]
            public bool Succeeded
            {
                get { return succeeded; }
            }


            [DirectDom("Returning Parameters")]
            public DirectCollection<SPParam> OParams
            {
                get { return oParams; }
            }
        }




    }
}
