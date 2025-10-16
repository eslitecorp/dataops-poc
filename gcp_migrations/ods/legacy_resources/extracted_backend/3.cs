#region Namespaces
using System;
using System.Data;
using Microsoft.SqlServer.Dts.Runtime;
using System.Windows.Forms;
using System.Data.OleDb;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
#endregion

namespace ST_c551596b80f14010b40912bd1abf9b4d
{
    /// <summary>
    /// ScriptMain is the entry point class of the script.  Do not change the name, attributes,
    /// or parent of this class.
    /// </summary>
	[Microsoft.SqlServer.Dts.Tasks.ScriptTask.SSISScriptTaskEntryPointAttribute]
	public partial class ScriptMain : Microsoft.SqlServer.Dts.Tasks.ScriptTask.VSTARTScriptObjectModelBase
	{
        #region Help:  Using Integration Services variables and parameters in a script
        /* To use a variable in this script, first ensure that the variable has been added to 
         * either the list contained in the ReadOnlyVariables property or the list contained in 
         * the ReadWriteVariables property of this script task, according to whether or not your
         * code needs to write to the variable.  To add the variable, save this script, close this instance of
         * Visual Studio, and update the ReadOnlyVariables and 
         * ReadWriteVariables properties in the Script Transformation Editor window.
         * To use a parameter in this script, follow the same steps. Parameters are always read-only.
         * 
         * Example of reading from a variable:
         *  DateTime startTime = (DateTime) Dts.Variables["System::StartTime"].Value;
         * 
         * Example of writing to a variable:
         *  Dts.Variables["User::myStringVariable"].Value = "new value";
         * 
         * Example of reading from a package parameter:
         *  int batchId = (int) Dts.Variables["$Package::batchId"].Value;
         *  
         * Example of reading from a project parameter:
         *  int batchId = (int) Dts.Variables["$Project::batchId"].Value;
         * 
         * Example of reading from a sensitive project parameter:
         *  int batchId = (int) Dts.Variables["$Project::batchId"].GetSensitiveValue();
         * */

        #endregion

        #region Help:  Firing Integration Services events from a script
        /* This script task can fire events for logging purposes.
         * 
         * Example of firing an error event:
         *  Dts.Events.FireError(18, "Process Values", "Bad value", "", 0);
         * 
         * Example of firing an information event:
         *  Dts.Events.FireInformation(3, "Process Values", "Processing has started", "", 0, ref fireAgain)
         * 
         * Example of firing a warning event:
         *  Dts.Events.FireWarning(14, "Process Values", "No values received for input", "", 0);
         * */
        #endregion

        #region Help:  Using Integration Services connection managers in a script
        /* Some types of connection managers can be used in this script task.  See the topic 
         * "Working with Connection Managers Programatically" for details.
         * 
         * Example of using an ADO.Net connection manager:
         *  object rawConnection = Dts.Connections["Sales DB"].AcquireConnection(Dts.Transaction);
         *  SqlConnection myADONETConnection = (SqlConnection)rawConnection;
         *  //Use the connection in some code here, then release the connection
         *  Dts.Connections["Sales DB"].ReleaseConnection(rawConnection);
         *
         * Example of using a File connection manager
         *  object rawConnection = Dts.Connections["Prices.zip"].AcquireConnection(Dts.Transaction);
         *  string filePath = (string)rawConnection;
         *  //Use the connection in some code here, then release the connection
         *  Dts.Connections["Prices.zip"].ReleaseConnection(rawConnection);
         * */
        #endregion


		/// <summary>
        /// This method is called when this script task executes in the control flow.
        /// Before returning from this method, set the value of Dts.TaskResult to indicate success or failure.
        /// To open Help, press F1.
        /// </summary>
		public void Main()
		{
            String dbTableOrderStr = Dts.Variables["User::dbTableOrder"].Value.ToString(); // 1:CUSL_D,2:CUSL_H,2:CUST_CRD_LOG,2:CUST_CRD_OUT
            List<String> dbTableOrderList = dbTableOrderStr.Split(',').ToList(); // [0]{1:CUSL_D},[1]{2:CUSL_H},[2]{2:CUST_CRD_LOG},[3]{2:CUST_CRD_OUT}
            Dictionary<int, String> dbTableOrderMap = new Dictionary<int, String>(); // {key:1,value:CUSL_D},{key:2,value:CUSL_H,CUST_CRD_LOG,CUST_CRD_OUT}
            foreach (String tableName in dbTableOrderList)
            {
                int orderKey = Int32.Parse(tableName.Split(':').ToArray()[0]);
                if (dbTableOrderMap.ContainsKey(orderKey))
                {
                    dbTableOrderMap[orderKey] = dbTableOrderMap[orderKey] + ',' + tableName.Split(':').ToArray()[1];
                }
                else
                {
                    dbTableOrderMap.Add(Int32.Parse(tableName.Split(':').ToArray()[0]), tableName.Split(':').ToArray()[1]);
                }
            }
            Dictionary<int, String> dbTableOrderSortedMap = dbTableOrderMap.OrderBy(o => o.Key).ToDictionary(o => o.Key, p => p.Value);
            int i = 0;
            List<String> fileOrders = new List<String>();
            Dictionary< String,int> tableOrderMap = new Dictionary<String, int>();
            foreach (KeyValuePair<int, String> k in dbTableOrderSortedMap)
            {
                // initial fileOrders
                fileOrders.Add("");
                // create table vs index map
                List<String> tables = k.Value.ToString().Split(',').ToList();
                foreach (String table in tables) {
                    tableOrderMap.Add(table.ToUpper(),i);
                }
                i++;
            }
            
            Dts.Variables["User::tableOrderMap"].Value = tableOrderMap;
            Dts.Variables["User::fileOrders"].Value = fileOrders;
            Dts.Variables["User::hasConcurrentFiles"].Value = false;
            Dts.TaskResult = (int)ScriptResults.Success;
        }

        #region ScriptResults declaration
        /// <summary>
        /// This enum provides a convenient shorthand within the scope of this class for setting the
        /// result of the script.
        /// 
        /// This code was generated automatically.
        /// </summary>
        enum ScriptResults
        {
            Success = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Success,
            Failure = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure
        };
        #endregion

	}
}