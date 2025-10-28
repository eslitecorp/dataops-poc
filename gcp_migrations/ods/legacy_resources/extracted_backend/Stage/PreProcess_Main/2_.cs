#region Namespaces
using System;
using Microsoft.SqlServer.Dts.Runtime;
using System.Text.RegularExpressions;
using System.Collections.Generic;
#endregion

namespace ST_fa63f52061d144388042db205dee6fba
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
            string srcSysName = Dts.Variables["$Package::srcSysName"].Value.ToString();
            string srcEkorg = Dts.Variables["$Package::srcEkorg"].Value.ToString();
            string dbTableName = Dts.Variables["$Package::dbTableName"].Value.ToString();
            string fileName = Dts.Variables["$Package::srcRawDataFileName"].Value.ToString();
            Dts.Variables["User::srcRawDataFileName"].Value = fileName;

            String checkFilePatternStr = "ODS" + srcSysName.Split('_')[0].ToUpper() + "(_|\\-)CHECKFILE(_|\\-)([0-9]{8})(.*).CSV";            
            Regex srcCheckFileRegex = new Regex(checkFilePatternStr);
            Match srcCheckFileMatches = srcCheckFileRegex.Match(fileName.ToUpper());
            if (srcCheckFileMatches.Success)
            {
                //string dateYYYYMMDD = srcRawDataFileMatches.Groups[2].ToString();
                //Dts.Variables["User::dateYYYYMMDD"].Value = dateYYYYMMDD;
                Dts.Variables["User::srcCheckFileName"].Value = fileName;
                Dts.Variables["User::srcCheckFilePath"].Value = Dts.Variables["$Package::srcFileFolder"].Value.ToString() + fileName;
                Dts.Variables["User::srcFileType"].Value = 1;
                Dts.TaskResult = (int)ScriptResults.Success;
            }            
            else
            {
                Variables vars = null;
                Dts.VariableDispenser.LockForRead("User::tablePatternMap");
                Dts.VariableDispenser.GetVariables(ref vars);
                Dictionary<string, string> tablePatternMap = (Dictionary<string, string>)vars["User::tablePatternMap"].Value;
                vars.Unlock();
                
                string rawDataFilePatternStr = tablePatternMap[dbTableName.ToUpper()];
                bool isZip = fileName.ToUpper().Contains(".ZIP") || fileName.ToUpper().Contains(".RAR") || fileName.ToUpper().Contains(".7Z");
                if(isZip)
                {
                    fileName = fileName.Remove(fileName.LastIndexOf('('), fileName.LastIndexOf(')') - fileName.LastIndexOf('(') + 1);
                }
                Regex srcRawDataFileRegex = new Regex(rawDataFilePatternStr);
                Match srcRawDataFileMatches = srcRawDataFileRegex.Match(fileName);

                if (srcRawDataFileMatches.Success)
                {
                    Dts.Variables["User::param_fileNamePattern"].Value = rawDataFilePatternStr;
                    string sysName = String.IsNullOrEmpty(srcEkorg) ? srcSysName : srcSysName + "_" + srcEkorg;
                    string dbSchemaName = "prestage_" + sysName;
                    string dateYYYYMMDD = srcRawDataFileMatches.Groups[1].ToString();
                    string dbStageTableName = sysName + "." + dbTableName;
                    Dts.Variables["User::dateYYYYMMDD"].Value = dateYYYYMMDD;
                    Dts.Variables["User::srcFileType"].Value = 0;
                    Dts.Variables["User::packageName"].Value = sysName + "_" + dbTableName + ".dtsx";
                    Dts.Variables["User::dbSchemaName"].Value = dbSchemaName;
                    Dts.Variables["User::dbPrestageTableName"].Value = dbSchemaName + "." + dbTableName;
                    Dts.Variables["User::dbStageTableName"].Value = dbStageTableName;
                    Dts.TaskResult = (int)ScriptResults.Success;
            }
                else
                {
                Dts.TaskResult = (int)ScriptResults.Failure;
                // log or other logic here
            }
        }
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