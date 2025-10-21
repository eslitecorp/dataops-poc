#region Namespaces
using System;
using System.Data;
using Microsoft.SqlServer.Dts.Runtime;
using System.Windows.Forms;
using System.Globalization;
#endregion

namespace ST_325531be07a84c3f9b40f7a02a314619
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
            try
            {
                string tmpFlowFileName = Dts.Variables["User::tmpFlowFileName"].Value.ToString();
                string tmpDetailFileName = Dts.Variables["User::tmpDetailFileName"].Value.ToString();
                string templateExtFileName = Dts.Variables["User::tmpExtFileName"].Value.ToString();
                string templateFilePath = Dts.Variables["User::tmpFilePath"].Value.ToString();

                string sysName = Dts.Variables["$Package::sysName"].Value.ToString();
                string ekorg = Dts.Variables["$Package::srcEkorg"].Value.ToString();
                string datetimeString = string.Format("{0:yyyyMMdd tthhmmss}", DateTime.Now);
                string baseFlowFileName = sysName + (String.IsNullOrWhiteSpace(ekorg) ? "" : ekorg) + Dts.Variables["User::baseFlowFileName"].Value.ToString() + "_" + datetimeString + templateExtFileName;
                string baseDetailFileName = sysName + (String.IsNullOrWhiteSpace(ekorg) ? "" : ekorg) + Dts.Variables["User::baseDetailFileName"].Value.ToString() + "_" + datetimeString  + templateExtFileName;


                string flowReportFile = System.IO.Path.Combine(templateFilePath, baseFlowFileName);
                Dts.Variables["User::flowReportFile"].Value = flowReportFile;
                System.IO.File.Copy(templateFilePath + tmpFlowFileName + templateExtFileName, flowReportFile, true);

                string detailReportFile = System.IO.Path.Combine(templateFilePath, baseDetailFileName);
                Dts.Variables["User::detailReportFile"].Value = detailReportFile;
                System.IO.File.Copy(templateFilePath + tmpDetailFileName + templateExtFileName, detailReportFile, true);

                Dts.TaskResult = (int)ScriptResults.Success;
            }
            catch (Exception e)
            {
                Dts.TaskResult = (int)ScriptResults.Failure;
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