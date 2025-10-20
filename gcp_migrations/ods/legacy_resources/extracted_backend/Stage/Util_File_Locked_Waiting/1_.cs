#region Namespaces
using System;
using System.Data;
using Microsoft.SqlServer.Dts.Runtime;
using System.Windows.Forms;
using System.Text.RegularExpressions;
using System.IO;
using System.Threading;
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
            String fileConnectionString = Dts.Variables["$Package::fileConnectionString"].Value.ToString();

            if (File.Exists(fileConnectionString))
            {
                Boolean fireAgain = false;
                Dts.Events.FireInformation(0, "File Lock Check", "File exists, now checking if it can be opened", string.Empty, 0, ref fireAgain);

                // Boolean variable to prevent endless lock warnings
                Boolean ShowLockWarning = true;

                // Boolean variable needed for the while loop
                Boolean FileLocked = true;
                while (FileLocked)
                {
                    try
                    {
                        Dts.Events.FireWarning(0, "Enter Point", "File locked: " + fileConnectionString, string.Empty, 0);
                        // Check if the file isn't locked by an other process by opening 
                        // the file. If it succeeds, set variable to false and close stream                     
                        FileStream fs = new FileStream(fileConnectionString, FileMode.Open,FileAccess.Read);
                        Dts.Events.FireWarning(0, "Test Point", "File locked: ", string.Empty, 0);
                        // No error so it is not locked
                        Dts.Events.FireInformation(0, "File Lock Check", "File not locked", string.Empty, 0, ref fireAgain);
                        FileLocked = false;

                        // Close the file and exit the Script Task
                        fs.Close();
                        Dts.TaskResult = (int)ScriptResults.Success;
                    }
                    catch (IOException ex)
                    {
                        // If opening fails, it's probably locked by an other process. This is the exact message:
                        // System.IO.IOException: The process cannot access the file 'D:\example.csv' because it is being used by another process. 

                        // Log locked status (once)
                        if (ShowLockWarning)
                        {
                            Dts.Events.FireWarning(0, "File Lock Check", "File locked: " + fileConnectionString + ex.Message, string.Empty, 0);
                        }
                        ShowLockWarning = false;

                        // Wait two seconds before rechecking
                        Thread.Sleep(2000);
                    }
                    catch (Exception ex)
                    {
                        // Catch other unexpected errors and break the while loop
                        Dts.Events.FireError(0, "File Lock Check", "Unexpected error: " + fileConnectionString + ex.Message, string.Empty, 0);
                        Dts.TaskResult = (int)ScriptResults.Failure;
                        break;
                    }
                }
            }
            else
            {
                // File doesn't exist, so no checking possible.
                Dts.Events.FireError(0, "File Lock Check", "File does not exist: " + fileConnectionString, string.Empty, 0);
                Dts.TaskResult = (int)ScriptResults.Failure;
            }
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