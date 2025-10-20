#region Namespaces
using System;
using System.Data;
using Microsoft.SqlServer.Dts.Runtime;
using System.Windows.Forms;
using System.IO;
using System.Collections.Generic;
#endregion

namespace ST_a034dfae22bb4052b02ca6a77d0f9bbb
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
            string zipFileFolder = Dts.Variables["$Package::param_folderPath"].Value.ToString();
            string unzipFileFolder = Dts.Variables["$Package::unzipFileFolder"].Value.ToString();
            string unzipFileTmpFolder = Dts.Variables["User::unzipFileTmpFolder"].Value.ToString();
            string zipFileName = Dts.Variables["$Package::srcRawDataFileName"].Value.ToString();
            string doneFileFolder = Dts.Variables["User::doneFilePath"].Value.ToString();
            var files = getAllFiles(unzipFileTmpFolder);

            if(!Directory.Exists(unzipFileFolder))
            {
                Directory.CreateDirectory(unzipFileFolder);
            }

            // rename all file
            foreach(FileInfo file in files)
            {
                string ext = file.Extension;
                string preName = Path.GetFileNameWithoutExtension(file.Name);
                //File.Move(file.FullName, file.DirectoryName + "\\" + preName + "(" + zipFileName + ")" + ext);
                File.Move(file.FullName, unzipFileFolder + "\\" + preName + "(" + zipFileName + ")" + ext);
            }

            File.Copy(zipFileFolder + "\\" + zipFileName, doneFileFolder + "\\" + zipFileName, true);
            File.Delete(zipFileFolder + "\\" + zipFileName);
            Directory.Delete(unzipFileTmpFolder);
            Dts.TaskResult = (int)ScriptResults.Success;
		}

        public IList<FileInfo> getAllFiles(string filePath)
        {
            DirectoryInfo d = new DirectoryInfo(filePath);
            FileInfo[] files = d.GetFiles("*.*");
            //String[] files = Directory.GetFiles(filePath, "*.*", SearchOption.AllDirectories);
            IList<FileInfo> result = new List<FileInfo>();
            foreach (FileInfo fileInfo in files)
            {
                string file = fileInfo.Name;
                if (file.ToUpper().EndsWith(".CSV") || file.ToUpper().EndsWith(".PUB"))
                {
                    result.Add(fileInfo);
                }
            }
            return result;
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