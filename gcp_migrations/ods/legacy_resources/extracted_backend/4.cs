#region Namespaces
using System;
using System.Data;
using Microsoft.SqlServer.Dts.Runtime;
using System.Windows.Forms;
using System.Collections.Generic;
using System.Text.RegularExpressions;
#endregion

namespace ST_ab90e71829eb43e4a95ba72d3e901bfa
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
            List<String> fileOrders = (List<String>)Dts.Variables["User::fileOrders"].Value;
            Dictionary<string, string> patternTableMap = (Dictionary<string, string>)Dts.Variables["User::patternTableMap"].Value;
            Dictionary<string, string> tableDefOpMap = (Dictionary<string, string>)Dts.Variables["User::tableDefOpMap"].Value;

            List<String> fileOrders2 = new List<string>();
            foreach (String fo in fileOrders)
            {
                // 當前Order拆解成I、IU、D三種 (foEmpty為找不到對應事件之檔案)
                List<String> foi = new List<string>();
                List<String> foiu = new List<string>();
                List<String> fod = new List<string>();
                List<String> foEmpty = new List<string>();

                if (!String.IsNullOrWhiteSpace(fo)) {
                    String[] foArrs = fo.Split(',');

                    // 每個檔案到patternTableMap中搜尋，有符合的檔案(代表需要處理)，則取得事件
                    foreach (String fileName in foArrs)
                    {
                        bool isZip = fileName.ToUpper().Contains(".ZIP") || fileName.ToUpper().Contains(".RAR") || fileName.ToUpper().Contains(".7Z");
                        String rawDataFilePatternStr = fileNameRegexMatch(patternTableMap, fileName, isZip);

                        if (!String.IsNullOrWhiteSpace(rawDataFilePatternStr))
                        {
                            try
                            {
                                string tableName = patternTableMap[rawDataFilePatternStr];
                                Regex srcRawDataFileRegex = new Regex(rawDataFilePatternStr.ToUpper());
                                Match srcRawDataFileMatches = srcRawDataFileRegex.Match(fileName.ToUpper());
                                string writeInEvent = "";

                                // 重檔名取得執行事件，如取沒有則取得預設執行事件
                                if (srcRawDataFileMatches.Success)
                                {
                                    writeInEvent = srcRawDataFileMatches.Groups[1].ToString();
                                }
                                if (String.IsNullOrWhiteSpace(writeInEvent))
                                {
                                    writeInEvent = tableDefOpMap[tableName];
                                }

                                switch (writeInEvent)
                                {
                                    case "I":
                                        foi.Add(fileName);
                                        break;
                                    case "IU":
                                        foiu.Add(fileName);
                                        break;
                                    case "D":
                                        fod.Add(fileName);
                                        break;
                                    default:
                                        foEmpty.Add(fileName);
                                        break;
                                }
                            } catch(Exception ex)
                            {
                                foEmpty.Add(fileName);
                            }
                        } else
                        {
                            foEmpty.Add(fileName);
                        }
                    }

                    List<String> fileOrdersTmp = new List<string>();
                    fileOrdersTmp.AddRange(foi);
                    fileOrdersTmp.AddRange(foiu);
                    fileOrdersTmp.AddRange(fod);
                    fileOrdersTmp.AddRange(foEmpty);
                    fileOrders2.Add(String.Join(",", fileOrdersTmp));
                }
            }

            Dts.Variables["User::fileOrders"].Value = fileOrders2;
            Dts.TaskResult = (int)ScriptResults.Success;
		}

        // 判斷檔名是否有符合pattern，符合傳回pattern String
        public string fileNameRegexMatch(Dictionary<string, string> patternMap, string fileName, bool zipFile)
        {
            string rawDataFilePatternStr = "";
            try
            {
                if (zipFile)
                {
                    fileName = fileName.Remove(fileName.LastIndexOf('('), fileName.LastIndexOf(')') - fileName.LastIndexOf('(') + 1);
                }
                foreach (KeyValuePair<string, string> kvp in patternMap)
                {
                    string pattern = kvp.Key;
                    Regex srcRawDataFileRegex = new Regex(pattern);
                    Match srcRawDataFileMatches = srcRawDataFileRegex.Match(fileName);
                    if (srcRawDataFileMatches.Success)
                    {
                        rawDataFilePatternStr = pattern;
                        break;
                    }
                }
                return rawDataFilePatternStr;
            }
            catch (Exception ex)
            {
                return "";
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