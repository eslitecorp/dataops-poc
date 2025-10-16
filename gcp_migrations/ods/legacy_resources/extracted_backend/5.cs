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
using System.IO;
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
            // 1. 先取得資料表順序
            Dictionary<String, int> tableOrderMap = (Dictionary<String, int>)Dts.Variables["User::tableOrderMap"].Value;
            Boolean isZipFile = Convert.ToBoolean(Dts.Variables["User::isZipFile"].Value.ToString());

            // 2. 確認每個檔案的檔案類型
            string srcSysName = Dts.Variables["$Package::srcSysName"].Value.ToString().Split('_')[0];
            String checkFilePatternStr = "ODS" + srcSysName.ToUpper() + "(_|\\-)CHECKFILE(_|\\-)([0-9]{8})(.*).CSV";            
            Regex srcCheckFileRegex = new Regex(checkFilePatternStr);

            // 3.取得table個別pattern Map對應
            Dictionary<string, string> patternTableMap = (Dictionary<string, string>)Dts.Variables["User::patternTableMap"].Value;
            Dts.Variables["User::srcFileType"].Value = 0;

            // 4.取得folder相關參數
            HashSet<String> fileNotMatchedHashSet = (HashSet<String>)Dts.Variables["User::fileNotMatchedHashSet"].Value;
            string folderPath = Dts.Variables["User::param_folderPath"].Value.ToString();

            try
            {
                if (isZipFile)
                {
                    string unzipFileFolder = Dts.Variables["User::unzipFileFolder"].Value.ToString();
                    string zipFileName = Dts.Variables["User::srcFileName"].Value.ToString();

                    var files = getAllFiles(unzipFileFolder);

                    // rename all file
                    List<String> fileOrders = (List<String>)Dts.Variables["User::fileOrders"].Value;
                    foreach (FileInfo file in files)
                    {
                        Boolean matchedFile = false;
                        string rawDataFilePatternStr = fileNameRegexMatch(patternTableMap, file.Name, true);
                        if (!String.IsNullOrEmpty(rawDataFilePatternStr))
                        {
                            Dts.Variables["User::srcFileType"].Value = Dts.Variables["User::param_srcFileTypeRawData"].Value;

                            string srcTableName = patternTableMap[rawDataFilePatternStr];
                            int index = tableOrderMap[srcTableName];
                            if (fileOrders[index].Length == 0)
                            {
                                fileOrders[index] = file.Name;
                            }
                            else
                            {
                                combineFileOrders(fileOrders, index, file.Name);
                            }
                            Dts.Variables["User::hasConcurrentFiles"].Value = true;
                            matchedFile = true;
                        }
                        if(!matchedFile)
                        {
                            fileNotMatchedHashSet.Add(file.FullName);
                        }
                    }                    
                    Dts.Variables["User::fileOrders"].Value = fileOrders;
                } else
                {
                    Boolean matchedFile = false;
                    string fileName = Dts.Variables["User::srcFileName"].Value.ToString();
                    string rawDataFilePatternStr = fileNameRegexMatch(patternTableMap, fileName, false);
                    Match srcCheckFileMatches = srcCheckFileRegex.Match(Dts.Variables["User::srcFileName"].Value.ToString().ToUpper());
                    if (srcCheckFileMatches.Success)
                    {
                        Dts.Variables["User::srcFileType"].Value = Dts.Variables["User::param_srcFileTypeCheckFile"].Value;
                        matchedFile = true;
                    }
                    else if (!String.IsNullOrEmpty(rawDataFilePatternStr))
                    {
                        // 檔名與pattern比對看是否需要處理
                        Dts.Variables["User::srcFileType"].Value = Dts.Variables["User::param_srcFileTypeRawData"].Value;
                        List<String> fileOrders = (List<String>)Dts.Variables["User::fileOrders"].Value;
                        string srcTableName = patternTableMap[rawDataFilePatternStr];
                        int index = tableOrderMap[srcTableName];
                        if (fileOrders[index].Length == 0)
                        {
                            fileOrders[index] = Dts.Variables["User::srcFileName"].Value.ToString();
                        }
                        else
                        {
                            fileOrders[index] = fileOrders[index] + "," + Dts.Variables["User::srcFileName"].Value.ToString();
                        }
                        Dts.Variables["User::hasConcurrentFiles"].Value = true;
                        Dts.Variables["User::fileOrders"].Value = fileOrders;
                        matchedFile = true;
                    }
                    if (!matchedFile)
                    {
                        fileNotMatchedHashSet.Add(folderPath + fileName);
                    }
                }
            }
            catch (Exception ex)
            {
            }

            Dts.Variables["User::fileNotMatchedHashSet"].Value = fileNotMatchedHashSet;
            Dts.TaskResult = (int)ScriptResults.Success;
        }

        public void combineFileOrders(List<String> fileOrders, int index, string fileName)
        {
            List< string> fosList = fileOrders[index].Split(',').ToList();

            if(!fosList.Contains(fileName))
            {
                fileOrders[index] = fileOrders[index] + "," + fileName;
            }
        }

        // 判斷檔名是否有符合pattern，符合傳回pattern String
        public string fileNameRegexMatch(Dictionary<string, string> patternMap, string fileName, bool zipFile)
        {
            string rawDataFilePatternStr = "";
            if(zipFile)
            {
                fileName = fileName.Remove(fileName.LastIndexOf('('), fileName.LastIndexOf(')') - fileName.LastIndexOf('(') + 1);
            }
            foreach(KeyValuePair<string, string> kvp in patternMap)
            {
                string pattern = kvp.Key;
                // zip file pattern .前加入zip file name以符合rename後檔名
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