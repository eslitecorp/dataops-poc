#region Namespaces
using System;
using System.Data;
using Microsoft.SqlServer.Dts.Runtime;
using System.Windows.Forms;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Text.RegularExpressions;
#endregion

namespace ST_5ed5a4dadb194cc79022327979d90c1d
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
            string srcCheckFileName = Dts.Variables["User::reSrcCheckFileName"].Value.ToString();
            string srcCheckFilePath = Dts.Variables["User::reSrcCheckFilePath"].Value.ToString();
            string srcEkorg = Dts.Variables["$Package::srcEkorg"].Value.ToString();

            Variables vars = null;
            Dts.VariableDispenser.LockForRead("User::patternTableMap");
            Dts.VariableDispenser.GetVariables(ref vars);
            Dictionary<string, string> patternTableMap = (Dictionary<string, string>)vars["User::patternTableMap"].Value;
            vars.Unlock();

            var encode = new UTF8Encoding(false);
            StringBuilder errorMsg = new StringBuilder();
            using (StreamReader file = new System.IO.StreamReader(@srcCheckFilePath, encode))
            {
                string allLine = file.ReadToEnd();            

                string delimiter = "{CR}{LF}";
                if(allLine.Contains("\r\n"))
                {
                    delimiter = "{CR}{LF}";
                } else if (allLine.Contains("\n"))
                {
                    delimiter = "{LF}";
                }

                file.DiscardBufferedData();
                file.BaseStream.Seek(0, SeekOrigin.Begin);
                file.BaseStream.Position = 0;

                DataTable dt = new DataTable();
                dt.Columns.Add("fileName");
                dt.Columns.Add("rowCount");
                dt.Columns.Add("checkSum");
                dt.Columns.Add("tableName");
                string line;
                //bool firstLine = true;
                int index = 1;
                while ((line = file.ReadLine()) != null)
                {
                    if(index == 1)
                    {
                        line = removeUTF8BOM(line);
                        //firstLine = false;
                    }
                    if (!String.IsNullOrWhiteSpace(line))
                    {
                        string[] sc = line.Split(',');
                        validateFormat(errorMsg, sc, index);

                        DataRow dr = dt.NewRow();
                        string fileName = sc[0];

                        //如採購組織有植，檔案最後補上採購組織
                        if (!String.IsNullOrWhiteSpace(srcEkorg))
                        {
                            string ext = Path.GetExtension(fileName);
                            string preName = Path.GetFileNameWithoutExtension(fileName);
                            fileName = preName + "-" + srcEkorg + ext;
                        }
                        dr["fileName"] = fileName;
                        dr["rowCount"] = sc[1];
                        dr["checkSum"] = sc[2];
                        // 取得此fileName對應table
                        string tableName = fileNameRegexMatch(patternTableMap, fileName);
                        dr["tableName"] = tableName;
                        dt.Rows.Add(dr);
                    }
                    index++;
                }

                file.Close();                
                Dts.Variables["User::checkFileDataTable"].Value = dt;
            }
            if(errorMsg.Length > 0)
            {
                Dts.Variables["User::checkFileFormatErrorMsg"].Value = srcCheckFileName + "檔案內容格式有誤(詳細行數如下):\r\n" + errorMsg.ToString();
                Dts.TaskResult = (int)ScriptResults.Failure;
            } else
            {
                Dts.TaskResult = (int)ScriptResults.Success;
            }
            
		}

        // 驗證checkfile內容
        public void validateFormat(StringBuilder errorMsg, string[] sc, int index)
        {
            StringBuilder rowErrMsg = new StringBuilder();

            if (sc.Length != 3)
            {
                rowErrMsg.Append("<欄位數不符>");
            }

            if(sc[0].Length > 100)
            {
                rowErrMsg.Append("<fileName長度不符>");
            }

            int defv = 0;
            bool result = int.TryParse(sc[1], out defv);
            if(!result)
            {
                rowErrMsg.Append("<rowCount不為integer格式>");
            }

            if (sc[2].Length > 100)
            {
                rowErrMsg.Append("<checkSum長度不符>");
            }
            if (rowErrMsg.Length > 0)
            {
                errorMsg.Append("checkfile第" + index + "列格式錯誤 ");
                errorMsg.Append(rowErrMsg.ToString() + "\r\n");
            } 
        }

        // 判斷檔名是否有符合pattern，符合傳回tableName
        public string fileNameRegexMatch(Dictionary<string, string> patternMap, string fileName)
        {
            string rtnTableName = "";
            try
            {
                foreach (KeyValuePair<string, string> kvp in patternMap)
                {
                    string pattern = kvp.Key;
                    string tableName = kvp.Value;
                    Regex srcRawDataFileRegex = new Regex(pattern);
                    Match srcRawDataFileMatches = srcRawDataFileRegex.Match(fileName);
                    if (srcRawDataFileMatches.Success)
                    {
                        rtnTableName = tableName;
                        break;
                    }
                }
                return rtnTableName;
            }
            catch (Exception ex)
            {
                return "";
            }
        }

        public string removeUTF8BOM(string line)
        {
            if(line.StartsWith("\uFEFF"))
            {
                line = line.TrimStart('\uFEFF');
            }
            return line;
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