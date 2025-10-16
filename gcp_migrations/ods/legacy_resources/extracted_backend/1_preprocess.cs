#region Namespaces
using System;
using System.Data;
using Microsoft.SqlServer.Dts.Runtime;
using System.Windows.Forms;
using System.IO;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Text.RegularExpressions;
#endregion

namespace ST_af6001d683ba4f269e0f8f96c2b180e8
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
            string[] totalParamArr = new string[] { "MaxConcurrent", "MailTo", "PoolingTimeOutMins" };
            string srcSysName = Dts.Variables["$Package::srcSysName"].Value.ToString();
            int workDateFlag = Int32.Parse(Dts.Variables["User::param_workDateFlag"].Value.ToString());
            string emailTilte = Dts.Variables["$Project::EmailTilte"].Value.ToString();
            List<string> dbParamNameList = (List<string>)Dts.Variables["User::dbParamNameList"].Value;
            string alertMsg = "";

            // -----------------------
            // 1. USER_PARAM參數檢查
            // -----------------------

            int headerSkipRowsNo = Int32.Parse(Dts.Variables["User::param_headerSkipRowsNo"].Value.ToString());
            int columnNameRowNo = Int32.Parse(Dts.Variables["User::param_columnNameRowNo"].Value.ToString());
            if (headerSkipRowsNo < columnNameRowNo)
            {
                Dts.TaskResult = (int)ScriptResults.Failure;
                Dts.Variables["User::mail_subject"].Value = emailTilte + "組態值錯誤-" + DateTime.Now.ToString("yyyyMMdd");
                Dts.Variables["User::mail_messageSource"].Value = srcSysName + "組態值設定錯誤，ColumnNameRowNo必需小於等於HeaderSkipRowsNo";
                return;
            }

            // 是否Daily Initial Job執行成功
            if (workDateFlag != 1)
            {
                Dts.TaskResult = (int)ScriptResults.Failure;
                Dts.Variables["User::mail_subject"].Value = emailTilte + "組態值錯誤-" + DateTime.Now.ToString("yyyyMMdd");
                Dts.Variables["User::mail_messageSource"].Value = "Daily Initial Job紀錄批號未正常更新!";
                return;
            }
            
            // -----------------------
            // 2. FileNamePattern
            // -----------------------
            // 建立table個別pattern Map對應
            OleDbDataAdapter adapter = new OleDbDataAdapter();
            DataTable fnpDataTable = new DataTable();
            adapter.Fill(fnpDataTable, Dts.Variables["User::dbResultSet_STA"].Value);

            Dictionary<string, string> patternTableMap = new Dictionary<string, string>();
            Dictionary<string, string> tablePatternMap = new Dictionary<string, string>();
            Dictionary<string, string> tableDefOpMap = new Dictionary<string, string>();
            Boolean patternError = false;
            String patternMsg = "";
            foreach (DataRow row in fnpDataTable.Rows)
            {
                string tableName = row[0].ToString().ToUpper();
                string fnp = row[1].ToString();
                string zfnp = row[2].ToString();
                string defOp = row[3].ToString();
                if (String.IsNullOrEmpty(fnp))
                {
                    patternMsg += "組態值:" + srcSysName + "的FILE_NAME_PATTERN設定錯誤,未設定值";
                    patternError = true;
                    break;
                }
                else
                {
                    try
                    {
                        new Regex(fnp);
                    }
                    catch (Exception)
                    {
                        patternMsg += "組態值:" + srcSysName + "中" + tableName + "的FILE_NAME_PATTERN設定錯誤,不合法的pattern";
                        patternError = true;
                        break;
                    }
                }
                if (patternTableMap.ContainsKey(fnp))
                {
                    patternMsg += "組態值:" + srcSysName + "中的FILE_NAME_PATTERN設定錯誤,不能有重複的pattern，" + fnp + "值重複";
                    patternError = true;
                    break;
                }
                patternTableMap.Add(fnp, tableName);
                tablePatternMap.Add(tableName, fnp);
                tableDefOpMap.Add(tableName, defOp);
            }

            if (patternError)
            {
                Dts.TaskResult = (int)ScriptResults.Failure;
                Dts.Variables["User::mail_subject"].Value = emailTilte + "組態值設定錯誤-" + DateTime.Now.ToString("yyyyMMdd");
                Dts.Variables["User::mail_messageSource"].Value = patternMsg;
                return;
            }

            Dts.Variables["User::patternTableMap"].Value = patternTableMap;
            Dts.Variables["User::tablePatternMap"].Value = tablePatternMap;
            Dts.Variables["User::tableDefOpMap"].Value = tableDefOpMap;

            // -----------------------
            // 3. 檢查是否有參數未設定
            // -----------------------
            Boolean errorFlag = false;
            foreach (string paramName in totalParamArr)
            {
                if (!dbParamNameList.Contains(paramName))
                {
                    Dts.Log(paramName + "未設定", 999, null);
                    alertMsg = alertMsg + //
                        (errorFlag ? "、" : "") + //
                        (paramName == "MaxConcurrent" ? srcSysName + "的" : "") + //
                        paramName;
                    errorFlag = true;
                }
            }

            if (errorFlag)
            {
                Dts.TaskResult = (int)ScriptResults.Failure;
                Dts.Variables["User::mail_subject"].Value = emailTilte + "組態值未設定，系統發生錯誤-" + DateTime.Now.ToString("yyyyMMdd");
                Dts.Variables["User::mail_messageSource"].Value = "組態值:" + alertMsg + "未設定";
                return;
            }

            // -------------------------------
            // 4. 檢查設定的資料夾路徑是否有錯
            // -------------------------------
            Boolean folderPathError = false;
            String folderPathMsg = "";
            string folderPath = Dts.Variables["User::param_folderPath"].Value.ToString();

            Dts.Log(srcSysName + "FolderPath：" + folderPath, 999, null);
            if (folderPath.Contains(","))
            {
                folderPathMsg += "組態值:" + srcSysName + "的FolderPath設定錯誤 (資料夾路徑不一致)";
                folderPathError = true;
            }
            else if (!System.IO.Directory.Exists(folderPath))
            {
                folderPathMsg += "組態值:" + srcSysName + "的FolderPath設定錯誤,無此路徑" + folderPath + (folderPathError ? "\r\n" : "");
                folderPathError = true;
            }

            // -------------------------------
            // 5. 建立預設資料夾
            // -------------------------------
            string patternNotMatchedFileFolder = Dts.Variables["User::patternNotMatchedFileFolder"].Value.ToString();
            string srcOngoingFileFolder = Dts.Variables["User::srcOngoingFileFolder"].Value.ToString();
            string srcErrorFileFolder = Dts.Variables["User::srcErrorFileFolder"].Value.ToString();
            string srcDoneFileFolder = Dts.Variables["User::srcDoneFileFolder"].Value.ToString();
            if (!Directory.Exists(patternNotMatchedFileFolder))
            {
                Directory.CreateDirectory(patternNotMatchedFileFolder);
            }
            if (!Directory.Exists(srcOngoingFileFolder))
            {
                Directory.CreateDirectory(srcOngoingFileFolder);
            }
            if (!Directory.Exists(srcErrorFileFolder))
            {
                Directory.CreateDirectory(srcErrorFileFolder);
            }
            if (!Directory.Exists(srcDoneFileFolder))
            {
                Directory.CreateDirectory(srcDoneFileFolder);
            }

            if (!folderPathError)
            {
                Dts.TaskResult = (int)ScriptResults.Success;
            }
            else
            {
                Dts.TaskResult = (int)ScriptResults.Failure;
                Dts.Variables["User::mail_subject"].Value = emailTilte + "組態值設定錯誤-" + DateTime.Now.ToString("yyyyMMdd");
                Dts.Variables["User::mail_messageSource"].Value = folderPathMsg;
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