#region Namespaces
using System;
using System.IO;
using System.Data;
using Microsoft.SqlServer.Dts.Runtime;
using System.Windows.Forms;
using NPOI.SS.UserModel;
using NPOI.XSSF.UserModel;
using System.Data.OleDb;
#endregion

namespace ST_f28b723f02644bbb883ef06efd8422d7
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
            string detailReportFile = Dts.Variables["User::detailReportFile"].Value.ToString();
            string flowReportFile = Dts.Variables["User::flowReportFile"].Value.ToString();
            string mail_msgNoDataTable = "";
            // User::resultSet_detail,User::resultSet_flow1,User::resultSet_flow2
            OleDbDataAdapter adapter = new OleDbDataAdapter();
            DataTable resultSet_stat = new DataTable();
            DataTable resultSet_detail = new DataTable();
            DataTable resultSet_flow1 = new DataTable();
            DataTable resultSet_flow2 = new DataTable();
            DataTable resultNoDataTable = new DataTable();

            adapter.Fill(resultSet_stat, Dts.Variables["User::resultSet_stat"].Value);
            adapter.Fill(resultSet_detail, Dts.Variables["User::resultSet_detail"].Value);
            adapter.Fill(resultSet_flow1, Dts.Variables["User::resultSet_flow1"].Value);
            adapter.Fill(resultSet_flow2, Dts.Variables["User::resultSet_flow2"].Value);
            adapter.Fill(resultNoDataTable, Dts.Variables["User::resultNoDataTable"].Value);

            try
            {
                this.fixExcelStyle(flowReportFile, 0, 4, resultSet_stat, 0);
                this.fixExcelStyle(flowReportFile, 0, 7, resultSet_flow1, 1);
                this.fixExcelStyle(flowReportFile, 1, 4, resultSet_flow2, 2);
                this.fixExcelStyle(detailReportFile, 0, 4, resultSet_detail, 3);
            }
            catch (Exception ex)
            {
                Dts.Log(ex.Message, 999, null);
            }

            foreach (DataRow r in resultNoDataTable.Rows)
            {
                mail_msgNoDataTable += r[0] + "\r\n" ;
            }
            Dts.Variables["User::mail_msgNoDataTable"].Value = mail_msgNoDataTable;
            Dts.TaskResult = (int)ScriptResults.Success;
		}


        public void fixExcelStyle(String excelPath, int workSheetIndex, int startRowIdx, DataTable dataTable, int type)
        {
            try
            {
                XSSFWorkbook wb;
                using (FileStream fs = new FileStream(excelPath, FileMode.Open, FileAccess.ReadWrite))
                {
                    wb = new XSSFWorkbook(fs);

                    ISheet ws = wb.GetSheetAt(workSheetIndex);

                    // Header處理
                    ws.GetRow(1).GetCell(0).SetCellValue(Dts.Variables["User::excelDateTimeString"].Value.ToString());


                    // detail format 處理
                    IFont font = wb.CreateFont();
                    font.FontName = "微軟正黑體";
                    font.FontHeightInPoints = 8;

                    IRow baseRow = ws.GetRow(startRowIdx);
                    foreach (DataRow dr in dataTable.Rows)
                    {
                        // 取得excel資料列
                        IRow row = ws.GetRow(startRowIdx);
                        if(row == null)
                        {
                            row = ws.CreateRow(startRowIdx);
                        }

                        setRowDataByType(baseRow, row, dr, type, font);
                        startRowIdx++;
                    }

                    // 設定自動欄位寬
                    if (type == 0 || type == 1)
                    {
                        for (int j = 0; j < 15; j++)
                        {
                            if (j == 0 || j == 1 || j == 2 || j == 3 || j == 6 || j == 14)
                                ws.AutoSizeColumn(j);
                        }
                    }
                    else if (type == 2)
                    {
                        for (int j = 0; j < 16; j++)
                        {
                            if (!(j == 7 || j == 8 || j == 11))
                                ws.AutoSizeColumn(j);
                        }
                    }

                    using (FileStream fs2 = new FileStream(excelPath, FileMode.Create, FileAccess.Write))
                    {
                        wb.Write(fs2);
                        fs2.Close();
                        fs2.Dispose();
                    }

                    fs.Close();
                    fs.Dispose();
                }

                wb.Close();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void setRowDataByType(IRow baseRow, IRow row, DataRow dr, int type, IFont font)
        {
            if (type == 0)  // flow sheet1 stat
            {
                ICell cell = setCellFormat(row, 3, null);
                cell.SetCellValue(Int32.Parse(dr[0].ToString()));
                Dts.Variables["User::resultStatErrAmt"].Value = Int32.Parse(dr[0].ToString());
                ICell cell2 = setCellFormat(row, 6, null);
                cell2.SetCellValue(Int32.Parse(dr[1].ToString()));
                ICell cell3 = setCellFormat(row, 9, null);
                cell3.SetCellValue(Int32.Parse(dr[2].ToString()));
            }
            else if (type == 1) // flow sheet1 data
            {
                // 依照sql select 中column順序設定到對應excel (順序需一致!)
                for (int j = 0; j <= 14; j++)
                {
                    ICell cell = setCellFormat(row, j, font);
                    ICell baseCell = baseRow.GetCell(j);
                    try
                    {
                        if (baseCell != null)
                        {
                            cell.SetCellType(baseCell.CellType);    //複製第一個資料列資料型態
                        }

                        if (j >= 4 || j == 2)
                        {
                            // 數值格式
                            int v = Int32.Parse(dr[j].ToString());
                            cell.SetCellValue(v);
                        }
                        else
                        {
                            cell.SetCellValue(dr[j].ToString());
                        }
                    }
                    catch (Exception)
                    {
                    }
                }
            } else if(type == 2) // flow sheet2 data
            {
                // 依照sql select 中column順序設定到對應excel (順序需一致!)
                for (int j = 0; j < 16; j++)
                {
                    ICell cell = setCellFormat(row, j, font);
                    ICell baseCell = baseRow.GetCell(j);
                    try
                    {
                        if (baseCell != null)
                        {
                            cell.SetCellType(baseCell.CellType);    //複製第一個資料列資料型態
                        }

                        if (j == 8)
                        {
                            // 數值格式
                            Double v = Double.Parse(dr[j].ToString());
                            cell.SetCellValue(v);
                        }
                        else if (j == 3 || (j >= 11 && j <= 13))
                        {
                            // 數值格式
                            int v = Int32.Parse(dr[j].ToString());
                            cell.SetCellValue(v);
                        }
                        else
                        {
                            cell.SetCellValue(dr[j].ToString());
                        }
                    }
                    catch (Exception)
                    {
                    }
                }
            }
            else if (type == 3) // detail sheet1 data
            {
                // 依照sql select 中column順序設定到對應excel (順序需一致!)
                for (int j = 0; j < 9; j++)
                {
                    ICell cell = setCellFormat(row, j, font);
                    ICell baseCell = baseRow.GetCell(j);
                    try
                    {
                        if (baseCell != null)
                        {
                            cell.SetCellType(baseCell.CellType);    //複製第一個資料列資料型態
                        }
                        string s = dr[j].ToString();
                        if(j==6)
                        {
                            s = s.Replace("\u0001", " ");
                        }
                        cell.SetCellValue(s);
                    }
                    catch (Exception)
                    {
                    }
                }
            }
        }

        public ICell setCellFormat(IRow row, int colInx, IFont font){
            ICell cell = row.GetCell(colInx);
            if(cell == null)
            {
                cell = row.CreateCell(colInx);
            }
            if(font!=null)
            {
                ICellStyle cloneStyle = cell.CellStyle;
                cloneStyle.VerticalAlignment = VerticalAlignment.Center;
                cloneStyle.Alignment = NPOI.SS.UserModel.HorizontalAlignment.Left;
                cloneStyle.SetFont(font);
                cell.CellStyle.WrapText = true;
            }
            return cell;
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