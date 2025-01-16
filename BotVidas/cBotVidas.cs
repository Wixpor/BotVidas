using Newtonsoft.Json.Linq;
using SQLiteHelper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Globalization;
using System.Linq;
using System.Net.Http.Headers;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using static System.Windows.Forms.VisualStyles.VisualStyleElement.StartPanel;
using static System.Windows.Forms.VisualStyles.VisualStyleElement;
using System.Windows.Forms;
using System.DirectoryServices.ActiveDirectory;
using static System.Windows.Forms.VisualStyles.VisualStyleElement.TrackBar;
using System.Threading;
using static System.Net.Mime.MediaTypeNames;


namespace BotVidas
{
    public class cBotVidas
    {

        private static SQLiteDatabaseManager dbManager = new SQLiteDatabaseManager("C:\\BotGhl2Notion\\BatchNotion.db");
        private static string tableConfiguration = "Configuration";
        private static string tableConfigurationDefinition = @"
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    HourPerLife BIGINT NOT NULL
                                ";
        private static string tableSMS = "messageOutBound";
        private static string tableCallLogs = "CallLogs";

        private static string tableLastLifeContacts = "LastLifeContacts";
        private static string tableLastLifeContactsDefinition = @"
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                ContactId TEXT NOT NULL,
                                DateLastLife DATETIME NOT NULL,
                                SMS	INTEGER NOT NULL DEFAULT 0,
	                            CALL INTEGER NOT NULL DEFAULT 0
                                ";



        private static string tableLastLifeContactsLDL = "LastLifeContactsLDL";
        private static string tableLastLifeContactsDefinitionLDL = @"
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                ContactId TEXT NOT NULL,
                                DateLastLife DATETIME NOT NULL,
                                SMS	INTEGER NOT NULL DEFAULT 0,
	                            CALL INTEGER NOT NULL DEFAULT 0
                                ";


        private static readonly string NotionToken = "secret_HeMB4Rd9etcmDb1jOKIY4gEhdFYZLLPdyXQyQVqnuNS";
        private static readonly string databaseIdVidas = "d6d2748af21c4bca8c95bd7c5452016c";  //"169634efa4688009836ccce92a6678c5"; DB de pruebas             d6d2748af21c4bca8c95bd7c5452016c : producción
        private static readonly string databaseIdVidasLDL = "c54490c2ab5f407c8971970aab3b6454";      //                                                       c54490c2ab5f407c8971970aab3b6454 : producción

        public static bool stopRequested = false;
        private static readonly string bearerTokenGHL = "pit-2af37261-968e-4764-a16f-00bd69676be0";
        private static readonly string baseUrlGHL = "https://services.leadconnectorhq.com";

        private static DataGridView _logGrid;
        private static readonly object _logLock = new object();

        public void CancelProcess()
        {
            stopRequested = true;
            AddLog("El proceso ha sido detenido.");
        }

        public cBotVidas(DataGridView logGrid)
        {
            _logGrid = logGrid;
        }

        public async void principal()
        {

            dbManager.CreateTable(tableLastLifeContacts, tableLastLifeContactsDefinition);
            dbManager.CreateTable(tableLastLifeContactsLDL, tableLastLifeContactsDefinitionLDL);
            AddLog("Se inicia bot");
            stopRequested=false;

            while (!stopRequested)
            {
                try
                {

                    AddLog("Buscando contactos para agregar vidas");



                    var limitDateSMS = dbManager.SelectData($"SELECT CAST(max(datemessage) as TEXT) As MAXDATEMESSAGE FROM {tableSMS} WHERE Syncro=0");
                    var listLastSMSperContacts = LastSMSperContacts();
                    var listLastSMSperContactsLDL = LastSMSperContactsLDL();



                    int HourPerLife = 0;
                    var exist = dbManager.SelectData($"SELECT HourPerLife FROM {tableConfiguration}");
                    if (exist != null && exist.Rows.Count > 0)
                    {
                        var value = exist.Rows[0]["HourPerLife"];

                        if (value != DBNull.Value)
                        {
                            if (int.TryParse(value.ToString(), out int result))
                            {
                                HourPerLife = result;
                            }
                            else
                            {
                                throw new InvalidCastException($"No se pudo convertir el valor '{value}' a un entero.");
                            }
                        }
                        else
                        {
                            throw new InvalidOperationException("El campo HourPerLife es nulo en la base de datos.");
                        }
                    }


                    AddLog($"Se buscará {listLastSMSperContacts.Rows.Count} de contactos (SMS) en Notion");
                    var countLife2Notion = 0;

                    int batchSize = 20; // Tamaño del lote para procesar contactos en paralelo
                    var tasks = new List<Task>();

                    foreach (var batch in listLastSMSperContacts.AsEnumerable().Chunk(batchSize))
                    {
                        if (stopRequested) break;

                        var batchTasks = batch.Select(async row =>
                        {

                            string contactId = row["ContactId"].ToString();
                            string dateMessageStr = row["DateMessage"].ToString();
                            string dateLastLifeStr = row["DateLastLife"].ToString();
                            string TypeMessage = row["Source"].ToString();
                            string SMS = row["SMS"] != DBNull.Value ? row["SMS"].ToString() : "0";
                            string call = row["CALL"] != DBNull.Value ? row["CALL"].ToString() : "0";

                            string format = "MM/dd/yyyy HH:mm:ss";

                            try
                            {
                                DateTime parsedDate = DateTime.ParseExact(dateMessageStr, format, CultureInfo.InvariantCulture);
                                string formattedDate = parsedDate.ToString("yyyy-MM-ddTHH:mm:ss");

                                if (dateLastLifeStr== "SIN CARGAR")
                                {
                                    if (await LoadLifeNotion(contactId, TypeMessage, formattedDate, true))
                                    {
                                        Interlocked.Increment(ref countLife2Notion);
                                    }

                                    dbManager.ExecuteNonQuery($"UPDATE {tableSMS} SET Syncro=1 WHERE ContactId='{contactId}' AND Syncro=0");
                                    dbManager.InsertData(tableLastLifeContacts, "ContactId,DateLastLife,SMS", $"'{contactId}','{dateMessageStr}',1");
                                    return;
                                }

                                if (DateTime.TryParseExact(dateMessageStr, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dateMessage) &&
                                    DateTime.TryParseExact(dateLastLifeStr, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dateLastLife))
                                {
                                    if (dateMessage > dateLastLife.AddHours(HourPerLife))
                                    {
                                        if (await LoadLifeNotion(contactId, TypeMessage, formattedDate, true))
                                        {
                                            Interlocked.Increment(ref countLife2Notion);
                                        }

                                        dbManager.ExecuteNonQuery($"UPDATE {tableSMS} SET Syncro=1 WHERE ContactId='{contactId}' AND Syncro=0");
                                        dbManager.ExecuteNonQuery($"UPDATE {tableLastLifeContacts} SET DateLastLife='{dateMessageStr}',SMS=1,CALL=0 WHERE ContactId='{contactId}'");
                                    }
                                    else if (SMS == "0")
                                    {
                                        if (await LoadLifeNotion(contactId, TypeMessage, formattedDate, false))
                                        {
                                            AddLog($"Se actualizó hora mensaje en notion PQF de contacto ({contactId})");
                                        }

                                        dbManager.ExecuteNonQuery($"UPDATE {tableSMS} SET Syncro=1 WHERE ContactId='{contactId}' AND Syncro=0");
                                        dbManager.ExecuteNonQuery($"UPDATE {tableLastLifeContacts} SET SMS=1 WHERE ContactId='{contactId}'");
                                    }
                                }
                                else
                                {
                                    Console.WriteLine($"ContactId: {contactId} - No se pudo convertir una de las fechas.");
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Error procesando ContactId: {contactId} - {ex.Message}");
                            }
                        });

                        tasks.AddRange(batchTasks);
                        await Task.WhenAll(batchTasks); 
                    }

                    await Task.WhenAll(tasks);


                    foreach (var batch in listLastSMSperContactsLDL.AsEnumerable().Chunk(batchSize))
                    {
                        if (stopRequested) break;

                        var batchTasks = batch.Select(async row =>
                        {

                            string contactId = row["ContactId"].ToString();
                            string dateMessageStr = row["DateMessage"].ToString();
                            string dateLastLifeStr = row["DateLastLife"].ToString();
                            string TypeMessage = row["Source"].ToString();
                            string SMS = row["SMS"] != DBNull.Value ? row["SMS"].ToString() : "0";
                            string call = row["CALL"] != DBNull.Value ? row["CALL"].ToString() : "0";

                            string format = "MM/dd/yyyy HH:mm:ss";

                            try
                            {
                                DateTime parsedDate = DateTime.ParseExact(dateMessageStr, format, CultureInfo.InvariantCulture);
                                string formattedDate = parsedDate.ToString("yyyy-MM-ddTHH:mm:ss");

                                if (dateLastLifeStr == "SIN CARGAR")
                                {

                                    if (await LoadLifeNotionLDL(contactId, TypeMessage, formattedDate, true))
                                    {
                                        Interlocked.Increment(ref countLife2Notion);
                                    }

                                    dbManager.ExecuteNonQuery($"UPDATE {tableSMS} SET Syncro=1 WHERE ContactId='{contactId}' AND Syncro=0");
                                    dbManager.InsertData(tableLastLifeContactsLDL, "ContactId,DateLastLife,SMS", $"'{contactId}','{dateMessageStr}',1");
                                    return;
                                }

                                if (DateTime.TryParseExact(dateMessageStr, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dateMessage) &&
                                    DateTime.TryParseExact(dateLastLifeStr, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dateLastLife))
                                {
                                    if (dateMessage > dateLastLife.AddHours(HourPerLife))
                                    {
                                        if (await LoadLifeNotionLDL(contactId, TypeMessage, formattedDate, true))
                                        {
                                            Interlocked.Increment(ref countLife2Notion);
                                        }

                                        dbManager.ExecuteNonQuery($"UPDATE {tableSMS} SET Syncro=1 WHERE ContactId='{contactId}' AND Syncro=0");
                                        dbManager.ExecuteNonQuery($"UPDATE {tableLastLifeContactsLDL} SET DateLastLife='{dateMessageStr}',SMS=1,CALL=0 WHERE ContactId='{contactId}'");
                                    }
                                    else if (SMS == "0")
                                    {
                                        if (await LoadLifeNotionLDL(contactId, TypeMessage, formattedDate, false))
                                        {
                                            AddLog($"Se actualizó hora mensaje en notion LDL de contacto ({contactId})");
                                        }

                                        dbManager.ExecuteNonQuery($"UPDATE {tableSMS} SET Syncro=1 WHERE ContactId='{contactId}' AND Syncro=0");
                                        dbManager.ExecuteNonQuery($"UPDATE {tableLastLifeContactsLDL} SET SMS=1 WHERE ContactId='{contactId}'");
                                    }
                                }
                                else
                                {
                                    Console.WriteLine($"ContactId: {contactId} - No se pudo convertir una de las fechas.");
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Error procesando ContactId: {contactId} - {ex.Message}");
                            }
                        });

                        tasks.AddRange(batchTasks);
                        await Task.WhenAll(batchTasks); // Esperar a que todos los contactos del lote se procesen
                    }

                    await Task.WhenAll(tasks);


                    if (!stopRequested)
                    {
                        dbManager.ExecuteNonQuery($@"
                                            UPDATE {tableSMS} 
                                            SET Syncro = 1 
                                            WHERE Syncro = 0 
                                            AND strftime('%s', datetime(substr(DateMessage, 7, 4) || '-' || 
                                                                       substr(DateMessage, 1, 2) || '-' || 
                                                                       substr(DateMessage, 4, 2) || ' ' || 
                                                                       substr(DateMessage, 12))) < strftime('%s', datetime(substr('{limitDateSMS.Rows[0]["MAXDATEMESSAGE"]}', 7, 4) || '-' || 
                                                                                                                         substr('{limitDateSMS.Rows[0]["MAXDATEMESSAGE"]}', 1, 2) || '-' || 
                                                                                                                         substr('{limitDateSMS.Rows[0]["MAXDATEMESSAGE"]}', 4, 2) || ' ' || 
                                                                                                                         substr('{limitDateSMS.Rows[0]["MAXDATEMESSAGE"]}', 12)))");
                    }



                    var limitDateCall = dbManager.SelectData($"SELECT CAST(max(DateAdded) as TEXT) AS MAXDATEMESSAGE FROM {tableCallLogs} WHERE Syncro=0");

                    var listLastCALLperContacts = LastCALLperContacts();
                    var listLastCALLperContactsLDL = LastCALLperContactsLDL();

                    AddLog($"Se buscará {listLastCALLperContacts.Rows.Count} de contactos (CALL) en Notion");


                    foreach (var batch in listLastCALLperContacts.AsEnumerable().Chunk(batchSize))
                    {
                        if (stopRequested) break;

                        var batchTasks = batch.Select(async row =>
                        {
                            // Obtener los valores de las columnas
                            string contactId = row["ContactId"].ToString();
                            string dateMessageStr = row["DateMessage"].ToString();
                            string dateLastLifeStr = row["DateLastLife"].ToString();
                            string typeMessage = row["Source"].ToString();
                            string sms = row["SMS"] != DBNull.Value ? row["SMS"].ToString() : "0";
                            string call = row["CALL"] != DBNull.Value ? row["CALL"].ToString() : "0";

                            string format = "MM/dd/yyyy HH:mm:ss";

                            // Convertir la fecha de mensaje
                            if (!DateTime.TryParseExact(dateMessageStr, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dateMessage))
                            {
                                Console.WriteLine($"ContactId: {contactId} - No se pudo convertir la fecha DateMessage.");
                                return;
                            }
                            string formattedDate = dateMessage.ToString("yyyy-MM-ddTHH:mm:ss");

                            // Si DateLastLife está vacío
                            if (dateLastLifeStr == "SIN CARGAR")
                            {
                                if (await LoadLifeNotion(contactId, typeMessage, formattedDate, true))
                                {
                                    countLife2Notion++;
                                }

                                dbManager.ExecuteNonQuery($"UPDATE {tableCallLogs} SET Syncro=1 WHERE ContactId='{contactId}' AND Syncro=0");
                                dbManager.InsertData(tableLastLifeContacts, "ContactId,DateLastLife,CALL", $"'{contactId}','{dateMessageStr}',1");
                                return;
                            }

                            // Intentar convertir DateLastLife
                            if (DateTime.TryParseExact(dateLastLifeStr, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dateLastLife))
                            {
                                if (dateMessage > dateLastLife.AddHours(HourPerLife))
                                {
                                    if (await LoadLifeNotion(contactId, typeMessage, formattedDate, true))
                                    {
                                        countLife2Notion++;
                                    }

                                    dbManager.ExecuteNonQuery($"UPDATE {tableCallLogs} SET Syncro=1 WHERE ContactId='{contactId}' AND Syncro=0");
                                    dbManager.ExecuteNonQuery($"UPDATE {tableLastLifeContacts} SET DateLastLife='{dateMessageStr}',SMS=0,CALL=1 WHERE ContactId='{contactId}'");
                                }
                                else if (call == "0")
                                {
                                    if (await LoadLifeNotion(contactId, typeMessage, formattedDate, false))
                                    {
                                        AddLog($"Se actualizó hora llamada en notion PQF de contacto ({contactId})");
                                    }

                                    dbManager.ExecuteNonQuery($"UPDATE {tableCallLogs} SET Syncro=1 WHERE ContactId='{contactId}' AND Syncro=0");
                                    dbManager.ExecuteNonQuery($"UPDATE {tableLastLifeContacts} SET CALL=1 WHERE ContactId='{contactId}'");
                                }
                            }
                            else
                            {
                                Console.WriteLine($"ContactId: {contactId} - No se pudo convertir la fecha DateLastLife.");
                            }
                        });

                        await Task.WhenAll(batchTasks);
                    }


                    foreach (var batch in listLastCALLperContactsLDL.AsEnumerable().Chunk(batchSize))
                    {
                        if (stopRequested) break;

                        var batchTasks = batch.Select(async row =>
                        {
                            // Obtener los valores de las columnas
                            string contactId = row["ContactId"].ToString();
                            string dateMessageStr = row["DateMessage"].ToString();
                            string dateLastLifeStr = row["DateLastLife"].ToString();
                            string typeMessage = row["Source"].ToString();
                            string sms = row["SMS"] != DBNull.Value ? row["SMS"].ToString() : "0";
                            string call = row["CALL"] != DBNull.Value ? row["CALL"].ToString() : "0";

                            string format = "MM/dd/yyyy HH:mm:ss";

                            // Convertir la fecha de mensaje
                            if (!DateTime.TryParseExact(dateMessageStr, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dateMessage))
                            {
                                Console.WriteLine($"ContactId: {contactId} - No se pudo convertir la fecha DateMessage.");
                                return;
                            }
                            string formattedDate = dateMessage.ToString("yyyy-MM-ddTHH:mm:ss");

                            // Si DateLastLife está vacío
                            if (dateLastLifeStr == "SIN CARGAR")
                            {
                                if (await LoadLifeNotionLDL(contactId, typeMessage, formattedDate, true))
                                {
                                    countLife2Notion++;
                                }

                                dbManager.ExecuteNonQuery($"UPDATE {tableCallLogs} SET Syncro=1 WHERE ContactId='{contactId}' AND Syncro=0");
                                dbManager.InsertData(tableLastLifeContactsLDL, "ContactId,DateLastLife,CALL", $"'{contactId}','{dateMessageStr}',1");
                                return;
                            }

                            // Intentar convertir DateLastLife
                            if (DateTime.TryParseExact(dateLastLifeStr, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dateLastLife))
                            {
                                if (dateMessage > dateLastLife.AddHours(HourPerLife))
                                {
                                    if (await LoadLifeNotionLDL(contactId, typeMessage, formattedDate, true))
                                    {
                                        countLife2Notion++;
                                    }

                                    dbManager.ExecuteNonQuery($"UPDATE {tableCallLogs} SET Syncro=1 WHERE ContactId='{contactId}' AND Syncro=0");
                                    dbManager.ExecuteNonQuery($"UPDATE {tableLastLifeContactsLDL} SET DateLastLife='{dateMessageStr}',SMS=0,CALL=1 WHERE ContactId='{contactId}'");
                                }
                                else if (call == "0")
                                {
                                    if (await LoadLifeNotionLDL(contactId, typeMessage, formattedDate, false))
                                    {
                                        AddLog($"Se actualizó hora llamada en notion LDL de contacto ({contactId})");
                                    }

                                    dbManager.ExecuteNonQuery($"UPDATE {tableCallLogs} SET Syncro=1 WHERE ContactId='{contactId}' AND Syncro=0");
                                    dbManager.ExecuteNonQuery($"UPDATE {tableLastLifeContactsLDL} SET CALL=1 WHERE ContactId='{contactId}'");
                                }
                            }
                            else
                            {
                                Console.WriteLine($"ContactId: {contactId} - No se pudo convertir la fecha DateLastLife.");
                            }
                        });

                        // Esperar a que se completen las tareas del lote
                        await Task.WhenAll(batchTasks);
                    }


                    AddLog($"Se cargaron todas las vidas en NOTION ({countLife2Notion})");
                    if (!stopRequested)
                    {
                        dbManager.ExecuteNonQuery($@"
                                            UPDATE {tableCallLogs} 
                                            SET Syncro = 1 
                                            WHERE Syncro = 0 
                                            AND strftime('%s', datetime(substr(DateMessage, 7, 4) || '-' || 
                                                                       substr(DateMessage, 1, 2) || '-' || 
                                                                       substr(DateMessage, 4, 2) || ' ' || 
                                                                       substr(DateMessage, 12))) < strftime('%s', datetime(substr('{limitDateCall.Rows[0]["MAXDATEMESSAGE"]}', 7, 4) || '-' || 
                                                                                                                         substr('{limitDateCall.Rows[0]["MAXDATEMESSAGE"]}', 1, 2) || '-' || 
                                                                                                                         substr('{limitDateCall.Rows[0]["MAXDATEMESSAGE"]}', 4, 2) || ' ' || 
                                                                                                                         substr('{limitDateCall.Rows[0]["MAXDATEMESSAGE"]}', 12)))");
                    }


                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);

                }
                AddLog($"Esperando 30seg para siguiente ciclo");
                await Task.Delay(30000);
            }

           

        }

        public static DataTable LastSMSperContacts()
        {
            var query = @"
                        SELECT 
                           MOB.ContactId, 
                           MAX(CAST(MOB.DateMessage as nvarchar)) as DateMessage,
                           'SMS' AS Source,
                            CASE
                                    WHEN MAX(CAST(llc.DateLastLife AS TEXT)) IS NULL THEN 'SIN CARGAR'
                                    ELSE MAX(CAST(llc.DateLastLife AS TEXT))
                                END AS DateLastLife,
	                       llc.SMS,
	                       llc.CALL
                           FROM messageOutBound MOB
		                    LEFT JOIN Lastlifecontacts llc ON MOB.ContactId = llc.ContactId
                           WHERE Syncro = 0 
                           GROUP BY MOB.ContactId";

            var exist = dbManager.SelectData(query);

            return exist;

        }
        public static DataTable LastSMSperContactsLDL()
        {
            var query = @"
                        SELECT 
                           MOB.ContactId, 
                           MAX(CAST(MOB.DateMessage as nvarchar)) as DateMessage,
                           'SMS' AS Source,
	                       CASE
                                    WHEN MAX(CAST(llc.DateLastLife AS TEXT)) IS NULL THEN 'SIN CARGAR'
                                    ELSE MAX(CAST(llc.DateLastLife AS TEXT))
                                END AS DateLastLife,
	                       llc.SMS,
	                       llc.CALL
                           FROM messageOutBound MOB
		                    LEFT JOIN LastlifecontactsLDL llc ON MOB.ContactId = llc.ContactId
                           WHERE Syncro = 0 
                           GROUP BY MOB.ContactId";

            var exist = dbManager.SelectData(query);

            return exist;

        }
        public static DataTable LastCALLperContacts()
        {
            var query = @"
                        SELECT 
                           CAL.ContactId, 
                           MAX(CAST(CAL.DateAdded as nvarchar)) as DateMessage,
                           'CALL' AS Source,
	                       CASE
                                    WHEN MAX(CAST(llc.DateLastLife AS TEXT)) IS NULL THEN 'SIN CARGAR'
                                    ELSE MAX(CAST(llc.DateLastLife AS TEXT))
                                END AS DateLastLife,
	                       llc.SMS,
	                       llc.CALL
                           FROM CallLogs CAL
		                    LEFT JOIN Lastlifecontacts llc ON CAL.ContactId = llc.ContactId
                           WHERE Syncro = 0 
                           GROUP BY CAL.ContactId";

            var exist = dbManager.SelectData(query);

            return exist;

        }
        public static DataTable LastCALLperContactsLDL()
        {
            var query = @"
                        SELECT 
                           CAL.ContactId, 
                           MAX(CAST(CAL.DateAdded as nvarchar)) as DateMessage,
                           'CALL' AS Source,
	                       CASE
                                    WHEN MAX(CAST(llc.DateLastLife AS TEXT)) IS NULL THEN 'SIN CARGAR'
                                    ELSE MAX(CAST(llc.DateLastLife AS TEXT))
                                END AS DateLastLife,
	                       llc.SMS,
	                       llc.CALL
                           FROM CallLogs CAL
		                    LEFT JOIN LastlifecontactsLDL llc ON CAL.ContactId = llc.ContactId
                           WHERE Syncro = 0 
                           GROUP BY CAL.ContactId";

            var exist = dbManager.SelectData(query);

            return exist;

        }



        public void ChangeTime(int Hours)
        {

            dbManager.CreateTable(tableConfiguration, tableConfigurationDefinition);


            var exist = dbManager.SelectData($"SELECT HourPerLife FROM {tableConfiguration} WHERE id=1");
            if (exist != null && exist.Rows.Count > 0)
            {
                dbManager.ExecuteNonQuery($"UPDATE {tableConfiguration} SET HourPerLife='{Hours}' WHERE id=1");
            }
            else
            {
                dbManager.InsertData(tableConfiguration, "HourPerLife", $"'{Hours}'");
            }
            AddLog($"las horas para saber si se carga vida ha cambiado a: {Hours}");
        }
        public string GetHoursConfiguration()
        {

            dbManager.CreateTable(tableConfiguration, tableConfigurationDefinition);

            var HoursConfiguration = "0";
            var exist = dbManager.SelectData($"SELECT HourPerLife FROM {tableConfiguration} WHERE id=1");
            if (exist != null && exist.Rows.Count > 0)
            {
                HoursConfiguration = exist.Rows[0]["HourPerLife"].ToString();
            }
            return HoursConfiguration;
        }


        //public static async Task<bool> LoadLifeNotion(string ContactId, string typeMessage,string dateTimeMessage, bool countlife)
        //{


        //   var contactPhone = await GetContactPhoneAsync(ContactId);

        //   var listPageContact = await GetPageIdByNameAsync(databaseIdVidas, contactPhone);
        //    var LifeLoad = 0;
        //    foreach(var PageContact in listPageContact)
        //    {
        //        if (await UpdatePageAsync(PageContact.id, countlife ? PageContact.lifes + 1 : PageContact.lifes, typeMessage, dateTimeMessage))
        //        {

        //            var listPageContactVerif = await GetPageIdByNameAsync(databaseIdVidas, contactPhone);



        //            if (countlife)
        //            {
        //                AddLog($"El contacto {ContactId} si estaba en Notion PQF y se sumó una vida");
        //            }
        //            LifeLoad++;
        //        }
        //        else
        //        {
        //            if (countlife)
        //            {
        //                AddLog($"El contacto {ContactId} NO estaba en Notion PQF");
        //            }
        //        }
        //    }
        //    if (LifeLoad>0)
        //    {
        //        return true;
        //    }
        //    else
        //    {
        //        return false; 
        //    }

        //}



        public static async Task<bool> LoadLifeNotion(string ContactId, string typeMessage, string dateTimeMessage, bool countlife)
        {
            var contactPhone = await GetContactPhoneAsync(ContactId);

            var listPageContact = await GetPageIdByNameAsync(databaseIdVidas, contactPhone);
            var LifeLoad = 0;

            foreach (var PageContact in listPageContact)
            {
                bool actualizadoCorrectamente = false;
                int intentos = 0; 
                const int maxIntentos = 3; 

                while (!actualizadoCorrectamente && intentos < maxIntentos)
                {
                    intentos++;

                    if (await UpdatePageAsync(PageContact.id, PageContact.owner == "" ? 0 : (countlife ? PageContact.lifes + 1 : PageContact.lifes), typeMessage, dateTimeMessage))
                    {
                        var listPageContactVerif = await GetPageIdByNameAsync(databaseIdVidas, contactPhone);

                        var contactVerificado = listPageContactVerif.FirstOrDefault(c => c.id == PageContact.id);
                        if (contactVerificado != null)
                        {
                            actualizadoCorrectamente = contactVerificado.lifes == (countlife ? PageContact.lifes + 1 : PageContact.lifes);
                        }

                        if (actualizadoCorrectamente)
                        {
                            AddLog($"El contacto {contactPhone} fue actualizado correctamente en Notion PQF.");
                            LifeLoad++;
                        }
                        else
                        {
                            AddLog($"El contacto {contactPhone} NO fue actualizado correctamente. Reintentando ({intentos}/{maxIntentos}).");
                        }
                    }
                    else
                    {
                        AddLog($"Fallo al actualizar la página para el contacto {contactPhone}. Reintentando ({intentos}/{maxIntentos}).");
                    }
                }

                if (!actualizadoCorrectamente)
                {
                    AddLog($"El contacto {contactPhone} no pudo ser actualizado después de {maxIntentos} intentos.");
                }
            }

            return LifeLoad > 0;
        }



        //public static async Task<bool> LoadLifeNotionLDL(string ContactId, string typeMessage, string dateTimeMessage, bool countlife)
        //{
        //    var contactPhone = await GetContactPhoneAsync(ContactId);

        //    var listPageContact = await GetPageIdByNameAsync(databaseIdVidasLDL, contactPhone);
        //    var LifeLoad = 0;

        //    foreach (var PageContact in listPageContact)
        //    {
        //        if (await UpdatePageAsync(PageContact.id, countlife ? PageContact.lifes + 1 : PageContact.lifes, typeMessage, dateTimeMessage))
        //        {

        //            if (countlife)
        //            {
        //                AddLog($"El contacto {ContactId} si estaba en Notion LDL y se sumó una vida");
        //            }
        //            LifeLoad++;
        //        }
        //        else
        //        {

        //            if (countlife)
        //            {
        //                AddLog($"El contacto {ContactId} NO estaba en Notion LDL");
        //            }
        //        }
        //    }
        //    return LifeLoad > 0;
        //}

        public static async Task<bool> LoadLifeNotionLDL(string ContactId, string typeMessage, string dateTimeMessage, bool countlife)
        {
            var contactPhone = await GetContactPhoneAsync(ContactId);

            var listPageContact = await GetPageIdByNameAsync(databaseIdVidasLDL, contactPhone);
            var LifeLoad = 0;

            foreach (var PageContact in listPageContact)
            {
                bool actualizadoCorrectamente = false;
                int intentos = 0;
                const int maxIntentos = 3;

                while (!actualizadoCorrectamente && intentos < maxIntentos)
                {
                    intentos++;

                    if (await UpdatePageAsync(PageContact.id, PageContact.owner == "" ? 0 : (countlife ? PageContact.lifes + 1 : PageContact.lifes), typeMessage, dateTimeMessage))
                    {
                        var listPageContactVerif = await GetPageIdByNameAsync(databaseIdVidasLDL, contactPhone);

                        var contactVerificado = listPageContactVerif.FirstOrDefault(c => c.id == PageContact.id);
                        if (contactVerificado != null)
                        {
                            actualizadoCorrectamente = contactVerificado.lifes == (countlife ? PageContact.lifes + 1 : PageContact.lifes);
                        }

                        if (actualizadoCorrectamente)
                        {
                            AddLog($"El contacto {contactPhone}fue actualizado correctamente en Notion PQF.");
                            LifeLoad++;
                        }
                        else
                        {
                            AddLog($"El contacto {contactPhone} NO fue actualizado correctamente. Reintentando ({intentos}/{maxIntentos}).");
                        }
                    }
                    else
                    {
                        AddLog($"Fallo al actualizar la página para el contacto {contactPhone} . Reintentando ({intentos}/{maxIntentos}).");
                    }
                }

                if (!actualizadoCorrectamente)
                {
                    AddLog($"El contacto {contactPhone} no pudo ser actualizado después de {maxIntentos} intentos.");
                }
            }

            return LifeLoad > 0;
        }



        private class pageContact
        {
            public string id { get; set; }
            public int lifes { get; set; }
            public string owner { get; set; }
        }

        private static async Task<List<pageContact>> GetPageIdByNameAsync(string databaseId, string contactPhone)
        {
            List<pageContact> contactLifeNotions = new List<pageContact>();
            using (HttpClient client = new HttpClient())
            {
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", NotionToken);
                client.DefaultRequestHeaders.Add("Notion-Version", "2022-06-28");

                var requestUrl = $"https://api.notion.com/v1/databases/{databaseId}/query";
                string requestBody = $@"
                {{
                  ""filter"": {{
                        ""property"": ""%3CSt%5B"",
                        ""phone_number"": {{
                          ""contains"": ""{contactPhone}""
                        }}
                  }}
                }}";




                var content = new StringContent(requestBody, Encoding.UTF8, "application/json");
                var response = await client.PostAsync(requestUrl, content);
                var responseBody = await response.Content.ReadAsStringAsync();

                if (response.IsSuccessStatusCode)
                {
                    JObject jsonResponse = JObject.Parse(responseBody);
                    JArray results = (JArray)jsonResponse["results"];
                    if (results.Count > 0)
                    {
                        

                        foreach (var result in results)
                        {
                            var contactLife = 0;

                            if (result["properties"]["❤️.Vida"] != null &&
                                result["properties"]["❤️.Vida"]["number"] != null &&
                                result["properties"]["❤️.Vida"]["number"].ToString() != "")
                            {
                                contactLife = (int)result["properties"]["❤️.Vida"]["number"];
                            }


                            var ownerContact = "";


                            if (result["properties"]["Owner"] != null &&
                                result["properties"]["Owner"]["people"] != null &&
                                result["properties"]["Owner"]["people"][0]["name"] != null &&
                                result["properties"]["Owner"]["people"][0]["name"].ToString() != "")  //name
                            {
                                ownerContact = result["properties"]["Owner"]["people"][0]["name"].ToString();
                            }


                            var notion = new pageContact
                            {
                                id = result["id"].ToString(),
                                lifes = contactLife,
                                owner = ownerContact,
                            };

                            contactLifeNotions.Add(notion);
                        }
                    }
                }
            }

            return contactLifeNotions;
        }





        private static async Task<bool> UpdatePageAsync(string pageId, int ActualLifes,string typeMessage,string dateMessage)
        {
            using (HttpClient client = new HttpClient())
            {
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", NotionToken);
                client.DefaultRequestHeaders.Add("Notion-Version", "2022-06-28");

                var requestUrl = $"https://api.notion.com/v1/pages/{pageId}";

                // Crear el cuerpo de la solicitud JSON
                string requestBody = "";
                if (typeMessage == "SMS")
                {
                    requestBody = $@"
                                {{
                                    ""properties"": {{
                                        ""J_I%5D"": {{
                                            ""number"": {ActualLifes}
                                    }},
                                        ""i%3EeV"" : {{
                                            ""date"": {{
                                                ""start"": ""{dateMessage}""
                                            }}
                                        }}
                                     }}
                                }}";

                }else if (typeMessage == "CALL")
                {
                    requestBody = $@"
                                {{
                                    ""properties"": {{
                                        ""J_I%5D"": {{
                                            ""number"": {ActualLifes}
                                    }},
                                        ""oDpA"" : {{
                                            ""date"": {{
                                                ""start"": ""{dateMessage}""
                                            }}
                                        }}
                                     }}
                                }}";

                }

                var content = new StringContent(requestBody, Encoding.UTF8, "application/json");

                // Crear la solicitud PATCH
                var request = new HttpRequestMessage(new HttpMethod("PATCH"), requestUrl)
                {
                    Content = content
                };

                // Enviar la solicitud
                var response = await client.SendAsync(request);
                var responseBody = await response.Content.ReadAsStringAsync();

                if (!response.IsSuccessStatusCode)
                {
                    return false;
                }
                return true;
            }
        }



        public static async Task<string> GetContactPhoneAsync(string contactId)
        {
            string apiUrl = $"{baseUrlGHL}/contacts/{contactId}";
            int errorCount = 0;
            int maxErrorCount = 10;

            using (HttpClient client = new HttpClient())
            {
                client.DefaultRequestHeaders.Add("Authorization", $"Bearer {bearerTokenGHL}");
                client.DefaultRequestHeaders.Add("Accept", "application/json");
                client.DefaultRequestHeaders.Add("Version", "2021-04-15");

                while (errorCount < maxErrorCount)
                {
                    if (stopRequested) break;
                    try
                    {
                        HttpResponseMessage response = await client.GetAsync(apiUrl);

                        if (response.IsSuccessStatusCode)
                        {
                            string responseBody = await response.Content.ReadAsStringAsync();
                            JObject jsonResponse = JObject.Parse(responseBody);
                            string contactPhone = jsonResponse["contact"]?["phone"]?.ToString();

                            errorCount = 0;

                            return contactPhone.Replace("+","");
                        }
                        else
                        {
                            Console.WriteLine($"Error: {response.StatusCode}");
                            errorCount++;

                            if (errorCount >= maxErrorCount)
                            {
                                Console.WriteLine("Número máximo de intentos fallidos alcanzado. Finalizando.");
                                return null;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Excepción: {ex.Message}");
                        errorCount++;

                        if (errorCount >= maxErrorCount)
                        {
                            Console.WriteLine("Número máximo de intentos fallidos alcanzado debido a una excepción. Finalizando.");
                            return null;
                        }
                    }
                }
            }

            return null;
        }
        private static void AddLog(string message)
        {
            try
            {
                string timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                if (_logGrid.InvokeRequired)
                {
                    _logGrid.Invoke(new Action(() =>
                    {
                        lock (_logLock)
                        {
                            _logGrid.Rows.Insert(0, timestamp, message);

                            if (_logGrid.Rows.Count > 500)
                            {
                                _logGrid.Rows.RemoveAt(_logGrid.Rows.Count - 1);
                            }
                        }
                    }));
                }
                else
                {
                    lock (_logLock)
                    {
                        _logGrid.Rows.Insert(0, timestamp, message);

                        if (_logGrid.Rows.Count > 500)
                        {
                            _logGrid.Rows.RemoveAt(_logGrid.Rows.Count - 1);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }




    }
}
