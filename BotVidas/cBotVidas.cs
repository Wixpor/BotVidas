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
                                DateLastLife DATETIME NOT NULL
                                ";


        private static readonly string NotionToken = "secret_HeMB4Rd9etcmDb1jOKIY4gEhdFYZLLPdyXQyQVqnuNS";
        private static readonly string databaseIdVidas = "169634efa4688009836ccce92a6678c5";  //"169634efa4688009836ccce92a6678c5"; DB de pruebas             d6d2748af21c4bca8c95bd7c5452016c : producción
        private static readonly string databaseIdVidasLDL = "169634efa4688009836ccce92a6678c5";      //                                                       c54490c2ab5f407c8971970aab3b6454 : producción

        public static bool stopRequested = false;
        private static readonly string bearerTokenGHL = "pit-2af37261-968e-4764-a16f-00bd69676be0";
        private static readonly string baseUrlGHL = "https://services.leadconnectorhq.com";

        private static DataGridView _logGrid;

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
            AddLog("Se inicia bot");
            stopRequested=false;

            while (!stopRequested)
            {
                try
                {

                    AddLog("Buscando contactos para agregar vidas");
                    var listLastSMSperContacts = LastSMSperContacts();

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


                    AddLog($"Se buscará {listLastSMSperContacts.Rows.Count} de contactos en Notion");
                    var countLife2Notion = 0;
                    foreach (DataRow row in listLastSMSperContacts.Rows)
                    {
                        if (stopRequested) break;
                        // Obtener los valores de las columnas
                        string contactId = row["ContactId"].ToString();
                        string dateMessageStr = row["DateMessage"].ToString();
                        string dateLastLifeStr = row["DateLastLife"].ToString();
                        string TypeMessage = row["Source"].ToString();






                        string format = "MM/dd/yyyy HH:mm:ss";  

                        DateTime parsedDate = DateTime.ParseExact(dateMessageStr, format, CultureInfo.InvariantCulture);
                        string formattedDate = parsedDate.ToString("yyyy-MM-ddTHH:mm:ss");

                        if (dateLastLifeStr == "")
                        {
                            if (await LoadLifeNotion(contactId, TypeMessage, formattedDate))
                            {
                                countLife2Notion++;
                            }

                            if (await LoadLifeNotionLDL(contactId, TypeMessage, formattedDate))
                            {
                                countLife2Notion++;
                            }
                            dbManager.ExecuteNonQuery($"UPDATE {tableSMS} SET Syncro=1 WHERE ContactId='{contactId}' AND Syncro=0");
                            dbManager.ExecuteNonQuery($"UPDATE {tableCallLogs} SET Syncro=1 WHERE ContactId='{contactId}' AND Syncro=0");
                            dbManager.InsertData(tableLastLifeContacts, "ContactId,DateLastLife", $"'{contactId}','{dateMessageStr}'");
                            continue;
                        }
                        // Intentar convertir las fechas con el formato exacto
                        if (DateTime.TryParseExact(dateMessageStr, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dateMessage) &&
                            DateTime.TryParseExact(dateLastLifeStr, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dateLastLife))
                        {
                            // Comparar las horas
                            if (dateMessage > dateLastLife.AddHours(HourPerLife))
                            {
                                if (await LoadLifeNotion(contactId, TypeMessage, formattedDate))
                                {
                                    countLife2Notion++;
                                }
                                if (await LoadLifeNotionLDL(contactId, TypeMessage, formattedDate))
                                {
                                    countLife2Notion++;
                                }
                                dbManager.ExecuteNonQuery($"UPDATE {tableSMS} SET Syncro=1 WHERE ContactId='{contactId}' AND Syncro=0");
                                dbManager.ExecuteNonQuery($"UPDATE {tableCallLogs} SET Syncro=1 WHERE ContactId='{contactId}' AND Syncro=0");
                                dbManager.ExecuteNonQuery($"UPDATE {tableLastLifeContacts} SET ContactId='{contactId}','{dateMessageStr}' WHERE ContactId='{contactId}'");
                            }
                        }
                        else
                        {
                            Console.WriteLine($"ContactId: {contactId} - No se pudo convertir una de las fechas.");
                        }

                    }

                    AddLog($"Se cargaron todas las vidas en NOTION ({countLife2Notion})");
                    if (!stopRequested)
                    {
                        dbManager.ExecuteNonQuery($"UPDATE {tableSMS} SET Syncro=1 WHERE Syncro=0");
                        dbManager.ExecuteNonQuery($"UPDATE {tableCallLogs} SET Syncro=1 WHERE Syncro=0");
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
                           combined.ContactId AS ContactId,
                           CAST(combined.DateMessage as nvarchar) AS DateMessage,
                           CAST(llc.DatelastLife  as nvarchar) AS DatelastLife,
                            combined.Source AS Source
                        FROM (
                           SELECT 
                               ContactId,
                               MAX(DateMessage) as DateMessage,
                               Source
                           FROM (
                         SELECT 
                               ContactId, 
                               MAX(CAST(DateMessage as nvarchar)) as DateMessage,
                               'SMS' AS Source 
                               FROM {0}
                               WHERE Syncro = 0 
                               GROUP BY ContactId
                               UNION ALL
                               SELECT 
                               ContactId, 
                               MAX(CAST(DateAdded as nvarchar)) as DateMessage,
                               'CALL' AS Source 
                               FROM {1} 
                               WHERE Syncro = 0 AND callStatus = 'completed'
                               GROUP BY ContactId
                               ) as subcombined
                               GROUP BY ContactId, Source
                        ) as combined
                        LEFT JOIN Lastlifecontacts llc ON combined.ContactId = llc.ContactId
                        ORDER BY combined.DateMessage DESC";

            var formattedQuery = string.Format(query, tableSMS, tableCallLogs);
            var exist = dbManager.SelectData(formattedQuery);

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


        public static async Task<bool> LoadLifeNotion(string ContactId, string typeMessage,string dateTimeMessage)
        {


           var contactPhone = await GetContactPhoneAsync(ContactId);

           var listPageContact = await GetPageIdByNameAsync(databaseIdVidas, contactPhone);
            var LifeLoad = 0;
            foreach(var PageContact in listPageContact)
            {
                if (await UpdatePageAsync(PageContact.id, PageContact.lifes + 1, typeMessage, dateTimeMessage))
                {
                    AddLog($"El contacto {ContactId} si estaba en Notion PQF y se sumó una vida");
                    LifeLoad++;
                }
                else
                {
                    AddLog($"El contacto {ContactId} NO estaba en Notion PQF");
                }
            }
            if (LifeLoad>0)
            {
                return true;
            }
            else
            {
                return false; 
            }
            
        }


        public static async Task<bool> LoadLifeNotionLDL(string ContactId, string typeMessage, string dateTimeMessage)
        {
            var contactPhone = await GetContactPhoneAsync(ContactId);

            var listPageContact = await GetPageIdByNameAsync(databaseIdVidasLDL, contactPhone);
            var LifeLoad = 0;

            foreach (var PageContact in listPageContact)
            {
                if (await UpdatePageAsync(PageContact.id, PageContact.lifes + 1, typeMessage, dateTimeMessage))
                {
                    AddLog($"El contacto {ContactId} si estaba en Notion LDL y se sumó una vida");
                    LifeLoad++;
                }
                else
                {
                    AddLog($"El contacto {ContactId} NO estaba en Notion LDL");
                }
            }
            return LifeLoad > 0;
        }




        private class pageContact
        {
            public string id { get; set; }
            public int lifes { get; set; }
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

                            var notion = new pageContact
                            {
                                id = result["id"].ToString(),
                                lifes = contactLife
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
                _logGrid.Rows.Insert(0, timestamp, message);

                if (_logGrid.Rows.Count > 500)
                {
                    _logGrid.Rows.RemoveAt(_logGrid.Rows.Count - 1);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }



    }
}
